import asyncio
import unittest

try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.trace import StatusCode

    from pylinxi.otel import OTelExporter
except ImportError:
    TracerProvider = None

from pylinxi import CircuitBreaker, Policy, Retry, flow, step


@unittest.skipUnless(TracerProvider, "install opentelemetry-sdk")
class TestOTel(unittest.TestCase):
    def setUp(self):
        self.mem = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(self.mem))
        self.exporter = OTelExporter(tracer=provider.get_tracer("test"))

    def spans(self):
        return {s.name: s for s in self.mem.get_finished_spans()}

    def test_span_tree_with_healing_events(self):
        n = {"c": 0}

        @step(policy=Policy(retry=Retry(attempts=3, backoff=0)))
        def flaky():
            n["c"] += 1
            if n["c"] < 2:
                raise ConnectionError("blip")
            return 1

        @step(policy=Policy(breaker=CircuitBreaker(threshold=1), fallback=lambda: "cached"))
        def down():
            raise ConnectionError("503")

        @flow(on_event=self.exporter)
        async def agent(ctx):
            await flaky()
            return await down()

        r = asyncio.run(agent.run(run_id="o1"))
        self.assertEqual(r.output, "cached")
        s = self.spans()
        run, fl, dn = s["pylinxi.run agent"], s["pylinxi.step flaky"], s["pylinxi.step down"]
        self.assertEqual(fl.parent.span_id, run.context.span_id)
        self.assertEqual(fl.context.trace_id, run.context.trace_id)
        self.assertEqual((fl.attributes["pylinxi.attempts"], fl.attributes["pylinxi.healed"]), (2, "retry"))
        self.assertIn("pylinxi.attempt_failed", [e.name for e in fl.events])
        self.assertIn("pylinxi.fallback", [e.name for e in dn.events])
        self.assertEqual(run.attributes["pylinxi.status"], "completed")

    def test_failure_suspend_and_replay(self):
        boom = {"on": True}

        @step
        def a():
            return 1

        @step
        def b():
            if boom["on"]:
                raise ValueError("bad input")
            return 2

        @flow(on_event=self.exporter)
        async def f(ctx):
            await a()
            await b()
            return await ctx.wait_for("ok")

        asyncio.run(f.run(run_id="o2"))
        spans = self.mem.get_finished_spans()
        run1 = [x for x in spans if x.name == "pylinxi.run f"][0]
        self.assertEqual(run1.status.status_code, StatusCode.ERROR)
        self.assertEqual([x for x in spans if x.name == "pylinxi.step b"][0].status.status_code, StatusCode.ERROR)

        self.mem.clear()
        boom["on"] = False
        asyncio.run(f.resume("o2"))
        s = self.spans()
        self.assertNotIn("pylinxi.step a", s)                       # replayed, not re-executed
        run2 = s["pylinxi.run f"]
        self.assertIn("pylinxi.replayed", [e.name for e in run2.events])
        self.assertEqual((run2.attributes["pylinxi.status"], run2.attributes["pylinxi.waiting_for"]),
                         ("suspended", "ok"))

    def test_record_errors_off_strips_messages(self):
        exp = OTelExporter(tracer=self.exporter.tracer, record_errors=False)

        @flow(on_event=exp)
        async def f(ctx):
            await ctx.step(lambda: (_ for _ in ()).throw(ValueError("ssn 123-45-6789")), name="x")

        asyncio.run(f.run())
        for sp in self.mem.get_finished_spans():
            self.assertNotIn("123-45-6789", str(sp.attributes) + str(sp.status.description))

    def test_continue_as_new_ends_every_generation_span(self):
        @flow(on_event=self.exporter)
        async def loop(ctx, n):
            if n < 2:
                ctx.continue_as_new(n + 1)
            return n

        asyncio.run(loop.run(0, run_id="c"))
        runs = [s for s in self.mem.get_finished_spans() if s.name == "pylinxi.run loop"]
        self.assertEqual(sorted(s.attributes["pylinxi.run_id"] for s in runs), ["c", "c.g1", "c.g2"])
        self.assertEqual(sorted(s.attributes["pylinxi.status"] for s in runs), ["completed", "continued", "continued"])

    def test_broken_observer_never_breaks_a_run(self):
        def bad(ev):
            raise RuntimeError("exporter down")

        @flow(on_event=[bad, self.exporter])
        async def f(ctx):
            return await ctx.step(lambda: "fine", name="s")

        self.assertEqual(asyncio.run(f.run()).output, "fine")
        self.assertIn("pylinxi.step s", self.spans())


if __name__ == "__main__":
    unittest.main()
