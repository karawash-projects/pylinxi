import asyncio
import json
import tempfile
import unittest
from dataclasses import dataclass

from pylinxi import CircuitBreaker, FileJournal, MemoryJournal, Policy, Retry, SQLiteJournal, default_codec, flow, step


def run(coro):
    return asyncio.run(coro)


class Crash(Exception):
    pass


class TestDurability(unittest.TestCase):
    def test_resume_skips_completed_steps(self):
        calls = {"a": 0, "b": 0}
        crash = {"on": True}

        @step
        def a(x):
            calls["a"] += 1
            return x + 1

        @step
        def b(x):
            calls["b"] += 1
            if crash["on"]:
                raise Crash("process died")
            return x * 10

        @flow
        async def f(ctx, x):
            return await b(await a(x))

        r1 = run(f.run(1, run_id="r1"))
        self.assertEqual(r1.status, "failed")
        crash["on"] = False
        r2 = run(f.resume("r1"))            # no args needed: read from journal
        self.assertEqual((r2.status, r2.output), ("completed", 20))
        self.assertEqual(calls, {"a": 1, "b": 2})
        r3 = run(f.resume("r1"))            # finished runs return cached output
        self.assertEqual(r3.output, 20)
        self.assertEqual(calls, {"a": 1, "b": 2})

    def test_plain_python_branching_and_loops(self):
        @step
        def classify(t):
            return "refund" if "money" in t else "faq"

        @step
        def refund(t):
            return f"refund:{t}"

        @step
        def faq(t):
            return f"faq:{t}"

        @flow
        async def support(ctx, t):
            kind = await classify(t)
            out = []
            for i in range(3):                  # loops just work
                out.append(await ctx.route(kind, {"refund": refund, "faq": faq}, f"{t}{i}"))
            return out

        self.assertEqual(run(support.run("money back")).output,
                         ["refund:money back0", "refund:money back1", "refund:money back2"])
        self.assertEqual(run(support.run("hours?")).output[0], "faq:hours?0")

    def test_parallel_map_replays_deterministically(self):
        n = {"calls": 0}
        fail = {"on": True}

        @step
        async def work(i):
            n["calls"] += 1
            await asyncio.sleep(0.001 * (5 - i))   # finish out of order
            if i == 3 and fail["on"]:
                raise Crash()
            return i * i

        @flow
        async def f(ctx):
            return await ctx.map(work, range(5), concurrency=5)

        self.assertEqual(run(f.run(run_id="p")).status, "failed")
        fail["on"] = False
        r = run(f.resume("p"))
        self.assertEqual(r.output, [0, 1, 4, 9, 16])
        self.assertEqual(n["calls"], 6)   # 5 first time, only item 3 re-run

    def test_human_in_the_loop_signal(self):
        @step
        def draft():
            return "draft v1"

        @step
        def publish(d):
            return f"published {d}"

        @flow
        async def f(ctx):
            d = await draft()
            ok = await ctx.wait_for("approval")
            return await publish(d) if ok else "rejected"

        r = run(f.run(run_id="h"))
        self.assertEqual((r.status, r.waiting_for), ("suspended", "approval"))
        r = run(f.resume("h", signals={"approval": True}))
        self.assertEqual(r.output, "published draft v1")


class TestSelfHealing(unittest.TestCase):
    def test_retry_then_success(self):
        n = {"c": 0}

        @step(policy=Policy(retry=Retry(attempts=3, backoff=0)))
        def flaky():
            n["c"] += 1
            if n["c"] < 3:
                raise ConnectionError("blip")
            return "ok"

        @flow
        async def f(ctx):
            return await flaky()

        r = run(f.run())
        self.assertEqual(r.output, "ok")
        done = [e for e in r.events if e["event"] == "completed"][0]
        self.assertEqual((done["attempts"], done["healed"]), (3, "retry"))

    def test_validate_and_repair_feeds_error_back(self):
        # Simulates an LLM that returns broken JSON until told about the error.
        def fake_llm(prompt):
            return '{"answer": 42}' if "previous error" in prompt else "{answer: 42"

        def repair(err, args, kwargs, attempt):
            return (args[0] + f"\nprevious error: {err}",), kwargs

        def is_json(s):
            try:
                json.loads(s)
                return True
            except ValueError:
                return False

        llm = step(fake_llm, policy=Policy(retry=Retry(attempts=3, backoff=0),
                                           validate=is_json, repair=repair))

        @flow
        async def f(ctx):
            return json.loads(await llm("give json"))

        r = run(f.run())
        self.assertEqual(r.output, {"answer": 42})
        self.assertTrue(any(e["event"] == "repaired" for e in r.events))

    def test_fallback_and_timeout(self):
        async def slow(q):
            await asyncio.sleep(1)
            return "primary"

        s = step(slow, policy=Policy(timeout=0.02, retry=Retry(attempts=2, backoff=0),
                                     fallback=lambda q: f"cached:{q}"))

        @flow
        async def f(ctx):
            return await s("q")

        r = run(f.run())
        self.assertEqual(r.output, "cached:q")

    def test_circuit_breaker_short_circuits_across_runs(self):
        br = CircuitBreaker(threshold=2, reset_after=60)
        n = {"c": 0}

        def down():
            n["c"] += 1
            raise ConnectionError("503")

        s = step(down, policy=Policy(breaker=br, fallback=lambda: "degraded"))

        @flow
        async def f(ctx):
            return await s()

        for _ in range(5):
            self.assertEqual(run(f.run()).output, "degraded")
        self.assertEqual(n["c"], 2)        # breaker opened after 2 real failures
        self.assertEqual(br.state, "open")


@dataclass
class Ticket:
    id: int
    tags: tuple


default_codec.register(Ticket)


class TestJournal(unittest.TestCase):
    def _durable_flow(self, journal):
        @step
        def make():
            return Ticket(7, ("a", "b"))

        @flow(journal=journal)
        async def f(ctx):
            t = await make()
            ctx.state["ticket_id"] = t.id
            ctx.state["api_key"] = "sk-live-123"
            await asyncio.sleep(0)
            return await ctx.step(lambda: t, name="echo")
        return f

    def test_backends_roundtrip_types_and_verify(self):
        with tempfile.TemporaryDirectory() as d:
            for j in (MemoryJournal(), FileJournal(d), SQLiteJournal(), MemoryJournal(secret=b"k")):
                f = self._durable_flow(j)
                r = run(f.run(run_id="t1"))
                self.assertEqual(r.output, Ticket(7, ("a", "b")))
                again = run(f.resume("t1"))
                self.assertIsInstance(again.output, Ticket)
                self.assertTrue(j.verify("t1"), type(j).__name__)
                j.close()

    def test_deltas_redaction_time_travel(self):
        j = MemoryJournal()
        run(self._durable_flow(j).run(run_id="t2"))
        entries = j.load("t2")
        echo = [e for e in entries if e.key == "echo#0"][0]
        self.assertEqual(echo.delta, {"ticket_id": 7, "api_key": "[REDACTED]"})  # only changed keys
        self.assertEqual(j.state_at("t2", seq=1), {})
        self.assertEqual(j.state_at("t2")["ticket_id"], 7)

    def test_tamper_detection(self):
        j = MemoryJournal(secret=b"k")
        run(self._durable_flow(j).run(run_id="t3"))
        payload, sig = j._data["t3"][1]
        j._data["t3"][1] = (payload.replace("7", "8"), sig)
        self.assertFalse(j.verify("t3"))

    def test_sensitive_results_not_persisted(self):
        j = MemoryJournal()
        n = {"c": 0}

        @step(sensitive=True)
        def get_token():
            n["c"] += 1
            return "secret-token"

        @flow(journal=j)
        async def f(ctx):
            tok = await get_token()
            await ctx.wait_for("go")
            return len(tok)

        run(f.run(run_id="s"))
        self.assertNotIn("secret-token", str(j._data["s"]))
        self.assertEqual(run(f.resume("s", signals={"go": 1})).output, 12)
        self.assertEqual(n["c"], 2)      # recomputed on resume instead of stored


if __name__ == "__main__":
    unittest.main()
