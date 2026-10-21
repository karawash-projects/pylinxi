import asyncio
import tempfile
import time
import unittest

from pylinxi import FileJournal, MemoryJournal, Policy, Retry, SQLiteJournal, flow, idempotency_key, open_journal, step


def run(coro):
    return asyncio.run(coro)


async def collect(agen):
    return [ev async for ev in agen]


class TestStreaming(unittest.TestCase):
    def test_async_generator_streams_and_journals_aggregate(self):
        @step
        async def llm(prompt):
            for tok in ["Hel", "lo ", "world"]:
                await asyncio.sleep(0)
                yield tok

        @flow
        async def chat(ctx, p):
            return (await llm(p)).upper()

        evs = run(collect(chat.stream("hi", run_id="s1")))
        self.assertEqual([e["chunk"] for e in evs if e["event"] == "chunk"], ["Hel", "lo ", "world"])
        self.assertEqual(evs[-1]["result"].output, "HELLO WORLD")
        # replay returns the aggregate without re-streaming
        evs2 = run(collect(chat.stream(run_id="s1")))
        self.assertFalse([e for e in evs2 if e["event"] == "chunk"])
        self.assertEqual(evs2[-1]["result"].output, "HELLO WORLD")

    def test_sync_generator_and_custom_aggregate_with_stream_reset(self):
        n = {"c": 0}

        def gen():
            n["c"] += 1
            yield {"t": 1}
            if n["c"] == 1:
                raise ConnectionError("dropped mid-stream")
            yield {"t": 2}

        s = step(gen, policy=Policy(retry=Retry(attempts=2, backoff=0),
                                    aggregate=lambda cs: sum(c["t"] for c in cs)))

        @flow
        async def f(ctx):
            return await s()

        evs = run(collect(f.stream()))
        names = [e["event"] for e in evs]
        self.assertIn("stream_reset", names)
        self.assertEqual(evs[-1]["result"].output, 3)

    def test_stream_surfaces_crash(self):
        class Boom(BaseException):
            pass

        @flow
        async def f(ctx):
            await ctx.step(lambda: 1, name="a")
            raise Boom()

        with self.assertRaises(Boom):
            run(collect(f.stream()))


class TestRuntime(unittest.TestCase):
    def test_sync_steps_do_not_block_parallel_branches(self):
        @step
        def slow(i):
            time.sleep(0.2)
            return i

        @flow
        async def f(ctx):
            return await ctx.map(slow, range(4), concurrency=4)

        t = time.perf_counter()
        self.assertEqual(run(f.run()).output, [0, 1, 2, 3])
        self.assertLess(time.perf_counter() - t, 0.6)

    def test_idempotency_key_stable_across_retries_and_resume(self):
        seen = []
        fail = {"n": 2}

        @step(policy=Policy(retry=Retry(attempts=2, backoff=0)))
        def charge():
            seen.append(idempotency_key())
            if fail["n"] > 0:
                fail["n"] -= 1
                raise ConnectionError()
            return "ok"

        @flow
        async def f(ctx):
            return await charge()

        self.assertEqual(run(f.run(run_id="pay")).status, "failed")
        self.assertEqual(run(f.resume("pay")).output, "ok")
        self.assertEqual(len(set(seen)), 1)
        self.assertEqual(seen[0], "pay:charge#0")
        with self.assertRaises(RuntimeError):
            idempotency_key()

    def test_now_and_uuid_are_deterministic_on_replay(self):
        @flow
        async def f(ctx):
            t, u = await ctx.now(), await ctx.uuid()
            await ctx.wait_for("go")
            return t, u

        run(f.run(run_id="d"))
        time.sleep(0.01)
        r = run(f.resume("d", signals={"go": 1}))
        r2 = run(f.resume("d"))
        self.assertEqual(r.output, r2.output)
        j = f.journal
        recorded = [e.result for e in j.load("d") if e.key in ("pylinxi.now#0", "pylinxi.uuid#0")]
        self.assertEqual(tuple(recorded), r.output)


class TestTimers(unittest.TestCase):
    def test_short_sleep_is_inline(self):
        @flow
        async def f(ctx):
            await ctx.sleep(0.05)
            return "awake"

        self.assertEqual(run(f.run()).output, "awake")

    def test_long_sleep_suspends_and_is_found_by_scheduler(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            await ctx.sleep(0.15, inline_max=0)
            return "awake"

        r = run(f.run(run_id="z"))
        self.assertEqual(r.status, "suspended")
        self.assertIsNone(r.waiting_for)
        self.assertEqual(j.pending_timers()[0][0], "z")
        self.assertEqual(j.pending_timers(due_before=time.time()), [])
        early = run(f.resume("z"))
        self.assertEqual((early.status, early.wake_at), ("suspended", r.wake_at))   # same deadline
        time.sleep(0.16)
        self.assertEqual([rid for rid, _ in j.pending_timers(due_before=time.time())], ["z"])
        self.assertEqual(run(f.resume("z")).output, "awake")
        self.assertEqual(j.pending_timers(), [])

    def test_wait_for_with_timeout(self):
        @flow
        async def f(ctx):
            return await ctx.wait_for("approval", timeout=0.1, default="auto-escalated")

        r = run(f.run(run_id="w1"))
        self.assertEqual((r.status, r.waiting_for), ("suspended", "approval"))
        self.assertIsNotNone(r.wake_at)
        self.assertEqual(run(f.resume("w1", signals={"approval": "yes"})).output, "yes")

        run(f.run(run_id="w2"))
        time.sleep(0.11)
        self.assertEqual(run(f.resume("w2")).output, "auto-escalated")
        self.assertEqual(run(f.resume("w2", signals={"approval": "late"})).output, "auto-escalated")


class TestEvolution(unittest.TestCase):
    def test_patched_keeps_old_runs_on_old_path(self):
        j = MemoryJournal()

        async def v1(ctx):
            a = await ctx.step(lambda: "a", name="a")
            await ctx.wait_for("go")
            b = await ctx.step(lambda: "b", name="b")
            await ctx.wait_for("finish")
            return [a, b]

        async def v2(ctx):                        # deploy adds a step between go and b
            a = await ctx.step(lambda: "a", name="a")
            await ctx.wait_for("go")
            extra = await ctx.step(lambda: "NEW", name="extra") if ctx.patched("add-extra") else None
            b = await ctx.step(lambda: "b", name="b")
            await ctx.wait_for("finish")
            return [a, b, extra]

        old, new = flow(v1, journal=j), flow(v2, journal=j)
        run(old.run(run_id="before_point"))                    # suspended at "go"
        run(old.run(run_id="past_point", signals={"go": 1}))   # already ran b, suspended at "finish"

        # never reached the patch point -> safely takes the new path
        r1 = run(new.resume("before_point", signals={"go": 1, "finish": 1}))
        self.assertEqual(r1.output, ["a", "b", "NEW"])
        # already past it -> keeps the old path, history replays without divergence
        r2 = run(new.resume("past_point", signals={"finish": 1}))
        self.assertEqual(r2.output, ["a", "b", None])
        self.assertFalse([e for e in r2.events if e["event"] == "divergence"])
        # brand-new runs always take the new path
        self.assertEqual(run(new.run(run_id="fresh", signals={"go": 1, "finish": 1})).output,
                         ["a", "b", "NEW"])

    def test_continue_as_new_keeps_journals_short(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def counter(ctx, n):
            for _ in range(3):
                n = await ctx.step(lambda x: x + 1, n, name="inc")
            ctx.state["total"] = n
            if n < 9:
                ctx.continue_as_new(n)
            return n

        r = run(counter.run(0, run_id="loop"))
        self.assertEqual((r.output, r.run_id), (9, "loop.g2"))
        self.assertEqual(r.state["total"], 9)
        self.assertEqual(sorted(j.runs()), ["loop", "loop.g1", "loop.g2"])
        self.assertTrue(all(len(j.load(x)) <= 6 for x in j.runs()))
        again = run(counter.resume("loop"))           # resuming the root follows the chain
        self.assertEqual((again.output, again.run_id), (9, "loop.g2"))


class TestJournalOps(unittest.TestCase):
    def _run_once(self, journal, rid, signals=None):
        @flow(journal=journal)
        async def f(ctx):
            await ctx.step(lambda: 1, name="a")
            return await ctx.wait_for("go")
        return run(f.run(run_id=rid, signals=signals))

    def test_key_rotation(self):
        with tempfile.TemporaryDirectory() as d:
            self._run_once(FileJournal(d, secret={"k1": b"old"}), "r")
            rotated = FileJournal(d, secret={"k1": b"old", "k2": b"new"})       # k2 now active
            self.assertEqual(self._run_once(rotated, "r", {"go": 5}).output, 5)
            kids = [e.kid for e in rotated.load("r")]
            self.assertEqual((kids[0], kids[-1]), ("k1", "k2"))
            self.assertTrue(rotated.verify("r", require_signed=True))
            self.assertFalse(FileJournal(d, secret={"k2": b"new"}).verify("r"))  # k1 retired too early
            self._run_once(FileJournal(d), "plain")
            self.assertTrue(rotated.verify("plain"))
            self.assertFalse(rotated.verify("plain", require_signed=True))

    def test_delete_on_every_backend(self):
        with tempfile.TemporaryDirectory() as d:
            for j in (MemoryJournal(), FileJournal(d), SQLiteJournal()):
                self._run_once(j, "gone")
                j.delete("gone")
                self.assertEqual((j.load("gone"), list(j.runs())), ([], []))
                self._run_once(j, "gone")              # id reusable, chain restarts cleanly
                self.assertTrue(j.verify("gone"))
                j.close()

    def test_open_journal_urls(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsInstance(open_journal("memory://"), MemoryJournal)
            self.assertIsInstance(open_journal(f"file://{d}"), FileJournal)
            for url in (f"sqlite:///{d}/x.db", f"sqlite:////{d.lstrip('/')}/y.db"):
                with open_journal(url) as j:
                    self.assertIsInstance(j, SQLiteJournal)
            with self.assertRaises(ValueError):
                open_journal("redis://x")


if __name__ == "__main__":
    unittest.main()
