"""Regression tests for bugs found in independent review (numbers match the review report)."""
import asyncio
import dataclasses
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from pylinxi import (
    CircuitBreaker,
    FileJournal,
    JournalCorrupt,
    MemoryJournal,
    Policy,
    Retry,
    RunLocked,
    SQLiteJournal,
    StepFailed,
    flow,
    step,
)
from pylinxi.cli import run_status
from pylinxi.worker import Worker


def run(coro):
    return asyncio.run(coro)


class TestJournalIntegrity(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    def test_01_torn_final_line_is_repaired_not_hiding_later_entries(self):
        j = FileJournal(self.d)

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: 1, name="a")
            v = await ctx.wait_for("go")
            await ctx.step(lambda: 2, name="b")
            return v

        run(f.run(run_id="r"))
        with open(Path(self.d) / "r.jsonl", "a") as fh:
            fh.write('{"p": "{\\"half')                     # crash mid-write
        self.assertEqual(run(f.resume("r", signals={"go": 7})).output, 7)
        self.assertEqual(run(f.resume("r")).output, 7)     # signal not lost, nothing hidden
        self.assertTrue(j.verify("r"))

    def test_02_many_instances_share_one_store(self):
        for make in (lambda: FileJournal(self.d), lambda: SQLiteJournal(f"{self.d}/x.db")):
            a, b = make(), make()
            for i in range(6):
                (a if i % 2 else b).append(__import__("pylinxi").Entry("shared", f"k{i}", "step", i))
            self.assertEqual([e.seq for e in a.load("shared")], list(range(6)))
            self.assertTrue(a.verify("shared"))
            b.delete("shared")
            a.append(__import__("pylinxi").Entry("shared", "again", "step", 0))
            self.assertEqual([e.seq for e in b.load("shared")], [0])
            self.assertTrue(b.verify("shared"))
            a.close()
            b.close()

    def test_02b_threads_appending_through_separate_instances(self):
        def writer(i):
            j = SQLiteJournal(f"{self.d}/t.db")
            for n in range(20):
                j.append(__import__("pylinxi").Entry("t", f"w{i}#{n}", "step", n))
            j.close()

        ts = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        with SQLiteJournal(f"{self.d}/t.db") as j:
            self.assertEqual([e.seq for e in j.load("t")], list(range(80)))
            self.assertTrue(j.verify("t"))

    def test_02c_simultaneous_open_of_a_fresh_sqlite_file(self):
        # CI found: concurrent PRAGMA journal_mode=WAL / CREATE TABLE raised "database is locked"
        path, n = f"{self.d}/race.db", 8
        barrier, errors = threading.Barrier(n), []

        def opener(i):
            try:
                barrier.wait()
                j = SQLiteJournal(path)
                j.append(__import__("pylinxi").Entry("r", f"o{i}", "step", i))
                j.close()
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

        ts = [threading.Thread(target=opener, args=(i,)) for i in range(n)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        with SQLiteJournal(path) as j:
            self.assertEqual([e.seq for e in j.load("r")], list(range(n)))
            self.assertTrue(j.verify("r"))

    def test_16_codec_is_lossless_and_strict(self):
        j = MemoryJournal()

        @dataclasses.dataclass
        class Unregistered:
            id: int

        @flow(journal=j)
        async def f(ctx):
            a = await ctx.step(lambda: {1: "a", (2, 3): "b"}, name="a")
            t = await ctx.step(lambda: {"__t__": "tuple", "v": ["x"]}, name="t")
            await ctx.wait_for("go")
            return a, t

        run(f.run(run_id="r"))
        out = run(f.resume("r", signals={"go": 1})).output
        self.assertEqual(out, ({1: "a", (2, 3): "b"}, {"__t__": "tuple", "v": ["x"]}))

        @flow
        async def g(ctx):
            return await ctx.step(lambda: Unregistered(1), name="u")
        r = run(g.run())
        self.assertEqual(r.status, "failed")
        self.assertIn("register", str(r.error))

        payload, sig = j._data["r"][1]
        j._data["r"][1] = (payload.replace('"seq":1', '"seq":1,"evil":1'), sig)
        with self.assertRaises(JournalCorrupt):
            j.load("r")
        self.assertFalse(j.verify("r"))

    def test_18_redaction_reaches_tuples_and_dataclasses(self):
        @dataclasses.dataclass
        class Conn:
            host: str
            api_key: str
        __import__("pylinxi").default_codec.register(Conn)
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            ctx.state["conns"] = (Conn("db", "sk-DATACLASS"), {"token": "sk-TUPLE"})
            await ctx.step(lambda: 1, name="a")

        run(f.run(run_id="r"))
        raw = str(j._data["r"])
        self.assertNotIn("sk-DATACLASS", raw)
        self.assertNotIn("sk-TUPLE", raw)

    def test_19_same_run_cannot_execute_twice_concurrently(self):
        for j in (MemoryJournal(), SQLiteJournal(f"{self.d}/l.db"), FileJournal(self.d)):
            sent = []

            @flow(journal=j)
            async def f(ctx):
                await ctx.step(lambda: sent.append(1), name="send")
                await asyncio.sleep(0.05)

            async def both():
                return await asyncio.gather(f.run(run_id="r"), f.run(run_id="r"), return_exceptions=True)
            results = run(both())
            self.assertEqual(sum(isinstance(r, RunLocked) for r in results), 1)
            self.assertEqual(len(sent), 1)
            j.close()

    def test_19b_lock_spans_journal_instances(self):
        a, b = SQLiteJournal(f"{self.d}/m.db"), SQLiteJournal(f"{self.d}/m.db")
        with a.lock("r"), self.assertRaises(RunLocked), b.lock("r"):
            pass
        with b.lock("r"):
            pass
        a.close()
        b.close()

    def test_20_verify_never_raises_and_anchor_detects_truncation(self):
        j = FileJournal(self.d)

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: 1, name="a")
            await ctx.step(lambda: 2, name="b")

        run(f.run(run_id="r"))
        anchor = j.head("r")
        p = Path(self.d) / "r.jsonl"
        lines = p.read_text().splitlines(keepends=True)
        p.write_text("".join(lines[:-1]))                  # attacker drops the newest entry
        self.assertTrue(j.verify("r"))                     # undetectable without an anchor...
        self.assertFalse(j.verify("r", anchor=anchor))     # ...detected with one
        p.write_text(lines[0] + "garbage\n" + "".join(lines[1:]))
        self.assertFalse(j.verify("r"))
        with self.assertRaises(JournalCorrupt):
            j.load("r")


class TestReplay(unittest.TestCase):
    def test_03_secret_inputs_are_rejected_not_leaked(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx, user, password=None):
            return user

        with self.assertRaises(ValueError) as cm:
            run(f.run("ana", password="hunter2", state={"apiKey": "sk"}))
        self.assertIn("password", str(cm.exception))
        self.assertIn("apiKey", str(cm.exception))
        self.assertEqual(list(j.runs()), [])               # nothing written
        self.assertEqual(run(f.run("ana", state={"max_tokens": 5})).output, "ana")   # no false positive

        @flow(journal=MemoryJournal(redact_pattern=None))
        async def g(ctx, token):
            return token
        self.assertEqual(run(g.run(token="t")).output, "t")

    def test_04_nested_step_calls_are_plain_calls(self):
        @step
        def inner(x):
            return f"inner({x})"

        @step
        def outer():
            return inner("nested")

        @flow
        async def f(ctx):
            await outer()
            await ctx.wait_for("go")
            return await inner("top")

        run(f.run(run_id="r"))
        self.assertEqual(run(f.resume("r", signals={"go": 1})).output, "inner(top)")

    def test_05_concurrent_branches_replay_by_branch_not_by_timing(self):
        delay = {"alice": 0.03, "bob": 0.0}

        @step
        async def fetch(who):
            await asyncio.sleep(delay[who])
            return who

        @step
        def summarize(x):
            return f"summary of {x}"

        @flow
        async def f(ctx):
            async def branch(who):
                return await summarize(await fetch(who))
            out = await asyncio.gather(branch("alice"), branch("bob"))
            await ctx.wait_for("go")
            return out

        run(f.run(run_id="r"))
        delay.update(alice=0.0, bob=0.03)                  # opposite finishing order on replay
        self.assertEqual(run(f.resume("r", signals={"go": 1})).output, ["summary of alice", "summary of bob"])

    def test_06_suspending_branch_cancels_siblings(self):
        effects = []

        @flow
        async def f(ctx):
            async def slow():
                await asyncio.sleep(0.05)
                await ctx.step(lambda: effects.append(1), name="side_effect")
            await asyncio.gather(ctx.wait_for("approve"), slow())

        r = run(f.run(run_id="r"))
        self.assertEqual(r.status, "suspended")
        time.sleep(0.08)
        run(f.resume("r", signals={"approve": True}))
        self.assertEqual(len(effects), 1)

    def test_07_caught_step_failure_replays_as_the_same_failure(self):
        effects = []
        card_ok = {"v": False}

        @step
        def charge():
            effects.append("charge")
            if not card_ok["v"]:
                raise ValueError("declined")
            return "paid"

        @flow
        async def checkout(ctx):
            try:
                await charge()
            except StepFailed:
                await ctx.step(lambda: effects.append("sorry-email"), name="email")
                await ctx.wait_for("ack")
                return "failed-path"
            return "paid-path"

        run(checkout.run(run_id="c"))
        card_ok["v"] = True
        r = run(checkout.resume("c", signals={"ack": 1}))
        self.assertEqual(r.output, "failed-path")
        self.assertEqual(effects, ["charge", "sorry-email"])
        self.assertFalse([e for e in r.events if e["event"] == "divergence"])

    def test_07b_uncaught_failure_is_still_retried_on_resume(self):
        ok = {"v": False}

        @step
        def flaky():
            if not ok["v"]:
                raise ConnectionError()
            return 1

        @flow
        async def f(ctx):
            return await flaky()

        self.assertEqual(run(f.run(run_id="f")).status, "failed")
        ok["v"] = True
        self.assertEqual(run(f.resume("f")).output, 1)

    def test_08_inline_sleep_then_signal_is_waiting_not_a_due_timer(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            await ctx.sleep(0.01)
            return await ctx.wait_for("ok")

        run(f.run(run_id="r"))
        self.assertEqual(j.pending_timers(), [])
        self.assertEqual(run_status(j.load("r")), "waiting")
        self.assertEqual(run(Worker(j, [f]).tick()).resumed, [])

    def test_09_10_11_continue_as_new_chain_is_durable(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def loop(ctx, n):
            msg = await ctx.wait_for("msg")
            if n < 2:
                ctx.continue_as_new(n + 1)
            return (n, msg)

        run(loop.run(0, run_id="build.g1"))                # user id that looks like a generation
        r = run(loop.resume("build.g1", signals={"msg": "a"}))
        self.assertEqual(r.run_id, "build.g1.g1")
        j.delete("build.g1.g1")                            # crash before next generation started
        r = run(loop.resume("build.g1", signals={"msg": "b"}))   # signals kept when following chain
        self.assertEqual((r.run_id, r.status), ("build.g1.g2", "suspended"))
        r = run(loop.resume("build.g1", signals={"msg": "c"}))
        self.assertEqual(r.output, (2, "c"))

    def test_12_suspension_cannot_be_swallowed(self):
        @flow
        async def f(ctx):
            try:
                approved = await ctx.wait_for("approval")
            except Exception:  # noqa: BLE001
                approved = None
            return approved

        self.assertEqual(run(f.run()).status, "suspended")

    def test_13_patched_counts_sensitive_and_failed_history(self):
        log = []
        j = MemoryJournal()

        async def v1(ctx):
            await ctx.wait_for("go")
            await ctx.step(lambda: log.append("creds"), name="creds", sensitive=True)
            await ctx.wait_for("done")

        async def v2(ctx):
            await ctx.wait_for("go")
            if ctx.patched("check"):
                log.append("NEW")
            await ctx.step(lambda: log.append("creds"), name="creds", sensitive=True)
            await ctx.wait_for("done")

        run(flow(v1, journal=j).run(run_id="r", signals={"go": 1}))
        run(flow(v2, journal=j).resume("r", signals={"done": 1}))
        self.assertNotIn("NEW", log)

    def test_14_worker_ignores_failures_the_flow_handled(self):
        j = MemoryJournal()
        n = {"c": 0}

        def charge():
            n["c"] += 1
            raise ValueError("declined")

        @flow(journal=j)
        async def f(ctx):
            try:
                await ctx.step(charge, name="charge")
            except StepFailed:
                return await ctx.wait_for("new_card")

        run(f.run(run_id="c"))
        w = Worker(j, [f], retry_failed=True, backoff=0)
        for _ in range(3):
            self.assertEqual(run(w.tick()).resumed, [])
        self.assertEqual(n["c"], 1)

    def test_15_unserializable_output_fails_cleanly_and_worker_continues(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def bad(ctx):
            await ctx.sleep(0.01, inline_max=0)
            return object()

        @flow(journal=j)
        async def good(ctx):
            await ctx.sleep(0.01, inline_max=0)
            return "ok"

        run(bad.run(run_id="a-bad"))
        run(good.run(run_id="b-good"))
        time.sleep(0.02)
        rep = run(Worker(j, [bad, good]).tick())
        self.assertEqual(rep.completed, ["b-good"])
        self.assertEqual(run_status(j.load("a-bad")), "failed")

    def test_24_step_params_named_like_options(self):
        @step
        async def greet(name, policy="x", sensitive=False):
            return f"hi {name} {policy} {sensitive}"

        @flow
        async def f(ctx):
            return await greet(name="bob", policy="p", sensitive=True)

        self.assertEqual(run(f.run()).output, "hi bob p True")

    def test_27_signals_queue_until_consumed(self):
        @flow
        async def f(ctx):
            a = await ctx.wait_for("manager")
            b = await ctx.wait_for("legal")
            c = await ctx.wait_for("legal")
            return a, b, c

        run(f.run(run_id="r", signals={"legal": "L1"}))    # arrives early
        run(f.resume("r", signals={"legal": "L2"}))
        self.assertEqual(run(f.resume("r", signals={"manager": "M"})).output, ("M", "L1", "L2"))


class TestPolicy(unittest.TestCase):
    def test_17_validation_errors_never_contain_the_value(self):
        j = MemoryJournal()
        n = {"c": 0}

        def token():
            n["c"] += 1
            return "SECRET-abc" if n["c"] == 1 else "ok"

        @flow(journal=j)
        async def f(ctx):
            return await ctx.step(token, name="tok", sensitive=True,
                                  policy=Policy(retry=Retry(attempts=2, backoff=0), validate=lambda v: v == "ok"))

        r = run(f.run(run_id="r"))
        self.assertNotIn("SECRET", json.dumps(r.events, default=str))
        self.assertNotIn("SECRET", str(j._data["r"]))

    def test_22_23_stream_only_shows_the_live_attempt(self):
        def gen():
            for i in range(4):
                time.sleep(0.03)
                yield f"A{i}"

        s = step(gen, policy=Policy(timeout=0.05, retry=Retry(attempts=2, backoff=0),
                                    fallback=lambda: "fallback"))

        @flow
        async def f(ctx):
            return await s()

        async def collect():
            return [e async for e in f.stream()]
        evs = run(collect())
        time.sleep(0.15)                                   # let abandoned threads try to emit
        seen = [e.get("chunk", f"<{e['event']}>") for e in evs if e["event"] in ("chunk", "stream_reset")]
        # every partial stream is followed by a reset before anything else is shown
        last_reset = max(i for i, x in enumerate(seen) if x == "<stream_reset>")
        self.assertTrue(all(x == "<stream_reset>" or i < last_reset for i, x in enumerate(seen)))
        self.assertEqual(evs[-1]["result"].output, "fallback")

    def test_25_raising_fallback_is_a_step_failure(self):
        j = MemoryJournal()
        s = step(lambda: 1 / 0, name="lookup",
                 policy=Policy(fallback=lambda: {}["missing"]))

        @flow(journal=j)
        async def f(ctx):
            return await s()

        r = run(f.run(run_id="r"))
        self.assertIsInstance(r.error, StepFailed)
        self.assertIn(("failed", "lookup#0"), [(e.kind, e.key) for e in j.load("r")])
        self.assertIn("step_failed", [e["event"] for e in r.events])

    def test_breaker_half_open_allows_a_single_trial(self):
        br = CircuitBreaker(threshold=1, reset_after=0.02)
        self.assertTrue(br.allow())
        br.failure()
        self.assertFalse(br.allow())
        time.sleep(0.03)
        self.assertEqual([br.allow() for _ in range(3)], [True, False, False])
        br.failure()                                       # late failure from an old call while open
        self.assertEqual(br.state, "open")
        time.sleep(0.03)
        self.assertTrue(br.allow())
        br.success()
        self.assertEqual(br.state, "closed")


if __name__ == "__main__":
    unittest.main()
