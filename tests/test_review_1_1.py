"""Regression tests for the pylinxi 1.0.0 production-readiness review.

Numbers match the review (#1-#33, M1-M10); "missed" tests cover bugs found while
verifying it. Each test reproduces the reported failure and checks the fix.
"""
import asyncio
import contextlib
import dataclasses
import datetime as dt
import decimal
import enum
import io
import json
import os
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
from pathlib import Path

import pylinxi
from pylinxi import (
    AttemptStillRunning,
    CircuitBreaker,
    Codec,
    Entry,
    FileJournal,
    FlowMismatch,
    JournalCorrupt,
    MemoryJournal,
    Policy,
    ReplayDivergence,
    ResultNotJournaled,
    Retry,
    RunExists,
    RunLocked,
    SQLiteJournal,
    StepFailed,
    UnknownRun,
    Worker,
    default_codec,
    flow,
    idempotency_key,
    step,
)
from pylinxi.cli import main as cli_main


def run(coro):
    return asyncio.run(coro)


def kinds(j, rid):
    return [(e.kind, e.key) for e in j.load(rid)]


def cli(*argv, env=None):
    out, err = io.StringIO(), io.StringIO()
    old = {k: os.environ.get(k) for k in (env or {})}
    os.environ.update(env or {})
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(list(argv))
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return code, out.getvalue(), err.getvalue()


class TmpDir(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.d = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()


# ======================================================================== P0
class TestP0(TmpDir):
    def test_01_cleanup_after_uncaught_failure_does_not_mark_it_caught(self):
        calls = {"charge": 0}
        down = {"v": True}

        @step
        def charge():
            calls["charge"] += 1
            if down["v"]:
                raise ConnectionError("payment API down")
            return "paid"

        @step
        def cleanup():
            return "cleaned"

        class Res:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                await cleanup()          # a step during exception propagation

        @flow
        async def f(ctx):
            try:
                async with Res():
                    return await charge()
            finally:
                await ctx.step(lambda: 1, name="audit")

        self.assertEqual(run(f.run(run_id="r")).status, "failed")
        down["v"] = False
        r = run(f.resume("r"))
        self.assertEqual((r.status, r.output), ("completed", "paid"))
        self.assertEqual(calls["charge"], 2)                  # retried, not replayed as "caught"

    def test_01b_a_genuinely_caught_failure_still_replays_as_caught(self):
        calls = {"charge": 0}

        @step
        def charge():
            calls["charge"] += 1
            if calls["charge"] == 1:
                raise ValueError("declined")
            return "paid"

        @flow
        async def f(ctx):
            try:
                await charge()
            except StepFailed:
                await ctx.wait_for("retry_card")
                return "declined-path"
            return "paid-path"

        run(f.run(run_id="r"))
        self.assertEqual(run(f.resume("r", signals={"retry_card": 1})).output, "declined-path")
        self.assertEqual(calls["charge"], 1)

    def test_02_non_retryable_errors_go_through_fallback_breaker_and_stepfailed(self):
        br = CircuitBreaker(threshold=1, reset_after=60)
        seen = []

        @step(policy=Policy(retry=Retry(attempts=3, backoff=0, on=(ConnectionError,)), breaker=br))
        def parse():
            raise ValueError("bad payload")

        @step(policy=Policy(retry=Retry(attempts=3, backoff=0, on=(ConnectionError,)),
                            fallback=lambda: "fallback"))
        def parse_fb():
            seen.append(1)
            raise ValueError("bad payload")

        @flow
        async def f(ctx):
            try:
                await parse()
            except StepFailed as e:
                first = type(e.error).__name__
            return first, await parse_fb()

        j = f.journal
        r = run(f.run(run_id="r"))
        self.assertEqual(r.output, ("ValueError", "fallback"))
        self.assertEqual(len(seen), 1)                        # not retried
        self.assertEqual(br.state, "open")                    # breaker was told
        self.assertIn(("failed", "parse#0"), kinds(j, "r"))

    def test_03_breaker_trial_cancelled_or_non_retryable_never_sticks(self):
        br = CircuitBreaker(threshold=1, reset_after=0.01)
        br.failure()
        time.sleep(0.02)

        async def slow():
            await asyncio.sleep(5)

        @flow
        async def f(ctx):
            t = asyncio.ensure_future(ctx.step(slow, policy=Policy(breaker=br), name="slow"))
            await asyncio.sleep(0.01)
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t

        run(f.run())
        self.assertTrue(br.allow())                            # cancelled trial gave its slot back
        br.release_trial()

        @flow
        async def g(ctx):
            with contextlib.suppress(StepFailed):
                await ctx.step(lambda: 1 / 0, policy=Policy(breaker=br), name="div")

        br2 = CircuitBreaker(threshold=1, reset_after=0.01)
        br2.failure()
        time.sleep(0.02)
        br = br2
        run(g.run())
        self.assertEqual(br2.state, "open")                    # failed trial re-opened, no stuck _trial
        time.sleep(0.02)
        self.assertTrue(br2.allow())

    def test_04_forged_entries_are_refused_on_load(self):
        refunds = []
        j = MemoryJournal(secret=b"k")

        @flow(journal=j)
        async def refund(ctx, order):
            if await ctx.wait_for("approval"):
                await ctx.step(lambda: refunds.append(order), name="refund")
            return "done"

        run(refund.run("A-1", run_id="t1"))
        # attacker with DB access appends an unsigned "approval" inbox entry
        rows = j._data["t1"]
        forged = Entry("t1", "inbox", "inbox", {"approval": [True]})
        forged.seq, forged.prev = len(rows), rows[-1][1]
        payload = j._serialize(forged)
        rows.append((payload, "0" * 64))
        with self.assertRaises(JournalCorrupt):
            run(refund.resume("t1"))
        self.assertEqual(refunds, [])
        self.assertFalse(j.verify("t1"))

    def test_04b_edits_in_an_unkeyed_journal_are_detected_on_load(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: 100, name="amount")
            await ctx.wait_for("go")

        run(f.run(run_id="r"))
        p, s = j._data["r"][1]
        j._data["r"][1] = (p.replace("100", "999"), s)
        with self.assertRaises(JournalCorrupt):
            j.load("r")

    def test_05_rows_copied_to_another_run_id_are_refused(self):
        j = SQLiteJournal(f"{self.d}/j.db", secret=b"k")

        @flow(journal=j)
        async def f(ctx, who):
            await ctx.wait_for("ok")
            return await ctx.step(lambda: f"refund {who} {idempotency_key()}", name="refund")

        run(f.run("alice", run_id="A", signals={"ok": 1}))
        j._db.execute("INSERT INTO pylinxi_journal SELECT 'B', seq, payload, sig FROM pylinxi_journal "
                      "WHERE run_id='A'")
        self.assertFalse(j.verify("B", require_signed=True))
        with self.assertRaises(JournalCorrupt):
            run(f.resume("B"))
        j.close()

    def test_06_resume_unknown_run_raises_and_writes_nothing(self):
        @flow
        async def f(ctx, order="DEFAULT"):
            return await ctx.step(lambda: f"charged {order}", name="charge")

        with self.assertRaises(UnknownRun):
            run(f.resume("t-typo"))
        self.assertEqual(list(f.journal.runs()), [])
        self.assertEqual(run(f.run("A-1", run_id="t-typo")).output, "charged A-1")

    def test_07_run_id_of_another_flow_is_refused(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def flow_a(ctx, who):
            await ctx.wait_for("go")
            return await ctx.step(lambda: f"charged {who}", name="charge")

        @flow(journal=j)
        async def flow_b(ctx, who):
            return await ctx.step(lambda: f"charged {who}", name="charge")

        run(flow_a.run("alice", run_id="s"))
        with self.assertRaises(FlowMismatch):
            run(flow_b.run("bob", run_id="s"))
        with self.assertRaises(FlowMismatch):
            run(flow_b.resume("s"))

    def test_08_builtin_types_round_trip(self):
        values = {"d": decimal.Decimal("9.99"), "t": dt.datetime(2026, 11, 6, 9, 30, tzinfo=dt.timezone.utc),
                  "day": dt.date(2026, 11, 6), "u": uuid.UUID(int=7), "td": dt.timedelta(seconds=90),
                  "clock": dt.time(9, 30)}

        @flow
        async def f(ctx):
            got = await ctx.step(lambda: dict(values), name="v")
            await ctx.wait_for("go")
            return got

        run(f.run(run_id="r"))
        out = run(f.resume("r", signals={"go": 1})).output
        self.assertEqual(out, values)
        self.assertIsInstance(out["d"], decimal.Decimal)

    def test_08b_unjournalable_result_never_repeats_the_side_effect(self):
        charges = []

        @step
        def charge():
            charges.append(1)
            return object()                                    # can't be journaled

        @flow
        async def f(ctx):
            await charge()

        r = run(f.run(run_id="r"))
        self.assertEqual(r.status, "failed")
        self.assertIsInstance(r.error.error, ResultNotJournaled)
        for _ in range(3):
            r = run(f.resume("r"))
            self.assertIsInstance(r.error.error, ResultNotJournaled)
        self.assertEqual(len(charges), 1)

    def test_08c_missed_unjournalable_state_never_repeats_the_side_effect(self):
        charges = []

        @flow
        async def f(ctx):
            ctx.state["conn"] = threading.Lock()               # not journalable
            await ctx.step(lambda: charges.append(1), name="charge")
            await ctx.wait_for("go")
            return "ok"

        events = []
        run(f.run(run_id="r", on_event=events.append))
        self.assertEqual(run(f.resume("r", signals={"go": 1})).output, "ok")
        self.assertEqual(len(charges), 1)
        self.assertIn("state_unserializable", [e["event"] for e in events])

    def test_09_complete_last_line_without_newline_is_kept(self):
        j = FileJournal(self.d)
        pays = []

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: pays.append(1), name="pay")
            await ctx.wait_for("go")

        run(f.run(run_id="r"))
        p = Path(self.d) / "r.jsonl"
        lines = p.read_text().splitlines()
        p.write_text("\n".join(lines[:2]))                     # start + pay, newline lost in a crash
        run(f.resume("r"))
        run(f.resume("r", signals={"go": 1}))
        self.assertEqual(len(pays), 1)
        self.assertTrue(j.verify("r"))

    def test_10_delete_takes_the_run_lock_and_deleted_runs_stay_deleted(self):
        j = MemoryJournal()
        started = threading.Event()

        @flow(journal=j)
        async def f(ctx):
            started.set()
            await ctx.step(lambda: time.sleep(0.2), name="slow")
            return "done"

        async def both():
            t = asyncio.ensure_future(f.run(run_id="r"))
            while not started.is_set():
                await asyncio.sleep(0.005)
            with self.assertRaises(RunLocked):
                j.delete("r")
            return await t

        self.assertEqual(run(both()).status, "completed")
        j.delete("r")
        with self.assertRaises(UnknownRun):                    # a late write can't resurrect it
            j.append(Entry("r", "slow#0", "step", 1))
        self.assertEqual(list(j.runs()), [])


# ======================================================================== P1
class TestP1(TmpDir):
    def test_12_earlier_run_failure_is_not_replay_history(self):
        ok = {"v": False}

        @flow
        async def f(ctx):
            await ctx.step(lambda: (_ for _ in ()).throw(ConnectionError()) if not ok["v"] else 1, name="a")
            assert not ctx.replaying
            return ctx.patched("new-point")

        run(f.run(run_id="r"))
        ok["v"] = True
        r = run(f.resume("r"))
        self.assertEqual(r.output, True)
        self.assertFalse([e for e in r.events if e["event"] == "divergence"])

    def test_13_suspends_at_the_earliest_deadline_of_all_branches(self):
        @flow
        async def f(ctx):
            await asyncio.gather(ctx.sleep(3600), ctx.wait_for("x", timeout=60))

        r = run(f.run())
        self.assertAlmostEqual(r.wake_at - time.time(), 60, delta=2)

        @flow
        async def g(ctx):
            await asyncio.gather(ctx.wait_for("approval"), ctx.sleep(60))

        r = run(g.run())
        self.assertIsNotNone(r.wake_at)
        self.assertEqual(r.waiting_for, "approval")
        self.assertEqual(len(g.journal.pending_timers()), 1)

    def test_13b_branch_still_working_reaches_its_timer_before_the_run_suspends(self):
        @step
        async def work():
            await asyncio.sleep(0.05)
            return 1

        @flow
        async def f(ctx):
            async def b():
                await work()
                await ctx.sleep(30)
            await asyncio.gather(ctx.wait_for("approval"), b())

        r = run(f.run(run_id="r"))
        self.assertAlmostEqual(r.wake_at - time.time(), 30, delta=2)
        self.assertIn(("step", "t1/work#0"), kinds(f.journal, "r"))

    def test_14_registered_enums_come_back_as_the_same_enum(self):
        class Intent(str, enum.Enum):
            REFUND = "refund"
            FAQ = "faq"

        class Prio(enum.Enum):
            HIGH = 1

        default_codec.register(Intent)
        default_codec.register(Prio)

        @flow
        async def f(ctx):
            i = await ctx.step(lambda: Intent.REFUND, name="classify")
            p = await ctx.step(lambda: Prio.HIGH, name="prio")
            await ctx.wait_for("go")
            return i is Intent.REFUND and p is Prio.HIGH

        run(f.run(run_id="r"))
        self.assertTrue(run(f.resume("r", signals={"go": 1})).output)

        class Unreg(str, enum.Enum):
            A = "a"
        with self.assertRaises(TypeError):
            Codec().encode(Unreg.A)

    def test_15_same_class_name_in_two_modules_never_collides(self):
        c = Codec()
        a = types.ModuleType("billing")
        b = types.ModuleType("crm")
        exec("from dataclasses import dataclass\n@dataclass\nclass Config:\n    x: int", a.__dict__)
        exec("from dataclasses import dataclass\n@dataclass\nclass Config:\n    x: int", b.__dict__)
        c.register(a.Config)
        c.register(b.Config)
        self.assertIs(type(c.loads(c.dumps(a.Config(1)))), a.Config)
        self.assertIs(type(c.loads(c.dumps(b.Config(1)))), b.Config)
        with self.assertRaises(JournalCorrupt):                # 1.0-style bare tag is ambiguous now
            c.decode({"__t__": "Config", "v": {"x": 1}})

    def test_16_leftover_signals_survive_a_crash_between_generations(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def loop(ctx, gen):
            if gen == 0:
                await ctx.wait_for("go")
                ctx.continue_as_new(1)
            return await ctx.wait_for("cancel")

        run(loop.run(0, run_id="m"))
        run(loop.resume("m", signals={"go": 1, "cancel": "yes"}))
        j.delete("m.g1", chain=False)                          # crash before gen 1 started
        self.assertEqual(run(loop.resume("m")).output, "yes")

    def test_17_resume_by_root_is_constant_time(self):
        loads = {"n": 0}

        class Counting(MemoryJournal):
            def load(self, rid):
                loads["n"] += 1
                return super().load(rid)

        j = Counting()

        @flow(journal=j)
        async def mon(ctx, n):
            if n < 300:
                ctx.continue_as_new(n + 1)
            return await ctx.wait_for("stop")

        run(mon.run(0, run_id="m"))
        loads["n"] = 0
        r = run(mon.resume("m"))
        self.assertEqual((r.status, r.run_id), ("suspended", "m.g300"))
        self.assertLessEqual(loads["n"], 4)        # + the previous generation, for stranded signals

    def test_18_replaced_task_factory_is_wrapped_again(self):
        delays = {"a": 0.03, "b": 0.0}
        calls = []

        @step
        async def fetch(k):
            calls.append(k)
            await asyncio.sleep(delays[k])
            return k

        @flow
        async def f(ctx):
            async def branch(k):                               # steps called inside real branches
                return await fetch(k)
            out = await asyncio.gather(branch("a"), branch("b"))
            await ctx.wait_for("go")
            return out

        async def main():
            await f.run(run_id="r")
            asyncio.get_running_loop().set_task_factory(None)   # app replaces the factory
            delays.update(a=0.0, b=0.03)
            return (await f.resume("r", signals={"go": 1})).output
        self.assertEqual(run(main()), ["a", "b"])
        self.assertEqual(sorted(calls), ["a", "b"])            # replayed by branch, not re-run

    def test_19_flow_run_inside_a_step_is_its_own_durable_run(self):
        j = MemoryJournal()
        sent = []

        @flow(journal=j)
        async def child(ctx, x):
            await ctx.step(lambda: sent.append(x), name="send")
            return x * 2

        @step
        async def call_child(x):
            return (await child.run(x, run_id=f"child-{x}")).output

        @flow(journal=j)
        async def parent(ctx):
            return await call_child(21)

        self.assertEqual(run(parent.run(run_id="p")).output, 42)
        self.assertIn(("step", "send#0"), kinds(j, "child-21"))
        self.assertEqual(sent, [21])

    def test_20_divergence_stops_before_the_new_step_runs(self):
        j = MemoryJournal()
        emails = []

        async def v1(ctx):
            await ctx.step(lambda: emails.append("v1"), name="send_email_v1")
            await ctx.wait_for("go")

        async def v2(ctx):
            await ctx.step(lambda: emails.append("v2"), name="send_email_v2")
            await ctx.wait_for("go")

        run(flow(v1, journal=j, name="f").run(run_id="r"))
        r = run(flow(v2, journal=j, name="f").resume("r", signals={"go": 1}))
        self.assertEqual(r.status, "failed")
        self.assertIsInstance(r.error, ReplayDivergence)
        self.assertEqual(emails, ["v1"])                       # nothing new ran

        events = []
        j2 = MemoryJournal()
        run(flow(v1, journal=j2, name="f").run(run_id="r"))
        run(flow(v2, journal=j2, name="f", strict=False).resume("r", signals={"go": 1}, on_event=events.append))
        first = next(i for i, e in enumerate(events) if e["event"] == "divergence")
        started = next(i for i, e in enumerate(events) if e["event"] == "started")
        self.assertLess(first, started)                        # reported before the step ran

    def test_21_verify_needs_neither_registered_classes_nor_a_stable_codec(self):
        @dataclasses.dataclass
        class Ticket:
            id: int
        default_codec.register(Ticket)
        path = f"{self.d}/j.db"
        j = SQLiteJournal(path, secret={"k1": b"s"})

        @flow(journal=j)
        async def f(ctx):
            return await ctx.step(lambda: Ticket(7), name="t")

        run(f.run(run_id="r"))
        j.close()
        plain = SQLiteJournal(path, secret={"k1": b"s"}, codec=Codec())   # knows no classes
        self.assertTrue(plain.verify("r", require_signed=True))
        plain.close()
        code, out, _ = cli("--journal", f"sqlite:///{path}", "verify", "r", env={"PYLINXI_KEYS": "k1=s"})
        self.assertEqual(code, 0, out)

    def test_22_verify_fails_for_a_missing_run(self):
        j = SQLiteJournal(f"{self.d}/j.db")
        self.assertFalse(j.verify("ticket-1010"))
        j.close()
        code, out, _ = cli("--journal", f"sqlite:///{self.d}/j.db", "verify", "ticket-1010")
        self.assertEqual(code, 1)
        self.assertIn("FAIL", out)

    def test_23_entries_from_a_newer_schema_load_and_verify(self):
        j = MemoryJournal(secret=b"k")

        @flow(journal=j)
        async def f(ctx):
            await ctx.wait_for("go")

        run(f.run(run_id="r"))
        # a future pylinxi writes an entry with a new field
        e = Entry("r", "note#0", "marker", True)
        rows = j._data["r"]
        body = {**e.body(), "seq": len(rows), "prev": rows[-1][1], "kid": "default", "v": 3, "trace_id": "abc"}
        payload = j.codec.dumps(body)
        rows.append((payload, j._sign(j._msg("r", rows[-1][1], payload, 3), "default")))
        self.assertTrue(j.verify("r", require_signed=True))
        self.assertEqual(j.load("r")[-1].v, 3)


# ======================================================================== security
class TestSecurity(TmpDir):
    def test_24_delete_removes_every_generation(self):
        path = f"{self.d}/j.db"
        j = SQLiteJournal(path)

        @flow(journal=j)
        async def f(ctx, n, email):
            if n < 3:
                ctx.continue_as_new(n + 1, email)
            return n

        run(f.run(0, "ana@example.com", run_id="user-42"))
        self.assertEqual(sorted(j.delete("user-42.g2")), ["user-42", "user-42.g1", "user-42.g2", "user-42.g3"])
        self.assertEqual(list(j.runs()), [])
        run(f.run(0, "bo@example.com", run_id="user-43"))
        j.close()
        code, out, _ = cli("--journal", f"sqlite:///{path}", "delete", "user-43", "--yes")
        self.assertEqual(code, 0)
        self.assertEqual(out.count("deleted"), 4)

    def test_25_secret_filter_matches_credentials_not_ordinary_names(self):
        @flow
        async def f(ctx, **kw):
            return sorted(kw)

        ok = ["token_budget", "auth_required", "pass_count", "session_id", "max_tokens", "token_count",
              "password_reset_sent", "secret_note"]
        self.assertEqual(run(f.run(**{k: 1 for k in ok})).output, sorted(ok))
        for bad in ["password", "access_token", "apiKey", "client_secret", "x-api-key", "auth", "ssn"]:
            with self.assertRaises(ValueError, msg=bad):
                run(f.run(**{bad: "x"}))

    def test_25b_missed_continue_as_new_with_secret_state_fails_at_the_call(self):
        @flow
        async def f(ctx):
            ctx.state["api_key"] = "sk"
            ctx.continue_as_new()

        r = run(f.run())
        self.assertEqual(r.status, "failed")
        self.assertIn("continue_as_new", str(r.error))

    def _signed_run(self, keys):
        path = f"{self.d}/s.db"
        j = SQLiteJournal(path, secret=keys)

        @flow(journal=j)
        async def f(ctx):
            return await ctx.step(lambda: 1, name="a")
        run(f.run(run_id="r"))
        j.close()
        # attacker rewrites the run as an unsigned chain
        u = SQLiteJournal(path, require_signed=False, verify_on_load=False)
        u._db.execute("DELETE FROM pylinxi_journal WHERE run_id='r2'")
        u.close()
        u2 = SQLiteJournal(path)                               # unkeyed writer
        run(flow(f.fn, journal=u2, name="f").run(run_id="r2"))
        u2.close()
        return path

    def test_26a_cli_verify_requires_signatures_when_keys_are_given(self):
        path = self._signed_run({"k1": b"s"})
        code, out, _ = cli("--journal", f"sqlite:///{path}", "verify", "r2", env={"PYLINXI_KEYS": "k1=s"})
        self.assertEqual(code, 1, out)
        code, _, _ = cli("--journal", f"sqlite:///{path}", "verify", "r2", "--allow-unsigned",
                         env={"PYLINXI_KEYS": "k1=s"})
        self.assertEqual(code, 0)

    def test_26bc_keys_from_files_and_binary_encodings(self):
        key = bytes(range(256))[:40] + b",="
        path = self._signed_run({"k1": key})
        kf = Path(self.d, "keys.json")
        kf.write_text(json.dumps({"k1": "hex:" + key.hex()}))
        self.assertEqual(cli("--journal", f"sqlite:///{path}", "--key-file", str(kf), "verify", "r")[0], 0)
        import base64
        env = {"PYLINXI_KEYS": "k1=base64:" + base64.b64encode(key).decode()}
        self.assertEqual(cli("--journal", f"sqlite:///{path}", "verify", "r", env=env)[0], 0)
        code, _, err = cli("--journal", f"sqlite:///{path}", "--key", "k1=hex:" + key.hex(), "verify", "r")
        self.assertEqual(code, 0)
        self.assertIn("warning", err)

    def test_26d_error_text_is_bounded_and_redactable(self):
        j = MemoryJournal(error_redactor=lambda t: t.replace("4242424242424242", "****"))

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: (_ for _ in ()).throw(ValueError("card 4242424242424242 " + "x" * 100000)),
                           name="pay")

        run(f.run(run_id="r"))
        err = next(e for e in j.load("r") if e.key == "pay#0").meta["error"]
        self.assertLessEqual(len(err), 500)
        self.assertNotIn("4242424242424242", err)


# ======================================================================== ops
class TestOps(TmpDir):
    def test_27_a_blocked_journal_does_not_stall_the_event_loop(self):
        path = f"{self.d}/j.db"
        j = SQLiteJournal(path)

        @flow(journal=j)
        async def f(ctx):
            for i in range(3):
                await ctx.step(lambda: 1, name="s")

        async def main():
            gaps, stop = [], asyncio.Event()

            async def hb():
                last = time.perf_counter()
                while not stop.is_set():
                    await asyncio.sleep(0.005)
                    now = time.perf_counter()
                    gaps.append(now - last)
                    last = now

            def hold():
                import sqlite3
                c = sqlite3.connect(path, isolation_level=None)
                c.execute("BEGIN IMMEDIATE")
                time.sleep(0.5)
                c.execute("COMMIT")
                c.close()
            h = asyncio.ensure_future(hb())
            threading.Thread(target=hold, daemon=True).start()
            await asyncio.sleep(0.05)
            await f.run(run_id="r")
            stop.set()
            await h
            return max(gaps)
        self.assertLess(run(main()), 0.15)
        j.close()

    def test_29_worker_resumes_concurrently_and_counts_failures_since_progress(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def slow(ctx, d):
            await ctx.sleep(0.01, inline_max=0)
            await ctx.step(lambda: time.sleep(d), name="work")

        for i, d in enumerate([0.3, 0.3, 0.3]):
            run(slow.run(d, run_id=f"s{i}"))
        time.sleep(0.02)
        t = time.perf_counter()
        rep = run(Worker(j, [slow], concurrency=3).tick())
        self.assertEqual(sorted(rep.completed), ["s0", "s1", "s2"])
        self.assertLess(time.perf_counter() - t, 0.8)          # not 0.9 s in sequence

        stages = {"n": 0}

        @flow(journal=j)
        async def staged(ctx):
            for i in range(6):
                def advance(i=i):
                    if stages["n"] == i:                       # each resume gets one stage further
                        stages["n"] += 1
                        raise ConnectionError("blip")
                    return i
                await ctx.step(advance, name="stage")
            return "all"

        run(staged.run(run_id="g"))
        w = Worker(j, [staged], retry_failed=True, max_failures=2, backoff=0, resume_stale_after=None)
        for _ in range(8):
            run(w.tick())
        self.assertEqual(run(staged.resume("g")).output, "all")

    def test_29d_run_forever_reports_tick_errors(self):
        class Broken(MemoryJournal):
            def heads(self, kinds=None):
                raise RuntimeError("db down")
        errors = []

        async def go():
            stop = asyncio.Event()
            w = Worker(Broken(), [], on_error=errors.append)
            t = asyncio.ensure_future(w.run_forever(interval=0.01, stop=stop))
            await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(t, 2)
        with self.assertLogs("pylinxi", "ERROR"):
            run(go())
        self.assertTrue(errors)

    def test_30_worker_recovers_runs_left_open_by_a_crash(self):
        j = MemoryJournal()

        class Killed(BaseException):
            pass
        crash = {"v": True}

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: 1, name="a")
            if crash["v"]:
                crash["v"] = False
                raise Killed()
            return "recovered"

        with contextlib.suppress(Killed):
            run(f.run(run_id="r"))
        rep = run(Worker(j, [f], resume_stale_after=0).tick())
        self.assertEqual(rep.completed, ["r"])

    def test_31_timed_out_sync_attempt_is_never_overlapped(self):
        charges = []

        @step(policy=Policy(timeout=0.05, retry=Retry(attempts=3, backoff=0)))
        def charge():
            charges.append(idempotency_key())
            time.sleep(0.08)                                   # a bit longer than the timeout
            return "charged"

        @flow
        async def f(ctx):
            return await charge()

        r = run(f.run())
        self.assertEqual(r.output, "charged")                  # the late result was used
        self.assertEqual(len(charges), 1)

        @step(policy=Policy(timeout=0.02, retry=Retry(attempts=3, backoff=0)))
        def stuck():
            charges.append("stuck")
            time.sleep(0.3)

        @flow
        async def g(ctx):
            await stuck()

        r = run(g.run())
        self.assertIsInstance(r.error.error, AttemptStillRunning)
        self.assertEqual(charges.count("stuck"), 1)

    def test_31b_sync_steps_use_their_own_pool(self):
        from pylinxi.policy import step_executor
        names = []

        @step
        def who():
            names.append(threading.current_thread().name)

        @flow
        async def f(ctx):
            await who()

        run(f.run())
        self.assertTrue(names[0].startswith("pylinxi-step"))
        self.assertGreaterEqual(step_executor()._max_workers, 32)

    def test_32_no_growth_in_long_lived_workers(self):
        from pylinxi.journal import _held
        j = FileJournal(self.d)

        @flow(journal=j)
        async def f(ctx):
            async def b():
                return 1
            await asyncio.gather(*(b() for _ in range(50)))
            self.assertLess(len(ctx._tasks), 50)               # finished branches aren't retained
            return 1

        before = len(_held)
        for i in range(30):
            run(f.run(run_id=f"r{i}"))
        self.assertEqual(len(_held), before)
        for i in range(30):
            j.delete(f"r{i}")
        self.assertEqual(list((Path(self.d) / ".locks").glob("*.lock")), [])

    def test_32d_rejected_signals_leave_no_open_span(self):
        try:
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import SimpleSpanProcessor
            from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

            from pylinxi.otel import OTelExporter
        except ImportError:
            self.skipTest("opentelemetry-sdk not installed")
        mem, prov = InMemorySpanExporter(), TracerProvider()
        prov.add_span_processor(SimpleSpanProcessor(mem))
        exp = OTelExporter(tracer=prov.get_tracer("t"))

        @flow(on_event=exp)
        async def f(ctx):
            await ctx.wait_for("x")

        run(f.run(run_id="o1"))
        with self.assertRaises(ValueError):
            run(f.resume("o1", signals={"api_key": "sk"}))
        self.assertEqual(exp._runs, {})


# ======================================================================== minor
class TestMinor(TmpDir):
    def test_m1_map_rejects_non_positive_concurrency(self):
        @flow
        async def f(ctx):
            return await ctx.map(lambda x: x, [1, 2], concurrency=0)

        r = run(asyncio.wait_for(f.run(), 2))
        self.assertIsInstance(r.error, ValueError)

    def test_m2_different_inputs_for_an_existing_run_are_refused(self):
        @flow
        async def f(ctx, who):
            await ctx.wait_for("go")
            return who

        run(f.run("ana", run_id="r"))
        with self.assertRaises(RunExists):
            run(f.run("bo", run_id="r"))
        self.assertEqual(run(f.run("ana", run_id="r", signals={"go": 1})).output, "ana")

    def test_m3_durable_signals(self):
        j = MemoryJournal()
        entered = threading.Event()

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: (entered.set(), time.sleep(0.2)), name="busy")
            return await ctx.wait_for("go")

        async def main():
            t = asyncio.ensure_future(f.run(run_id="s"))
            while not entered.is_set():
                await asyncio.sleep(0.005)
            self.assertIsNone(await f.signal("s", "go", "while-running"))   # busy: journaled, not lost
            return await t
        self.assertEqual(run(main()).output, "while-running")
        with self.assertRaises(ValueError):
            j.send_signal("s", "go", 2)                         # completed
        with self.assertRaises(UnknownRun):
            j.send_signal("nope", "go", 1)
        r = run(f.resume("s", signals={"late": 1}))
        self.assertEqual(r.dropped_signals, ["late"])

    def test_m3b_worker_resumes_runs_that_received_a_signal(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            return await ctx.wait_for("go")

        run(f.run(run_id="s"))
        j.send_signal("s", "go", "yes")
        self.assertEqual(run(Worker(j, [f]).tick()).completed, ["s"])
        self.assertEqual(run(f.resume("s")).output, "yes")

    def test_m4_failing_repair_hook_falls_back(self):
        def repair(*a):
            raise KeyError("repair blew up")

        @flow
        async def f(ctx):
            return await ctx.step(lambda: 1 / 0, name="x",
                                  policy=Policy(retry=Retry(attempts=3, backoff=0), repair=repair,
                                                fallback=lambda: "fb"))

        self.assertEqual(run(f.run()).output, "fb")

    def test_m5_attempts_count_what_actually_ran(self):
        br = CircuitBreaker(threshold=1, reset_after=60)
        br.failure()
        with self.assertRaises(StepFailed) as cm:
            run(pylinxi.policy.run_with_policy("k", lambda: 1, (), {}, Policy(breaker=br), lambda *a, **k: None))
        self.assertEqual(cm.exception.attempts, 0)
        br2 = CircuitBreaker(threshold=2, reset_after=60)

        def down():
            raise ConnectionError("503")
        with self.assertRaises(StepFailed) as cm:
            run(pylinxi.policy.run_with_policy("k", down, (), {},
                                               Policy(retry=Retry(attempts=4, backoff=0), breaker=br2),
                                               lambda *a, **k: None))
        self.assertEqual(cm.exception.attempts, 2)
        self.assertIsInstance(cm.exception.error, ConnectionError)   # the real error, not CircuitOpen

    def test_m6_file_journal_accepts_any_run_id_and_keeps_case_apart(self):
        j = FileJournal(self.d)

        @flow(journal=j)
        async def f(ctx, who):
            return who

        ids = ["user@example.com", "a/b", "a:b", "t 1", "ünïcode", "x" * 300, "CON", "Run1", "run1"]
        for rid in ids:
            self.assertEqual(run(f.run(rid, run_id=rid)).output, rid)
        self.assertEqual(sorted(j.runs()), sorted(ids))
        names = {p.name.lower() for p in Path(self.d).glob("*.jsonl")}
        self.assertEqual(len(names), len(ids))                 # no two ids share a file, even case-folded

    def test_m7_cli_refuses_mistyped_journals_and_handles_corrupt_runs(self):
        code, _, err = cli("--journal", f"sqlite:///{self.d}/rusn.db", "runs")
        self.assertEqual(code, 2)
        self.assertFalse(os.path.exists(f"{self.d}/rusn.db"))
        self.assertEqual(cli("--journal", f"file://{self.d}/nope", "runs")[0], 2)
        self.assertEqual(cli("--journal", "sqlite://runs.db", "runs")[0], 2)
        j = SQLiteJournal(f"{self.d}/j.db")

        @flow(journal=j)
        async def f(ctx):
            return 1
        run(f.run(run_id="r1"))
        j._db.execute("UPDATE pylinxi_journal SET payload='{' WHERE run_id='r1' AND seq=0")
        j.close()
        code, out, err = cli("--journal", f"sqlite:///{self.d}/j.db", "state", "r1")
        self.assertEqual(code, 1)
        self.assertNotIn("Traceback", err)
        self.assertIn("corrupt", err)

    def test_m8_run_result_says_where_its_state_came_from(self):
        @flow
        async def f(ctx):
            ctx.state["n"] = 1
            return 1

        first = run(f.run(run_id="r"))
        again = run(f.resume("r"))
        self.assertEqual((first.from_journal, again.from_journal), (False, True))
        self.assertEqual(first.state, again.state)

    def test_m9_wait_for_on_a_plain_coroutine_gives_the_same_keys_on_every_python(self):
        @step
        def fetch(i):
            return i

        @flow
        async def f(ctx):
            async def helper():
                return await fetch(1) + await fetch(2)
            return await asyncio.wait_for(helper(), 5)

        r = run(f.run(run_id="r"))
        self.assertEqual(r.output, 3)
        self.assertEqual([k for kd, k in kinds(f.journal, "r") if kd == "step"], ["fetch#0", "fetch#1"])




# ======================================================================== re-review of the 1.1 fixes
def _journals(d):
    """Every backend, so replay bugs that only show with thread-written journals are caught."""
    return [("memory", MemoryJournal()), ("file", FileJournal(f"{d}/files")), ("sqlite", SQLiteJournal(f"{d}/j.db"))]


class TestReReview(TmpDir):
    def test_rr1_journal_writes_never_shift_branch_keys(self):
        for name, j in _journals(self.d):
            calls = []

            @step
            async def a():
                calls.append("a")
                return 1

            @step
            async def b(x):
                calls.append(x)
                return x

            async def br(x):
                return await b(x)

            @flow(journal=j)
            async def f(ctx):
                await a()
                r = await asyncio.gather(br(1), br(2))
                await ctx.wait_for("go")
                return r

            run(f.run(run_id="r"))
            r = run(f.resume("r", signals={"go": 1}))
            self.assertEqual((r.status, r.output), ("completed", [1, 2]), name)
            self.assertEqual(calls, ["a", 1, 2], name)
            j.close()

    def test_rr2_retry_after_cleanup_steps_takes_the_success_path(self):
        for name, j in _journals(self.d):
            n = {"charge": 0}

            @step
            async def charge():
                n["charge"] += 1
                if n["charge"] == 1:
                    raise RuntimeError("gateway 503")
                return "ch"

            @step
            async def commit():
                return "c"

            @step
            async def rollback():
                return "r"

            @contextlib.asynccontextmanager
            async def tx():
                try:
                    yield
                except BaseException:
                    await rollback()
                    raise
                else:
                    await commit()

            @flow(journal=j)
            async def order(ctx):
                async with tx():
                    await charge()
                return "shipped"

            self.assertEqual(run(order.run(run_id="o")).status, "failed")
            for _ in range(2):
                self.assertEqual(run(order.resume("o")).output, "shipped", name)
            j.close()

    def test_rr3_durable_waits_inside_a_taskgroup(self):
        if sys.version_info < (3, 11):
            self.skipTest("TaskGroup needs Python 3.11")
        for name, j in _journals(self.d):
            @flow(journal=j)
            async def f(ctx):
                async with asyncio.TaskGroup() as tg:
                    t1 = tg.create_task(ctx.wait_for("go"))
                    tg.create_task(ctx.sleep(10))
                return t1.result()

            r = run(f.run(run_id="t"))
            self.assertEqual((r.status, r.waiting_for), ("suspended", "go"), name)
            self.assertAlmostEqual(r.wake_at - time.time(), 10, delta=2)
            j.close()

    def test_rr3b_worker_survives_exception_groups(self):
        class Boom(MemoryJournal):
            def heads(self, kinds=None):
                group = getattr(__import__("builtins"), "BaseExceptionGroup", None)
                raise group("x", [RuntimeError("db")]) if group else RuntimeError("db")

        async def go():
            stop = asyncio.Event()
            t = asyncio.ensure_future(Worker(Boom(), []).run_forever(interval=0.01, stop=stop))
            await asyncio.sleep(0.05)
            self.assertFalse(t.done())
            stop.set()
            await asyncio.wait_for(t, 2)
        with self.assertLogs("pylinxi", "ERROR"):
            run(go())

    def test_rr4_signal_picked_up_during_the_drain_is_not_lost(self):
        j = SQLiteJournal(f"{self.d}/j.db")

        @step
        async def slow():
            await asyncio.sleep(0.3)
            return 1

        @flow(journal=j)
        async def f(ctx):
            async def wb():
                await slow()
                return await ctx.wait_for("b")
            return await asyncio.gather(ctx.wait_for("a"), wb())

        async def main():
            async def deliver():
                await asyncio.sleep(0.1)
                await asyncio.to_thread(j.send_signal, "r", "a", "A")
            r, _ = await asyncio.gather(f.run(run_id="r"), deliver())
            return r
        r = run(main())
        self.assertEqual(r.waiting_for_all, ["b"])                # "a" was consumed, not left waiting
        self.assertEqual(run(f.resume("r", signals={"b": "B"})).output, ["A", "B"])
        j.close()

    def test_rr6_failure_inside_a_handler_doesnt_unhandle_the_first(self):
        n = {"primary": 0, "ship": 0}

        @step
        async def primary():
            n["primary"] += 1
            if n["primary"] == 1:
                raise RuntimeError("primary down")
            return "P"

        @step
        async def ship(x):
            n["ship"] += 1
            if n["ship"] == 1:
                raise RuntimeError("ship 503")
            return f"shipped {x}"

        @flow
        async def order(ctx):
            try:
                src = await primary()
            except StepFailed:
                return await ship("B")
            return await ship(src)

        run(order.run(run_id="o"))
        self.assertEqual(run(order.resume("o")).output, "shipped B")
        self.assertEqual(n["primary"], 1)

    def test_rr7_worker_recovers_a_crash_right_after_a_step_failure(self):
        j = MemoryJournal()
        j.append(Entry("r", "start", "start", {"args": [], "kwargs": {}, "state": {}, "flow": "f", "root": "r"}))
        j.append(Entry("r", "charge#0", "failed", None, meta={"error": "x", "attempts": 1}))

        @flow(journal=j)
        async def f(ctx):
            return await ctx.step(lambda: "ok", name="charge")

        self.assertEqual(run(Worker(j, [f], resume_stale_after=0).tick()).completed, ["r"])

    def test_rr8_cancelled_lock_acquisition_never_leaks_the_lock(self):
        j = SQLiteJournal(f"{self.d}/j.db")
        slow_lock = j.lock

        @contextlib.contextmanager
        def lock(rid):
            time.sleep(0.1)
            with slow_lock(rid):
                yield
        j.lock = lock

        @flow(journal=j)
        async def f(ctx):
            return "ok"

        async def main():
            t = asyncio.ensure_future(f.run(run_id="r"))
            await asyncio.sleep(0.02)
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
            await asyncio.sleep(0.2)
            j.lock = slow_lock
            return await f.run(run_id="r")
        self.assertEqual(run(main()).output, "ok")
        j.close()

    def test_rr9_concurrent_writes_keep_their_order(self):
        j = SQLiteJournal(f"{self.d}/j.db")
        orig = j._append_locked

        def slow_first(e):
            if e.key == "t0/a#0":
                time.sleep(0.1)
            orig(e)
        j._append_locked = slow_first

        @flow(journal=j)
        async def f(ctx):
            async def a():
                ctx.state["x"] = "first"
                await ctx.step(lambda: 1, name="a")

            async def b():
                await asyncio.sleep(0.01)
                ctx.state["x"] = "second"
                await ctx.step(lambda: 2, name="b")
            await asyncio.gather(a(), b())

        r = run(f.run(run_id="r"))
        self.assertEqual(r.state["x"], "second")
        self.assertEqual(j.state_at("r")["x"], "second")
        j.close()

    def test_rr10_retry_failed_gives_up_even_with_a_sensitive_step(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            await ctx.step(lambda: "tok", name="creds", sensitive=True)
            await ctx.step(lambda: 1 / 0, name="charge")

        run(f.run(run_id="r"))
        w = Worker(j, [f], retry_failed=True, max_failures=3, backoff=0, resume_stale_after=None)
        gave_up = []
        for _ in range(6):
            gave_up += run(w.tick()).gave_up
        self.assertEqual(gave_up, ["r"])

    def test_rr11_case_folded_names_never_touch_another_run(self):
        j = FileJournal(self.d)

        @flow(journal=j)
        async def f(ctx):
            return await ctx.wait_for("go")

        run(f.run(run_id="keep"))
        upper = Path(self.d) / "KEEP.jsonl"
        with contextlib.suppress(OSError):           # emulate a case-insensitive filesystem
            os.symlink(Path(self.d) / "keep.jsonl", upper)
        j2 = FileJournal(self.d)
        with self.assertRaises(UnknownRun):
            j2.send_signal("KEEP", "go", 1)
        self.assertEqual(j2.delete("KEEP"), [])
        self.assertTrue(j2.verify("keep"))

    def test_rr_risk_cancelled_sync_step_is_not_repeated_on_resume(self):
        charges = []

        @step
        def charge():
            time.sleep(0.2)
            charges.append(1)
            return "charged"

        @flow(drain_timeout=0.05)
        async def f(ctx):
            async def b():
                return await charge()
            res = await asyncio.gather(ctx.wait_for("go"), b())
            return res[1]

        r = run(f.run(run_id="r"))                    # the drain gives up on b while charge runs
        self.assertEqual(r.status, "suspended")
        time.sleep(0.3)
        self.assertEqual(run(f.resume("r", signals={"go": 1})).output, "charged")
        self.assertEqual(len(charges), 1)

    def test_rr_risk_step_names_with_slashes_keep_divergence_detection(self):
        j = MemoryJournal()

        async def v1(ctx):
            await ctx.step(lambda: 1, name="crm/sync")
            await ctx.wait_for("go")

        async def v2(ctx):
            await ctx.step(lambda: 1, name="crm/sync-v2")
            await ctx.wait_for("go")

        run(flow(v1, journal=j, name="f").run(run_id="r"))
        r = run(flow(v2, journal=j, name="f").resume("r", signals={"go": 1}))
        self.assertIsInstance(r.error, ReplayDivergence)

    def test_rr_load_abandoned_results_never_cross_journals_or_incarnations(self):
        # found under CPU load: a late result abandoned by one journal's run "r" was handed
        # to another journal's run "r" with the same step key
        @step(policy=Policy(timeout=0.02))
        def charge():
            time.sleep(0.1)
            return "late"

        j1 = MemoryJournal()

        @flow(journal=j1)
        async def a(ctx):
            return await charge()

        run(a.run(run_id="r"))                        # times out, its thread finishes later
        time.sleep(0.15)
        j2 = MemoryJournal()

        @flow(journal=j2)
        async def b(ctx):
            return await ctx.step(lambda: "fresh", name="charge")

        self.assertEqual(run(b.run(run_id="r")).output, "fresh")
        j1.delete("r")                                # same journal, new incarnation of "r"
        self.assertEqual(run(flow(b.fn, journal=j1, name="a").run(run_id="r")).output, "fresh")


class TestReReview2(TmpDir):
    """Second re-review: bugs introduced or left by the first round of fixes (N1-N7)."""

    def test_n1_siblings_that_finished_after_a_failure_are_not_rerun(self):
        from collections import Counter
        for use_map in (True, False):
            n = Counter()

            @step
            async def charge(item):
                n[item] += 1
                if item == "A" and n[item] == 1:
                    await asyncio.sleep(0.01)
                    raise RuntimeError("A declined")
                if item == "B":
                    await asyncio.sleep(0.05)        # finishes after A failed
                return f"charged {item}"

            @step
            async def release():
                n["release"] += 1
                return True

            @flow(journal=SQLiteJournal(f"{self.d}/n1{use_map}.db"), name="f")
            async def f(ctx):
                try:
                    if use_map:
                        return await ctx.map(charge, ["A", "B"])
                    return await asyncio.gather(charge("A"), charge("B"))
                finally:
                    await release()

            self.assertEqual(run(f.run(run_id="r")).status, "failed")
            r = run(f.resume("r"))
            self.assertEqual(r.output, ["charged A", "charged B"])
            self.assertEqual(dict(n), {"A": 2, "B": 1, "release": 1}, use_map)

    def test_n2_progress_made_while_a_branch_failure_was_pending_is_not_rerun(self):
        from collections import Counter
        n = Counter()
        boom = {"on": True}

        @step
        async def charge():
            n["charge"] += 1
            await asyncio.sleep(0.01)
            if boom["on"]:
                raise RuntimeError("declined")
            return "ok"

        @step
        def ship():
            n["ship"] += 1
            return "shipped"

        @flow(journal=SQLiteJournal(f"{self.d}/n2.db"))
        async def f(ctx):
            t = asyncio.ensure_future(charge())
            shipped = await ship()
            await asyncio.sleep(0.05)
            return await t, shipped

        self.assertEqual(run(f.run(run_id="r")).status, "failed")
        boom["on"] = False
        self.assertEqual(run(f.resume("r")).output, ("ok", "shipped"))
        self.assertEqual(dict(n), {"charge": 2, "ship": 1})

    def test_n7_cleanup_reached_again_after_a_successful_retry_replays(self):
        from collections import Counter
        n = Counter()

        @step
        def charge():
            n["charge"] += 1
            if n["charge"] == 1:
                raise RuntimeError("down")
            return "ch_1"

        @step
        def email():
            n["email"] += 1
            return True

        for name, j in _journals(self.d):
            n.clear()

            @flow(journal=j, name="f")
            async def f(ctx):
                try:
                    return await charge()
                finally:
                    await email()

            self.assertEqual(run(f.run(run_id="r")).status, "failed", name)
            r = run(f.resume("r"))
            self.assertEqual((r.status, r.output), ("completed", "ch_1"), name)
            self.assertEqual(dict(n), {"charge": 2, "email": 1}, name)
            j.close()

    def test_n1_retry_after_cleanup_still_takes_another_path(self):
        """The case the superseding was introduced for (first re-review #2) still works."""
        calls = []
        boom = {"on": True}

        @step
        def charge():
            calls.append("charge")
            if boom["on"]:
                raise RuntimeError("down")
            return 1

        @step
        def notify(what):
            calls.append(what)
            return what

        j = SQLiteJournal(f"{self.d}/j.db")

        @flow(journal=j)
        async def f(ctx):
            try:
                await charge()
            except StepFailed:
                await notify("failed")
                raise
            return await notify("shipped")

        self.assertEqual(run(f.run(run_id="r")).status, "failed")
        boom["on"] = False
        r = run(f.resume("r"))
        self.assertEqual((r.status, r.output), ("completed", "shipped"))
        self.assertEqual(calls, ["charge", "failed", "charge", "shipped"])

    def test_n3_taskgroup_sibling_inside_a_step_still_records_its_deadline(self):
        if sys.version_info < (3, 11):
            self.skipTest("TaskGroup needs Python 3.11")
        calls = []

        @step
        async def prep():
            calls.append(1)
            await asyncio.sleep(0.1)
            return 1

        for name, j in _journals(self.d):
            calls.clear()

            @flow(journal=j)
            async def f(ctx):
                async def reminder():
                    await prep()
                    await ctx.sleep(600)

                async with asyncio.TaskGroup() as tg:
                    t = tg.create_task(ctx.wait_for("go"))
                    tg.create_task(reminder())
                return t.result()

            r = run(f.run(run_id="t"))
            self.assertEqual(r.status, "suspended", name)
            self.assertAlmostEqual(r.wake_at - time.time(), 600, delta=5, msg=name)
            self.assertEqual(calls, [1], name)
            j.close()

    def test_n4_resume_while_an_earlier_attempt_runs_waits_instead_of_failing(self):
        from pylinxi import policy as pol
        old = pol.PRIOR_ATTEMPT_WAIT
        pol.PRIOR_ATTEMPT_WAIT = 0.2
        try:
            done = []

            @step
            def report():
                time.sleep(1.0)
                done.append(1)
                return "report.csv"

            j = SQLiteJournal(f"{self.d}/j.db")

            @flow(journal=j, drain_timeout=0.1)
            async def f(ctx):
                async def gen():
                    try:
                        return await report()
                    except StepFailed as e:
                        return f"FAILED: {e.error!r}"
                return await asyncio.gather(ctx.wait_for("go"), gen())

            async def go():
                r = await f.run(run_id="r")
                self.assertEqual(r.status, "suspended")
                r = await f.resume("r", signals={"go": 1})
                while r.status == "suspended":
                    self.assertIsNotNone(r.wake_at)
                    await asyncio.sleep(max(0.0, r.wake_at - time.time()))
                    r = await f.resume("r")
                return r
            r = run(go())
            self.assertEqual((r.status, r.output), ("completed", [1, "report.csv"]))
            self.assertEqual(done, [1])
            self.assertEqual([e.key for e in j.load("r") if e.kind == "marker"], [])
        finally:
            pol.PRIOR_ATTEMPT_WAIT = old

    def test_n5_a_slow_run_does_not_stall_other_runs_writes(self):
        slow_runs = {"slow"}

        class Slow(SQLiteJournal):
            def append(self, e):
                if e.run_id in slow_runs and e.kind == "step":
                    time.sleep(0.03)
                return super().append(e)

        j = Slow(f"{self.d}/j.db")

        @flow(journal=j, name="f")
        async def f(ctx, n):
            return await asyncio.gather(*(ctx.step(lambda: 1, name="s") for _ in range(n)))

        async def go():
            slow = asyncio.ensure_future(f.run(60, run_id="slow"))
            await asyncio.sleep(0.1)
            t = time.perf_counter()
            r = await f.run(1, run_id="fast")
            took = time.perf_counter() - t
            await slow
            return r, took
        r, took = run(go())
        self.assertEqual(r.status, "completed")
        self.assertLess(took, 0.5)

    def test_n5_a_failed_write_stops_later_writes_of_that_run(self):
        from pylinxi.flow import Context

        class Flaky(MemoryJournal):
            blocking = True

            def append(self, e):
                if e.key == "b":
                    time.sleep(0.05)
                    raise OSError("disk full")
                return super().append(e)

        j = Flaky()
        j.append(Entry("r", "start", "start", {}))
        ctx = Context("r", j, {}, j.load("r"), lambda *a, **k: None)
        futs = [ctx._submit(Entry("r", k, "step", 1)) for k in ("a", "b", "c")]
        concurrent_wait = __import__("concurrent.futures").futures.wait
        concurrent_wait(futs, timeout=5)
        self.assertIsNone(futs[0].exception())
        self.assertIsInstance(futs[1].exception(), OSError)
        self.assertIsInstance(futs[2].exception(), OSError)
        self.assertEqual([e.key for e in j.load("r")], ["start", "a"])

    def test_n6_file_journal_instances_agree_on_files_and_locks(self):
        a, b = FileJournal(self.d), FileJournal(self.d)

        @flow(journal=a, name="f")
        async def fa(ctx, x):
            return x

        fb = flow(fa.fn, journal=b, name="f")
        run(fa.run("old", run_id="Order-1"))
        self.assertEqual(a._lock_file("Order-1"), b._lock_file("Order-1"))
        self.assertEqual(a._lock_file("Order-1"), FileJournal(self.d)._lock_file("Order-1"))
        b.delete("Order-1")
        self.assertEqual(run(fb.run("new", run_id="Order-1")).output, "new")
        self.assertEqual(run(fa.resume("Order-1")).output, "new")     # A sees B's re-created run
        with a.lock("Order-1"):
            with self.assertRaises(RunLocked):
                with FileJournal(self.d).lock("Order-1"):
                    pass



class TestReReview3(TmpDir):
    """Third re-review: problems in the second round of fixes."""

    def test_rr3_1_cancelling_a_racing_step_does_not_shorten_later_sleeps(self):
        calls = {"fast": 0, "slow": 0}

        @step
        async def provider(name, delay):
            calls[name] += 1
            await asyncio.sleep(delay)
            return name

        @flow(journal=MemoryJournal())
        async def f(ctx):
            a = asyncio.ensure_future(provider("fast", 0.02))
            b = asyncio.ensure_future(provider("slow", 5))
            done, pending = await asyncio.wait([a, b], return_when=asyncio.FIRST_COMPLETED)
            for p in pending:
                p.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await ctx.sleep(3600)
            return done.pop().result()

        r = run(f.run(run_id="r"))
        self.assertEqual(r.status, "suspended")
        self.assertGreater(r.wake_at - time.time(), 3500)

    def test_rr3_2_flow_body_step_inside_a_taskgroup_is_not_cut_short(self):
        if sys.version_info < (3, 11):
            self.skipTest("TaskGroup needs Python 3.11")
        calls = []

        @step
        async def long_step():
            calls.append(1)
            await asyncio.sleep(0.2)
            return "done"

        j = MemoryJournal()

        @flow(journal=j)
        async def f(ctx):
            async with asyncio.TaskGroup() as tg:
                tg.create_task(ctx.sleep(3600))
                r = await long_step()
            return r

        r = run(f.run(run_id="r"))
        self.assertEqual(r.status, "suspended")
        self.assertGreater(r.wake_at - time.time(), 3500)
        self.assertIn("long_step#0", [e.key for e in j.load("r") if e.kind == "step"])
        self.assertEqual(calls, [1])

    def test_rr3_3_cancelled_wait_on_an_earlier_attempt_keeps_remembering_it(self):
        started, running, peak = [], [], []

        @step
        def report():
            started.append(1)
            running.append(1)
            peak.append(len(running))
            time.sleep(1.0)
            running.pop()
            return "report.csv"

        @flow(journal=SQLiteJournal(f"{self.d}/j.db"), drain_timeout=0.1)
        async def f(ctx):
            return await asyncio.gather(ctx.wait_for("go"), report())

        async def go():
            await f.run(run_id="r")
            await f.resume("r")                     # waits on the earlier attempt, gets cancelled
            await f.resume("r")
            await asyncio.sleep(1.3)
            r = await f.resume("r", signals={"go": 1})
            while r.status == "suspended":
                await asyncio.sleep(max(0.0, (r.wake_at or time.time() + 0.1) - time.time()))
                r = await f.resume("r")
            return r
        r = run(go())
        self.assertEqual(r.output, [1, "report.csv"])
        self.assertEqual((len(started), max(peak)), (1, 1))

    def test_rr3_4_superseded_cleanup_with_other_arguments_runs(self):
        sent = []
        boom = {"on": True}

        @step
        def charge():
            if boom["on"]:
                raise RuntimeError("down")
            return "ch"

        @step
        def email(status):
            sent.append(status)
            return f"emailed {status}"

        for name, j in _journals(self.d):
            sent.clear()
            boom["on"] = True

            @flow(journal=j, name="f")
            async def f(ctx):
                status = "FAILED"
                try:
                    await charge()
                    status = "SHIPPED"
                finally:
                    ctx.state["mail"] = await email(status)
                return status

            self.assertEqual(run(f.run(run_id="r")).status, "failed", name)
            boom["on"] = False
            r = run(f.resume("r"))
            self.assertEqual((r.output, r.state["mail"]), ("SHIPPED", "emailed SHIPPED"), name)
            self.assertEqual(sent, ["FAILED", "SHIPPED"], name)
            j.close()

    def test_rr3_4b_per_item_compensation_in_a_loop(self):
        effects = []
        fail = {"A": 1, "B": 1}

        @step
        def reserve(item):
            effects.append(f"reserve {item}")
            if fail.get(item):
                fail[item] -= 1
                raise RuntimeError(item)
            return item

        @step
        def release(item):
            effects.append(f"release {item}")
            return f"released {item}"

        @flow(journal=SQLiteJournal(f"{self.d}/j.db"))
        async def f(ctx):
            for item in ("A", "B"):
                try:
                    await reserve(item)
                except StepFailed:
                    ctx.state["last"] = await release(item)
                    raise
            return "ok"

        run(f.run(run_id="r"))
        r = run(f.resume("r"))
        self.assertEqual(r.state["last"], "released B")
        self.assertIn("release B", effects)

    def test_rr3_6_cancelling_run_while_steps_finish_stops_every_branch(self):
        calls = []

        @step
        async def audit(tag):
            calls.append(tag)
            await asyncio.sleep(0.5)
            return tag

        j = SQLiteJournal(f"{self.d}/j.db")

        @flow(journal=j)
        async def f(ctx):
            asyncio.ensure_future(audit("bg"))
            await asyncio.sleep(0.02)
            return "ok"

        async def go():
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(f.run(run_id="r"), 0.15)
            live = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            r = await f.resume("r")
            await asyncio.sleep(0.6)
            return live, r
        live, r = run(go())
        self.assertEqual(live, [])
        self.assertEqual(r.output, "ok")
        self.assertEqual([e.key for e in j.load("r")].count("audit#0"), 1)

    def test_rr3_7_suspension_latency_is_one_drain_timeout(self):
        @step
        async def tick(i):
            await asyncio.sleep(0.02)
            return i

        @flow(journal=MemoryJournal(), drain_timeout=0.5)
        async def f(ctx):
            async def worker():
                for i in range(10_000):
                    await tick(i)
            return await asyncio.gather(ctx.wait_for("go"), worker())

        t = time.monotonic()
        self.assertEqual(run(f.run(run_id="r")).status, "suspended")
        self.assertLess(time.monotonic() - t, 0.9)



class TestReview4(TmpDir):
    """Fourth independent review (fixed in 1.1.1)."""

    def test_r4_1_state_changed_in_place_while_a_write_is_pending(self):
        for name, j in _journals(self.d):
            charged = []

            @step
            def charge(i):
                charged.append(i)
                return i

            @flow(journal=j, name="f")
            async def f(ctx):
                ctx.state["seen"] = {k: k for k in range(2000)}
                stop = False

                async def tracker():
                    n = 2000
                    while not stop:
                        for _ in range(20):
                            ctx.state["seen"][n] = n          # in-place, changes the dict's size
                            ctx.state["seen"].pop(n - 50, None)
                            n += 1
                        await asyncio.sleep(0)
                t = asyncio.ensure_future(tracker())
                try:
                    for i in range(15):
                        await charge(i)
                finally:
                    stop = True
                    await t
                return "ok"

            r = run(f.run(run_id="s"))
            self.assertEqual((r.status, r.output), ("completed", "ok"), (name, r.error))
            self.assertEqual(charged, list(range(15)), name)
            j.close()

    def test_r4_1b_patched_marker_then_in_place_state_change(self):
        j = SQLiteJournal(f"{self.d}/j.db")

        @step
        def charge(i):
            return i

        @flow(journal=j)
        async def f(ctx, n):
            ctx.state["index"] = {}
            await charge(0)
            for k in range(n):
                ctx.state["index"][k] = k            # in the delta of the marker below...
            if ctx.patched("v2-reindex"):            # ...which is written by a writer thread
                for k in range(n):
                    ctx.state["index"][-k - 1] = k   # while flow code keeps changing the dict
            await charge(1)
            return "ok"

        r = run(f.run(100_000, run_id="r"))
        self.assertEqual(r.status, "completed", r.error)
        self.assertTrue(j.verify("r"))

    def test_r4_2_a_swallowed_strict_divergence_still_stops_the_run(self):
        j = SQLiteJournal(f"{self.d}/j.db")
        effects = []

        @step
        def a():
            return "a"

        @step
        def b():
            return "b"

        @step
        def charge():
            effects.append("charge")
            return 1

        def make(v):
            @flow(journal=j, name="div")
            async def f(ctx):
                async def branch():
                    return await (a() if v == 1 else b())
                await asyncio.gather(branch(), return_exceptions=True)
                await ctx.wait_for("go")
                return await charge()
            return f

        self.assertEqual(run(make(1).run(run_id="d")).status, "suspended")
        r = run(make(2).resume("d", signals={"go": 1}))
        self.assertEqual(r.status, "failed")
        self.assertIsInstance(r.error, ReplayDivergence)
        self.assertEqual(effects, [])

    def test_r4_2b_suspension_swallowed_by_gather_is_reported(self):
        events = []

        @flow(journal=MemoryJournal())
        async def f(ctx):
            await asyncio.gather(ctx.wait_for("approval"), return_exceptions=True)
            return "shipped"

        with self.assertLogs("pylinxi", "WARNING"):
            r = run(f.run(run_id="r", on_event=events.append))
        self.assertEqual(r.status, "completed")
        self.assertIn("suspension_ignored", [e["event"] for e in events])

    def test_r4_3_signal_to_a_failed_run_follows_the_retry_rules(self):
        j = SQLiteJournal(f"{self.d}/j.db")
        charges = []

        @step
        def charge():
            charges.append(1)
            raise RuntimeError("declined")

        @flow(journal=j)
        async def f(ctx):
            return await charge()

        self.assertEqual(run(f.run(run_id="c")).status, "failed")
        j.send_signal("c", "nudge", True)
        w = Worker(j, [f])                       # retry_failed=False
        for _ in range(3):
            self.assertEqual(run(w.tick()).completed, [])
        self.assertEqual(charges, [1])

    def test_r4_4_signal_sent_during_continue_as_new_is_delivered(self):
        class Racy(SQLiteJournal):
            inject = True

            def append(self, e):
                if e.kind == "done" and self.inject:
                    self.inject = False
                    SQLiteJournal.send_signal(self, e.run_id, "msg", "late")
                return super().append(e)

        j = Racy(f"{self.d}/j.db")

        @flow(journal=j)
        async def chat(ctx, turn=0):
            m = await ctx.wait_for("msg", timeout=3600)
            if turn == 0:
                ctx.continue_as_new(turn + 1)
            return m

        run(chat.run(0, run_id="c", signals={"msg": "first"}))
        r = run(chat.resume("c"))
        self.assertEqual((r.status, r.output), ("completed", "late"))
        r = run(chat.resume("c"))                # forwarded exactly once
        self.assertEqual(sum(1 for e in j.load("c.g1") if e.meta.get("forwarded_from")), 1)

    def test_r4_5_steps_in_flight_when_the_drain_budget_ends_are_journaled(self):
        sent = []

        @step
        async def send_email(i):
            sent.append(i)
            await asyncio.sleep(0.15)
            return i

        j = SQLiteJournal(f"{self.d}/j.db")

        @flow(journal=j, drain_timeout=0.4)
        async def f(ctx):
            async def mailer():
                for i in range(20):
                    await send_email(i)
            return await asyncio.gather(ctx.wait_for("go"), mailer())

        self.assertEqual(run(f.run(run_id="r")).status, "suspended")
        first = list(sent)
        journaled = [e.key for e in j.load("r") if e.kind == "step"]
        self.assertEqual(len(journaled), len(first))     # nothing that ran was left unjournaled
        sent.clear()
        run(f.resume("r"))
        self.assertFalse(set(first) & set(sent))         # and nothing is sent twice


if __name__ == "__main__":
    unittest.main()
