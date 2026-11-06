"""Run with:  PYLINXI_PG_DSN="postgresql://user@host:5432/db" python -m unittest tests/test_postgres.py"""
import asyncio
import os
import threading
import unittest
import uuid

from pylinxi import Entry, flow, step
from pylinxi.journal import RunLocked

try:
    from pylinxi.postgres import PostgresJournal
except ImportError:      # psycopg missing
    PostgresJournal = None

DSN = os.environ.get("PYLINXI_PG_DSN")


@unittest.skipUnless(DSN and PostgresJournal, "set PYLINXI_PG_DSN and install psycopg")
class TestPostgresJournal(unittest.TestCase):
    def setUp(self):
        self.table = f"pylinxi_test_{uuid.uuid4().hex[:8]}"
        self.j = PostgresJournal(DSN, table=self.table, secret=b"k")

    def tearDown(self):
        self.j._conn.execute(f"DROP TABLE {self.table}")
        self.j._conn.execute(f"DROP TABLE IF EXISTS {self.table}_heads")
        self.j.close()

    def test_crash_and_resume_from_a_fresh_worker(self):
        calls = {"a": 0}
        boom = {"on": True}

        @step
        def a():
            calls["a"] += 1
            return {"n": 41}

        @step
        def b(x):
            if boom["on"]:
                raise RuntimeError("down")
            return x["n"] + 1

        def make(journal):
            @flow(journal=journal)
            async def f(ctx):
                ctx.state["password"] = "hunter2"
                return await b(await a())
            return f

        self.assertEqual(asyncio.run(make(self.j).run(run_id="pg1")).status, "failed")
        self.assertEqual(list(self.j.runs(kind="failed")), ["pg1"])

        boom["on"] = False
        worker2 = PostgresJournal(DSN, table=self.table, secret=b"k")   # "another machine"
        r = asyncio.run(make(worker2).resume("pg1"))
        worker2.close()
        self.assertEqual((r.status, r.output, calls["a"]), ("completed", 42, 1))
        self.assertTrue(self.j.verify("pg1"))
        raw = self.j._conn.execute(f"SELECT string_agg(payload, '') FROM {self.table}").fetchone()[0]
        self.assertNotIn("hunter2", raw)

    def test_concurrent_writers_keep_chain_linear(self):
        self.j.append(Entry("shared", "start", "start", {}))   # a run's first entry is its start

        def writer(i):
            j = PostgresJournal(DSN, table=self.table, secret=b"k")
            for n in range(25):
                j.append(Entry("shared", f"w{i}#{n}", "step", n))
            j.close()

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        entries = self.j.load("shared")
        self.assertEqual([e.seq for e in entries], list(range(101)))
        self.assertTrue(self.j.verify("shared"))

    def test_second_worker_cannot_run_same_run(self):
        other = PostgresJournal(DSN, table=self.table)
        with self.j.lock("busy"):
            with self.assertRaises(RunLocked):
                with other.lock("busy"):
                    pass
        with other.lock("busy"):     # released after the first worker finishes
            pass
        other.close()

    def test_timers_delete_and_rotation(self):
        @flow(journal=self.j)
        async def f(ctx):
            await ctx.sleep(3600)
            return "done"

        r = asyncio.run(f.run(run_id="sleepy"))
        self.assertEqual(r.status, "suspended")
        timers = self.j.pending_timers()
        self.assertEqual([t[0] for t in timers], ["sleepy"])
        self.assertAlmostEqual(timers[0][1], r.wake_at, places=3)
        self.assertEqual(self.j.pending_timers(due_before=0), [])

        rotated = PostgresJournal(DSN, table=self.table, secret={"default": b"k", "k2": b"new"})
        rotated.append(Entry("sleepy", "note#0", "step", 1))
        self.assertTrue(rotated.verify("sleepy", require_signed=True))
        self.assertEqual(rotated.load("sleepy")[-1].kid, "k2")
        rotated.close()

        self.j.delete("sleepy")
        self.assertEqual(self.j.load("sleepy"), [])
        self.assertEqual(self.j.pending_timers(), [])

    def test_execution_lock_never_blocks_other_runs_appends(self):
        # review #21: run "X:w" used the same lock key as run "X"'s appends
        import time
        self.j.append(Entry("acct", "start", "start", {}))
        with self.j.lock("acct:w"):
            t = time.perf_counter()
            self.j.append(Entry("acct", "a#0", "step", 1))
            self.assertLess(time.perf_counter() - t, 0.5)

    def _admin(self):
        import psycopg
        return psycopg.connect(DSN, autocommit=True)

    def test_lost_lock_fences_the_old_worker(self):
        # review #11: a worker whose lock connection drops must not keep writing
        import threading

        from pylinxi import LockLost
        effects, entered = [], threading.Event()
        admin = self._admin()

        def make(j, who):
            @flow(journal=j, name="f")
            async def f(ctx):
                def charge():
                    entered.set()
                    if who == "w1":
                        import time
                        time.sleep(0.6)
                    effects.append(who)
                await ctx.step(charge, name="charge")
                return who
            return f

        # a second worker process (a distinct connection string = a distinct in-process lock namespace)
        sep = "&" if "?" in DSN else "?"
        j2 = PostgresJournal(DSN + f"{sep}application_name=worker2", table=self.table, secret=b"k")

        async def main():
            t1 = asyncio.ensure_future(make(self.j, "w1").run(run_id="r"))
            while not entered.is_set():
                await asyncio.sleep(0.01)
            with self.assertRaises(RunLocked):
                await make(j2, "w2").run(run_id="r")
            admin.execute("SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' "
                          "AND granted AND pid <> pg_backend_pid()")
            await asyncio.sleep(0.1)
            r2 = await make(j2, "w2").run(run_id="r")
            with self.assertRaises(LockLost):
                await t1
            return r2
        r2 = asyncio.run(main())
        self.assertEqual(r2.output, "w2")
        ks = [(e.kind, e.key) for e in j2.load("r")]
        self.assertEqual(ks.count(("step", "charge#0")), 1)   # w1 could not journal after losing its lock
        self.assertEqual(ks.count(("done", "done")), 1)
        self.assertTrue(j2.verify("r", require_signed=True))
        j2.close()
        admin.close()

    def test_connections_stay_bounded_and_shared_connection_reconnects(self):
        # review #28
        admin = self._admin()

        @flow(journal=self.j)
        async def f(ctx, i):
            await ctx.step(lambda: __import__("time").sleep(0.05), name="w")
            return i

        def conns():
            return admin.execute("SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' "
                                 "AND pid <> pg_backend_pid()").fetchone()[0]
        base = conns()
        peak = {"n": 0}

        async def main():
            tasks = [asyncio.ensure_future(f.run(i, run_id=f"c{i}")) for i in range(40)]
            while not all(t.done() for t in tasks):
                peak["n"] = max(peak["n"], conns())
                await asyncio.sleep(0.01)
            return [t.result().output for t in tasks]
        self.assertEqual(asyncio.run(main()), list(range(40)))
        self.assertLessEqual(peak["n"] - base, self.j.lock_connections)
        pid = self.j._conn.info.backend_pid
        admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
        self.assertEqual(len(list(self.j.runs())), 40)          # reconnected transparently
        admin.close()

    def test_heads_index_and_worker_on_postgres(self):
        from pylinxi import Worker

        @flow(journal=self.j)
        async def f(ctx):
            await ctx.sleep(0.01, inline_max=0)
            return "woke"

        asyncio.run(f.run(run_id="h1"))
        import time
        time.sleep(0.02)
        self.assertEqual([r for r, _ in self.j.pending_timers(due_before=time.time())], ["h1"])
        self.assertEqual(asyncio.run(Worker(self.j, [f]).tick()).completed, ["h1"])
        self.assertEqual(self.j.pending_timers(), [])

    def test_tamper_detected_in_database(self):
        @flow(journal=self.j)
        async def f(ctx):
            return await ctx.step(lambda: 100, name="amount")

        asyncio.run(f.run(run_id="t"))
        self.j._conn.execute(f"UPDATE {self.table} SET payload=replace(payload, '100', '999') "
                             f"WHERE run_id='t' AND kind='step'")
        self.assertFalse(self.j.verify("t"))


if __name__ == "__main__":
    unittest.main()
