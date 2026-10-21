import asyncio
import time
import unittest

from pylinxi import MemoryJournal, flow
from pylinxi.worker import Worker


class TestWorker(unittest.TestCase):
    def test_resumes_due_timers(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def reminder(ctx, who):
            await ctx.sleep(0.1, inline_max=0)
            return f"reminded {who}"

        @flow(journal=j)
        async def other(ctx):
            await ctx.sleep(3600)

        asyncio.run(reminder.run("ana", run_id="r1"))
        asyncio.run(other.run(run_id="r2"))
        w = Worker(j, [reminder, other])
        self.assertEqual(asyncio.run(w.tick()).resumed, [])          # nothing due yet
        time.sleep(0.11)
        rep = asyncio.run(w.tick())
        self.assertEqual((rep.resumed, rep.completed), (["r1"], ["r1"]))
        self.assertEqual(asyncio.run(reminder.resume("r1")).output, "reminded ana")

    def test_retries_failed_runs_with_backoff_then_gives_up(self):
        j = MemoryJournal()
        outage = {"left": 10}

        @flow(journal=j)
        async def sync_crm(ctx):
            def call():
                if outage["left"] > 0:
                    outage["left"] -= 1
                    raise ConnectionError("CRM down")
                return "synced"
            return await ctx.step(call, name="crm")

        asyncio.run(sync_crm.run(run_id="c1"))
        w = Worker(j, [sync_crm], retry_failed=True, max_failures=3, backoff=0.05)
        self.assertEqual(asyncio.run(w.tick()).resumed, [])          # backoff not elapsed
        time.sleep(0.06)
        self.assertEqual(asyncio.run(w.tick()).resumed, ["c1"])      # 2nd failure
        time.sleep(0.11)                                              # backoff doubled
        self.assertEqual(asyncio.run(w.tick()).resumed, ["c1"])      # 3rd failure
        self.assertEqual(asyncio.run(w.tick()).gave_up, ["c1"])
        self.assertEqual(asyncio.run(w.tick()).gave_up, [])          # reported once

    def test_transient_outage_recovers_via_worker(self):
        j = MemoryJournal()
        outage = {"left": 1}

        def call():
            if outage["left"] > 0:
                outage["left"] -= 1
                raise ConnectionError("CRM down")
            return "synced"

        @flow(journal=j)
        async def sync_crm(ctx):
            return await ctx.step(call, name="crm")

        self.assertEqual(asyncio.run(sync_crm.run(run_id="c1")).status, "failed")
        time.sleep(0.06)
        rep = asyncio.run(Worker(j, [sync_crm], retry_failed=True, backoff=0.05).tick())
        self.assertEqual(rep.completed, ["c1"])
        self.assertEqual(asyncio.run(sync_crm.resume("c1")).output, "synced")

    def test_unknown_flow_is_skipped_and_run_forever_stops(self):
        j = MemoryJournal()

        @flow(journal=j)
        async def a(ctx):
            await ctx.sleep(0.01, inline_max=0)

        asyncio.run(a.run(run_id="x"))
        time.sleep(0.02)
        self.assertEqual(asyncio.run(Worker(j, []).tick()).skipped, ["x"])

        async def go():
            stop = asyncio.Event()
            w = Worker(j, [a])
            task = asyncio.ensure_future(w.run_forever(interval=0.01, stop=stop))
            await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(task, 1)
        asyncio.run(go())
        self.assertEqual(asyncio.run(a.resume("x")).status, "completed")


if __name__ == "__main__":
    unittest.main()
