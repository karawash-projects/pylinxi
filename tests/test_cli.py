import asyncio
import contextlib
import io
import json
import os
import tempfile
import unittest

from pylinxi import SQLiteJournal, flow
from pylinxi.cli import main


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.url = f"sqlite:///{self.dir.name}/runs.db"
        j = SQLiteJournal(f"{self.dir.name}/runs.db", secret={"k1": b"s"})

        @flow(journal=j)
        async def f(ctx, mode):
            ctx.state["mode"] = mode
            await ctx.step(lambda: 1, name="a")
            if mode == "fail":
                raise ValueError("boom")
            if mode == "sleep":
                await ctx.sleep(3600)
            return "ok"

        for rid in ("ok", "fail", "sleep"):
            asyncio.run(f.run(rid, run_id=rid))
        j.close()

    def tearDown(self):
        self.dir.cleanup()

    def cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["--journal", self.url, "--key", "k1=s", *argv])
        return code, out.getvalue(), err.getvalue()

    def test_runs_and_status_filter(self):
        code, out, _ = self.cli("--json", "runs")
        self.assertEqual(code, 0)
        self.assertEqual({r["run_id"]: r["status"] for r in json.loads(out)},
                         {"ok": "completed", "fail": "failed", "sleep": "sleeping"})
        _, out, _ = self.cli("--json", "runs", "--status", "failed")
        self.assertEqual([r["run_id"] for r in json.loads(out)], ["fail"])

    def test_show_state_timers(self):
        code, out, _ = self.cli("show", "ok")
        self.assertEqual(code, 0)
        self.assertIn("a#0", out)
        _, out, _ = self.cli("state", "ok")
        self.assertEqual(json.loads(out), {"mode": "ok"})
        _, out, _ = self.cli("--json", "timers")
        self.assertEqual([t["run_id"] for t in json.loads(out)], ["sleep"])
        _, out, _ = self.cli("timers", "--due")
        self.assertIn("no pending timers", out)
        self.assertEqual(self.cli("show", "nope")[0], 1)

    def test_verify_exit_codes(self):
        self.assertEqual(self.cli("verify", "--all", "--require-signed")[0], 0)
        code, out, _ = main_no_key(self.url, "verify", "--all")
        self.assertEqual(code, 1)                      # keyed entries can't verify without the key
        with SQLiteJournal(f"{self.dir.name}/runs.db") as j:
            j._db.execute("UPDATE pylinxi_journal SET payload=replace(payload,'\"ok\"','\"KO\"') WHERE run_id='ok'")
        code, out, _ = self.cli("verify", "ok")
        self.assertEqual(code, 1)
        self.assertIn("FAIL", out)

    def test_delete_requires_yes(self):
        self.assertEqual(self.cli("delete", "ok")[0], 2)
        self.assertEqual(self.cli("delete", "ok", "--yes")[0], 0)
        _, out, _ = self.cli("--json", "runs")
        self.assertNotIn("ok", [r["run_id"] for r in json.loads(out)])

    def test_env_vars(self):
        os.environ["PYLINXI_JOURNAL"], os.environ["PYLINXI_KEYS"] = self.url, "k1=s"
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(["verify", "--all", "--require-signed"]), 0)
        finally:
            del os.environ["PYLINXI_JOURNAL"], os.environ["PYLINXI_KEYS"]


def main_no_key(url, *argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(["--journal", url, *argv])
    return code, out.getvalue(), err.getvalue()


if __name__ == "__main__":
    unittest.main()
