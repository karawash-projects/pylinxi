"""Load, verify and resume journals written by the previous release (see write_journals.py).
Exits non-zero on any incompatibility.  python tests/compat/read_journals.py DIR
"""
import asyncio
import dataclasses
import sys

from pylinxi import FileJournal, SQLiteJournal, __version__, default_codec, flow

d = sys.argv[1]

@dataclasses.dataclass
class Ticket:
    id: int
Ticket.__module__ = "__main__"
default_codec.register(Ticket)

for j in (FileJournal(f"{d}/files", secret={"k1": b"s"}), SQLiteJournal(f"{d}/j.db", secret={"k1": b"s"})):
    name = type(j).__name__
    @flow(journal=j)
    async def support(ctx, who):
        t = await ctx.step(lambda: Ticket(99), name="classify")   # must replay the recorded Ticket(7)
        ctx.state["n"] = t.id
        ok = await ctx.wait_for("approval")
        return f"{who}:{t.id}:{ok}"

    @flow(journal=j)
    async def loop(ctx, n):
        if n < 3:
            ctx.continue_as_new(n + 1)
        return await ctx.wait_for("stop")

    @flow(journal=j)
    async def done(ctx):
        return await ctx.step(lambda: 0, name="a") + 1

    ver = {rid: j.verify(rid, require_signed=True) for rid in sorted(j.runs())}
    r1 = asyncio.run(support.resume("suspended-run", signals={"approval": True}))
    r2 = asyncio.run(loop.resume("chain", signals={"stop": "bye"}))
    r3 = asyncio.run(done.resume("done-run"))
    after = all(j.verify(rid, require_signed=True) for rid in j.runs())
    print(name, __version__, "verify:", all(ver.values()), "|", r1.status, r1.output, "|",
          r2.status, r2.run_id, r2.output, "|", r3.status, r3.output, "| after-write verify:", after)
    assert all(ver.values()), ver
    assert (r1.status, r1.output) == ("completed", "ana:7:True"), r1
    assert (r2.status, r2.run_id, r2.output) == ("completed", "chain.g3", "bye"), r2
    assert (r3.status, r3.output) == ("completed", 42), r3
    assert after
print("compatible")
