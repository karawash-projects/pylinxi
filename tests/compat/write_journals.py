"""Write journals in every state a newer pylinxi must still handle.

CI runs this with the *previous* release installed from PyPI, then read_journals.py
with the current code:  python tests/compat/write_journals.py DIR
"""
import asyncio
import dataclasses
import sys

import pylinxi
from pylinxi import FileJournal, SQLiteJournal, default_codec, flow

d = sys.argv[1]

@dataclasses.dataclass
class Ticket:
    id: int
default_codec.register(Ticket)

for j in (FileJournal(f"{d}/files", secret={"k1": b"s"}), SQLiteJournal(f"{d}/j.db", secret={"k1": b"s"})):
    @flow(journal=j)
    async def support(ctx, who):
        t = await ctx.step(lambda: Ticket(7), name="classify")
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
        return await ctx.step(lambda: 41, name="a") + 1

    asyncio.run(support.run("ana", run_id="suspended-run"))
    asyncio.run(loop.run(0, run_id="chain"))
    asyncio.run(done.run(run_id="done-run"))
print("wrote journals with pylinxi", pylinxi.__version__)
