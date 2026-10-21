"""Streaming replies, durable follow-up timers, and a background worker.

A sales agent streams a reply, then waits for the customer. If they don't answer
before the deadline, it sends a follow-up -- even if the process restarted in
between. Run:  python examples/followup_agent.py
"""
import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pylinxi import SQLiteJournal, Worker, flow, idempotency_key, step  # noqa: E402

journal = SQLiteJournal(str(Path(tempfile.mkdtemp()) / "runs.db"), secret={"2026-q4": b"rotate-me"})
outbox = []


@step
async def draft_reply(lead: str):
    """Pretend LLM: streams tokens. Only the joined text is journaled."""
    for tok in ["Hi ", lead, ", ", "thanks ", "for ", "your ", "interest!"]:
        await asyncio.sleep(0.01)
        yield tok


@step
def send_email(to: str, body: str) -> str:
    key = idempotency_key()                 # same on retries/resumes -> provider dedupes
    outbox.append((to, body, key))
    return key


@flow(journal=journal)
async def nurture(ctx, lead: str):
    reply = await draft_reply(lead)
    await send_email(lead, reply)
    answer = await ctx.wait_for("customer_reply", timeout=0.3, default=None)  # 0.3s stands in for 3 days
    if answer is None:
        await send_email(lead, f"Just following up, {lead} -- any questions?")
        return "followed_up"
    return f"handed to sales: {answer}"


async def main():
    print("streaming the first reply:  ", end="", flush=True)
    async for ev in nurture.stream("Dana", run_id="lead-dana"):
        if ev["event"] == "chunk":
            print(ev["chunk"], end="", flush=True)
        elif ev["event"] == "result":
            r = ev["result"]
            print(f"\n-> {r.status}, waiting for {r.waiting_for!r} until the deadline")

    await nurture.run("Sam", run_id="lead-sam")
    print("Sam's run suspended too; Sam replies before the deadline:")
    r = await nurture.resume("lead-sam", signals={"customer_reply": "Can we talk Tuesday?"})
    print(f"-> {r.output}")

    print("\n(process restarts; a worker picks up due timers)")
    worker = Worker(journal, [nurture])
    await asyncio.sleep(0.35)
    report = await worker.tick()
    print(f"worker resumed {report.resumed}, completed {report.completed}")

    print("\noutbox:")
    for to, body, key in outbox:
        print(f"  to={to:<5} key={key:<28} {body}")
    print(f"\nall runs verify: {all(journal.verify(r, require_signed=True) for r in journal.runs())}")
    journal.close()


if __name__ == "__main__":
    asyncio.run(main())
