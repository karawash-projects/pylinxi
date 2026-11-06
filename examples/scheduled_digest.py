"""A long-running scheduled job: every "day" it collects new items and sends a
digest, then sleeps durably until the next day. Every few iterations it calls
ctx.continue_as_new() so the journal (and replay time) stays small no matter how
long the job lives -- months of daily runs never replay more than a few days.
A Worker resumes it whenever the sleep is due; restarting the worker loses nothing.
Run:  python examples/scheduled_digest.py
"""
import asyncio
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pylinxi import SQLiteJournal, Worker, flow, step  # noqa: E402

journal = SQLiteJournal(str(Path(tempfile.mkdtemp()) / "jobs.db"), secret=b"rotate-me")
DAY = 0.2                 # seconds per "day" for the demo
DAYS_PER_GENERATION = 3   # continue as new after this many iterations
sent = []


@step
def fetch_new_items(day: int) -> list:
    return [f"item-{day}-{i}" for i in range(day % 3 + 1)]


@step
def send_digest(day: int, items: list) -> str:
    sent.append(day)
    print(f"  day {day}: digest with {len(items)} item(s)")
    return f"msg-{day}"


@flow(journal=journal)
async def digest(ctx, start_day: int = 0, stop_after: int = 7):
    ctx.state.setdefault("total_items", 0)
    day = start_day
    for _ in range(DAYS_PER_GENERATION):
        if day >= stop_after:
            return {"days": day, "total_items": ctx.state["total_items"]}
        items = await fetch_new_items(day)
        await send_digest(day, items)
        ctx.state["total_items"] += len(items)
        day += 1
        await ctx.sleep(DAY, inline_max=0)      # suspends: no process held while waiting
    ctx.continue_as_new(start_day=day, stop_after=stop_after)   # fresh, short journal; state carries over


async def main() -> None:
    r = await digest.run(run_id="daily")
    print("started:", r.status, "| generation journal:", r.run_id)
    worker = Worker(journal, [digest])

    def generation(run_id: str) -> int:
        return int(run_id.rsplit(".g", 1)[1]) if ".g" in run_id else 0

    def finished() -> bool:
        latest = max((r for r in journal.runs() if r.startswith("daily")), key=generation)
        last = journal.load(latest)[-1]
        return last.kind == "done" and not last.meta.get("continued_as")

    deadline = time.time() + 10
    while not finished() and time.time() < deadline:
        await worker.tick()                     # in production: await worker.run_forever()
        await asyncio.sleep(DAY / 4)

    runs = sorted((r for r in journal.runs() if r.startswith("daily")), key=generation)
    final = journal.load(runs[-1])[-1].result
    print("\ngenerations:", runs)
    print("longest generation journal:", max(len(journal.load(r)) for r in runs), "entries")
    print("result:", final)
    assert sent == list(range(7)), sent                     # every day exactly once
    assert final["total_items"] == sum(d % 3 + 1 for d in range(7))


if __name__ == "__main__":
    asyncio.run(main())
