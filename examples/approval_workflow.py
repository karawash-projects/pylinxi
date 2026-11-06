"""Human-in-the-loop approval. An expense waits for a manager's decision; if nobody
answers in time it escalates to finance. The run is suspended while waiting (no
process, thread or connection held), and the decision can arrive from anywhere --
a web handler, a Slack button, another service -- via journal.send_signal().
A Worker resumes runs when a signal arrives or a deadline passes.
Run:  python examples/approval_workflow.py
"""
import asyncio
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pylinxi import SQLiteJournal, Worker, flow, step  # noqa: E402

DB = str(Path(tempfile.mkdtemp()) / "approvals.db")
journal = SQLiteJournal(DB, secret=b"rotate-me")
notifications = []
# In production these are hours/days; seconds keep the demo short.
MANAGER_SLA, FINANCE_SLA = 0.5, 30.0


@step
def notify(who: str, text: str) -> None:
    notifications.append((who, text))
    print(f"  -> notify {who}: {text}")


@step
def pay(expense_id: str, amount: float) -> str:
    print(f"  -> paid {expense_id} ({amount:.2f})")
    return f"pay-{expense_id}"


@flow(journal=journal)
async def expense(ctx, expense_id: str, amount: float, employee: str):
    await notify("manager", f"approve {expense_id} for {amount:.2f}?")
    decision = await ctx.wait_for("decision", timeout=MANAGER_SLA, default=None)
    if decision is None:                                   # manager didn't answer in time
        await notify("finance", f"{expense_id} escalated: manager did not answer")
        decision = await ctx.wait_for("decision", timeout=FINANCE_SLA, default="rejected")
    ctx.state["decision"] = decision
    if decision == "approved":
        return {"status": "paid", "payment": await pay(expense_id, amount)}
    await notify(employee, f"{expense_id} was {decision}")
    return {"status": decision}


def web_handler(run_id: str, decision: str) -> None:
    """Somewhere else entirely (a different process): just record the decision."""
    SQLiteJournal(DB, secret=b"rotate-me").send_signal(run_id, "decision", decision)


async def main() -> None:
    worker = Worker(journal, [expense])

    print("EXP-1: the manager approves quickly")
    r = await expense.run("EXP-1", 120.0, "ana", run_id="EXP-1")
    print("  status:", r.status, "waiting for", r.waiting_for)
    web_handler("EXP-1", "approved")
    print("  worker tick:", (await worker.tick()).completed)

    print("EXP-2: no answer from the manager -> escalated to finance, who rejects it")
    r = await expense.run("EXP-2", 9800.0, "ben", run_id="EXP-2")
    time.sleep(MANAGER_SLA + 0.1)                          # the manager's deadline passes
    await worker.tick()                                    # wakes the run, it escalates and suspends again
    web_handler("EXP-2", "rejected")
    print("  worker tick:", (await worker.tick()).completed)

    results = {rid: expense.journal.load(rid)[-1].result for rid in ("EXP-1", "EXP-2")}
    print("\nfinal:", results)
    assert results["EXP-1"]["status"] == "paid" and results["EXP-2"]["status"] == "rejected"
    assert [w for w, _ in notifications] == ["manager", "manager", "finance", "ben"]


if __name__ == "__main__":
    asyncio.run(main())
