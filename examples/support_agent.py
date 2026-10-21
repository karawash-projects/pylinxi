"""A support agent that self-heals, survives a crash, waits for human approval,
and journals deltas instead of full state snapshots.  Run:  python examples/support_agent.py
"""
import asyncio
import json
import random
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pylinxi import CircuitBreaker, FileJournal, Policy, Retry, flow, step  # noqa: E402

JOURNAL = FileJournal(tempfile.mkdtemp(prefix="pylinxi-"), secret=b"rotate-me")
CRASH_ONCE = {"armed": True}
random.seed(1)


class WorkerKilled(BaseException):
    """Not an Exception, so no policy catches it -- like a real process crash."""


# --- a fake LLM that sometimes returns malformed JSON ------------------------
def fake_llm(prompt: str) -> str:
    if "previous error" not in prompt and random.random() < 0.7:
        return "{intent: refund"                       # broken JSON
    return json.dumps({"intent": "refund" if "charged twice" in prompt else "faq", "confidence": 0.93})


def valid_json(s: str) -> bool:
    try:
        return "intent" in json.loads(s)
    except ValueError:
        return False


def feed_error_back(err, args, kwargs, attempt):           # real-time self-repair
    return (args[0] + f"\n\nprevious error: {err}. Return strict JSON.",), kwargs


classify = step(fake_llm, name="classify", policy=Policy(
    retry=Retry(attempts=4, backoff=0.01), validate=valid_json, repair=feed_error_back))

billing_breaker = CircuitBreaker(threshold=3, reset_after=30)


@step(policy=Policy(retry=Retry(attempts=3, backoff=0.01), breaker=billing_breaker,
                    fallback=lambda order: {"order": order, "charges": "unavailable", "degraded": True}))
def lookup_charges(order: str) -> dict:
    if CRASH_ONCE["armed"]:
        CRASH_ONCE["armed"] = False
        raise WorkerKilled("💥 worker killed mid-run")
    return {"order": order, "charges": [49.0, 49.0]}


@step
def draft_reply(intent: str, charges: dict) -> str:
    if intent == "refund":
        return f"Refunding duplicate charge of ${charges['charges'][0]:.2f} on {charges['order']}."
    return "Here's our FAQ link."


@step
def send(reply: str) -> str:
    return f"SENT: {reply}"


@flow(journal=JOURNAL, on_event=lambda e: print(f"  · {e['event']:<15} {e['key']}"
                                                 f"{'  ' + str(e.get('healed')) if e.get('healed') else ''}"))
async def support(ctx, message: str, order: str):
    intent = json.loads(await classify(f"Classify: {message}"))["intent"]
    ctx.state["intent"] = intent

    if intent == "refund":                                 # branching = plain Python
        charges, history = await asyncio.gather(lookup_charges(order),
                                                ctx.step(lambda: ["2 prior tickets"], name="history"))
        ctx.state["history"] = history
        reply = await draft_reply(intent, charges)
        if not await ctx.wait_for("manager_approval"):    # durable human-in-the-loop
            return "escalated"
    else:
        reply = await draft_reply(intent, {})
    return await send(reply)


async def main():
    msg = "I was charged twice for order A-1001"
    print("run 1 (will crash):")
    try:
        await support.run(msg, "A-1001", run_id="ticket-1001")
    except WorkerKilled as e:
        print(f"  {e}")

    print("\nrun 2 (resume after restart):")
    r = await support.resume("ticket-1001")
    print(f"  -> status={r.status}, waiting_for={r.waiting_for}")

    print("\nrun 3 (manager approves, hours later):")
    r = await support.resume("ticket-1001", signals={"manager_approval": True})
    print(f"  -> {r.output}")

    entries = JOURNAL.load("ticket-1001")
    path = JOURNAL._path("ticket-1001")
    print(f"\njournal: {len(entries)} entries, {path.stat().st_size} bytes, "
          f"tamper-check={'OK' if JOURNAL.verify('ticket-1001') else 'FAIL'}")
    print("state rebuilt from deltas:", JOURNAL.state_at("ticket-1001"))


if __name__ == "__main__":
    asyncio.run(main())
