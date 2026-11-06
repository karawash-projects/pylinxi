# Getting started

## Install

```bash
pip install pylinxi
```

## 1. A flow and two steps

```python
import asyncio
from pylinxi import SQLiteJournal, flow, step

@step
def fetch_order(order_id):
    print("fetching", order_id)          # pretend this is an API call
    return {"id": order_id, "total": 42}

@step
def send_receipt(order):
    print("emailing receipt for", order["id"])
    return "sent"

@flow(journal=SQLiteJournal("runs.db"))
async def receipt(ctx, order_id):
    order = await fetch_order(order_id)
    return await send_receipt(order)

print(asyncio.run(receipt.run("A-1", run_id="receipt-A-1")).output)
```

Run it: both steps print, and the output is `sent`. The run, each step's result,
and its final output are now in `runs.db`.

## 2. Resume replays instead of re-running

```python
print(asyncio.run(receipt.resume("receipt-A-1")).output)
```

Nothing prints this time — both steps return their journaled results — and the
output is still `sent`. That is the whole idea: if the process had died between
the two steps, `resume` would skip `fetch_order` and run only `send_receipt`.

## 3. The one rule

Code **outside** steps runs again on every resume, so it must be deterministic:
put I/O, LLM calls, randomness and clocks inside steps (or use `await ctx.now()`
and `await ctx.uuid()`, which record their value once). Plain logic — `if`,
loops, string formatting — is fine anywhere.

## 4. Heal a flaky step

```python
from pylinxi import Policy, Retry

@step(policy=Policy(retry=Retry(attempts=3, backoff=0.5), timeout=10,
                    fallback=lambda order_id: {"id": order_id, "total": None}))
def fetch_order(order_id):
    ...
```

See [Self-healing](self-healing.md) for validation, repair hooks and circuit breakers.

## 5. Wait for a human

```python
@flow(journal=SQLiteJournal("runs.db"))
async def refund(ctx, order_id):
    approved = await ctx.wait_for("approval", timeout=86400, default=False)
    return "refunded" if approved else "escalated"

r = asyncio.run(refund.run("A-1", run_id="refund-A-1"))   # r.status == "suspended"
# later, from any process:
SQLiteJournal("runs.db").send_signal("refund-A-1", "approval", True)
```

A `Worker` resumes the run once the signal arrives. See [Durable waits & signals](durable-waits.md).

## Next

- Read [Concepts](concepts.md) for exactly what replay guarantees.
- Browse the [Examples](examples.md).
