# pylinxi

**Durable, self-healing agent flows written as plain Python.** No graph to wire.

A flow is an ordinary `async def`; every call to a `@step` is journaled. If the
process dies, `resume(run_id)` runs the function again from the top: finished
steps return their recorded result instantly, and execution continues at the
first unfinished step. Nothing that already happened runs twice.

```python
import asyncio
from pylinxi import flow, step, idempotency_key, Policy, Retry, SQLiteJournal

@step(policy=Policy(retry=Retry(attempts=4), validate=is_json, repair=feed_error_back))
async def classify(message):
    return await llm(f"Classify: {message}")

@step
def refund(order):
    return billing.refund(order, idempotency_key=idempotency_key())

@flow(journal=SQLiteJournal("runs.db", secret=KEY))
async def support(ctx, message, order):
    if await classify(message) == "refund":               # branching is just `if`
        if not await ctx.wait_for("approval", timeout=86400, default=False):
            return "escalated"
        return await refund(order)
    return await answer_faq(message)
```

```bash
pip install pylinxi                    # core: zero dependencies, Python 3.9+
pip install "pylinxi[postgres,otel]"   # production extras
```

!!! note "Status: beta"
    The API is stable within 1.x and journals stay readable across 1.x releases.
    pylinxi is young: test it against your own failure modes before relying on it
    in production.

## Features

- **Plain Python control flow** — `if`, loops, `asyncio.gather`, `TaskGroup`; no graph or DSL.
- **Crash-proof execution** — resume any run by id, from any process; finished steps are never executed twice.
- **Self-healing steps** — retry, timeout, validation, input repair, fallback and circuit breaker in one policy.
- **Durable waits** — `ctx.sleep()` and `ctx.wait_for()` free the worker and survive restarts.
- **Lightweight state** — per-step deltas instead of full snapshots, with point-in-time reconstruction.
- **Tamper-evident audit trail** — signed hash chain, key rotation, redaction, retention deletes, `verify` CLI.
- **Production backends** — memory, file, SQLite and PostgreSQL journals; many workers with locking and fencing.
- **Zero core dependencies** — fully typed, Python 3.9–3.13.

## Where next

- [Getting started](getting-started.md) — your first durable flow in five minutes.
- [Concepts](concepts.md) — the replay model and its guarantees.
- [Examples](examples.md) — runnable agents, sagas, approvals and scheduled jobs.
- [Going to production](production.md) — Postgres, workers, tracing, deploys.
