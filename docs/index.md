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

## Why

| Gap in graph frameworks | pylinxi |
|---|---|
| **Manual graph wiring** for branching | The flow *is* the code: `if`, `match`, loops, `asyncio.gather`. `ctx.route()` replaces conditional edges in one line. Steps are plain functions outside a flow, so they unit-test without a harness. |
| **Weak real-time self-healing** | One declarative policy per step: retry → validate → **repair** (rewrite inputs between attempts) → fallback → circuit breaker. Crashes heal by **resume**: finished steps replay from the journal. A `Worker` resumes due timers and transient failures fleet-wide. |
| **Heavy state serialization** | Append-only **delta** journal: each entry stores one step's result plus only the state keys that changed. Rebuild any point in time with `state_at()`. Long loops roll over with `continue_as_new()`. |
| **Enterprise security** | HMAC-signed hash chain with key rotation, automatic redaction, never-persisted sensitive steps, per-run locks, retention deletes, idempotency keys, OTel tracing, a `verify` CLI for audits. |

## Where next

- [Getting started](getting-started.md) — your first durable flow in five minutes.
- [Concepts](concepts.md) — the replay model and its guarantees.
- [Examples](examples.md) — runnable agents, sagas, approvals and scheduled jobs.
- [Going to production](production.md) — Postgres, workers, tracing, deploys.
