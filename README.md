<h1 align="center">pylinxi</h1>

<p align="center">
  <strong>Durable, self-healing workflows and AI agents in plain Python.</strong>
</p>

<p align="center">
  <a href="https://github.com/karawash-projects/pylinxi/actions/workflows/ci.yml"><img src="https://github.com/karawash-projects/pylinxi/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pypi.org/project/pylinxi/"><img src="https://img.shields.io/pypi/v/pylinxi" alt="PyPI version"></a>
  <a href="https://pypi.org/project/pylinxi/"><img src="https://img.shields.io/pypi/pyversions/pylinxi" alt="Python versions"></a>
  <a href="https://github.com/karawash-projects/pylinxi/blob/main/LICENSE"><img src="https://img.shields.io/pypi/l/pylinxi" alt="License: MIT"></a>
</p>

<p align="center">
  <a href="https://karawash-projects.github.io/pylinxi/">Documentation</a> ·
  <a href="https://karawash-projects.github.io/pylinxi/getting-started/">Getting started</a> ·
  <a href="https://github.com/karawash-projects/pylinxi/tree/main/examples">Examples</a> ·
  <a href="https://github.com/karawash-projects/pylinxi/blob/main/CHANGELOG.md">Changelog</a>
</p>

---

pylinxi makes long-running Python code survive anything that can stop a process.
A flow is an ordinary `async` function; every call to a step is recorded in a
signed journal. When a process crashes, is redeployed, or waits days for a human,
the run resumes exactly where it stopped: finished steps return their recorded
result instantly and are **never executed twice**.

> **Status: beta.** The API is stable within 1.x and journals stay readable across
> 1.x releases. Test pylinxi against your own failure modes before relying on it
> in production.

## Contents

- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Usage](#usage)
- [Running in production](#running-in-production)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

## Features

- **Plain Python control flow.** Branch with `if`, loop with `for`, run work in
  parallel with `asyncio.gather` or `TaskGroup`. There is no graph or DSL to learn.
- **Crash-proof execution.** Resume any run by id, from any process; completed
  steps replay from the journal and side effects are not repeated.
- **Self-healing steps.** One declarative policy per step: retry with backoff,
  per-attempt timeout, output validation, input repair between attempts,
  fallback, and a shared circuit breaker.
- **Durable waits.** `ctx.sleep()` and `ctx.wait_for(signal, timeout=...)` free
  the worker and survive restarts; signals can be sent from any process.
- **Lightweight state.** The journal stores per-step deltas instead of full
  snapshots; rebuild state at any point in a run's history.
- **Tamper-evident audit trail.** HMAC-signed hash chain with key rotation,
  automatic redaction of secret-looking keys, retention deletes, and a `verify`
  command for audits.
- **Production backends.** In-memory, file, SQLite and PostgreSQL journals.
  PostgreSQL supports many concurrent workers with per-run locking and fencing.
- **Observability.** Structured events for every step and an optional
  OpenTelemetry exporter.
- **Zero core dependencies.** Fully typed, Python 3.9–3.13.

## Installation

```bash
pip install pylinxi
```

Optional extras:

```bash
pip install "pylinxi[postgres]"   # PostgreSQL journal (psycopg 3)
pip install "pylinxi[otel]"       # OpenTelemetry tracing
```

## Quick start

```python
import asyncio
from pylinxi import SQLiteJournal, flow, step

@step
def fetch_order(order_id: str) -> dict:
    print("fetching", order_id)              # runs once, then replays from the journal
    return {"id": order_id, "total": 42}

@step
def send_receipt(order: dict) -> str:
    print("emailing receipt for", order["id"])
    return "sent"

@flow(journal=SQLiteJournal("runs.db"))
async def receipt(ctx, order_id: str) -> str:
    order = await fetch_order(order_id)
    return await send_receipt(order)

result = asyncio.run(receipt.run("A-1001", run_id="receipt-A-1001"))
print(result.status, result.output)          # completed sent

# Later, from any process: nothing is executed again.
result = asyncio.run(receipt.resume("receipt-A-1001"))
```

## How it works

1. Every call to a `@step` inside a flow is journaled with its result.
2. `resume(run_id)` runs the flow function again from the top. Steps that already
   finished return their recorded result without executing; execution continues at
   the first step that did not finish.
3. **The one rule:** code *outside* steps must be deterministic. Put I/O, model
   calls, randomness and clocks inside steps, or use `await ctx.now()` and
   `await ctx.uuid()`.

If flow code changes so that replay no longer matches the journal, the run stops
with `ReplayDivergence` *before* executing anything new. Use `ctx.patched()` to
change code safely while runs are in flight. See
[Concepts](https://karawash-projects.github.io/pylinxi/concepts/) for the full
list of replay guarantees.

## Usage

### Self-healing steps

```python
from pylinxi import CircuitBreaker, Policy, Retry, step

billing = CircuitBreaker(threshold=5, reset_after=30)      # share one per dependency

@step(policy=Policy(
    retry=Retry(attempts=4, backoff=0.5, on=(ConnectionError, TimeoutError)),
    timeout=20,                                            # seconds per attempt
    validate=lambda out: "intent" in out,                  # False -> retried
    repair=lambda err, args, kwargs, attempt: ((args[0] + f"\nFix: {err}",), kwargs),
    fallback=lambda prompt: {"intent": "unknown"},
    breaker=billing,
))
def classify(prompt: str) -> dict:
    ...
```

### Human approval and deadlines

```python
@flow(journal=journal)
async def expense(ctx, expense_id: str, amount: float):
    decision = await ctx.wait_for("decision", timeout=86_400, default="rejected")
    if decision == "approved":
        return await pay(expense_id, amount)
    return "rejected"

await expense.run("EXP-1", 120.0, run_id="EXP-1")         # status: "suspended"

# From a web handler or any other process:
journal.send_signal("EXP-1", "decision", "approved")       # a Worker resumes the run
```

### Parallel branches

```python
@flow(journal=journal)
async def enrich(ctx, customer_id: str):
    profile, orders = await asyncio.gather(get_profile(customer_id), get_orders(customer_id))
    scores = await ctx.map(score_order, orders, concurrency=8)   # each item is its own checkpoint
    return {"profile": profile, "scores": scores}
```

### Idempotent side effects

```python
from pylinxi import idempotency_key

@step
def charge(order: dict) -> str:
    # "<run_id>:<step_key>", identical across retries and resumes
    return payments.charge(order["total"], idempotency_key=idempotency_key())
```

More patterns — streaming steps, long-lived loops with `continue_as_new`, sensitive
steps, events — are covered in the
[Flow API guide](https://karawash-projects.github.io/pylinxi/flow-api/).

## Running in production

| Journal | Use it for |
|---|---|
| `MemoryJournal()` | Tests |
| `FileJournal(directory)` | A single process; one JSONL file per run |
| `SQLiteJournal(path)` | One machine, several processes |
| `PostgresJournal(dsn)` | Many workers sharing one database |

```python
from pylinxi import Worker
from pylinxi.postgres import PostgresJournal

journal = PostgresJournal("postgresql://app@db:5432/agents", secret={"2026-q4": KEY})
worker = Worker(journal, [expense, enrich], retry_failed=True, max_failures=5)
await worker.run_forever(interval=5)
```

A `Worker` resumes runs whose timers are due, runs that received a signal, runs
left open by a crashed process, and (optionally) runs that failed, with backoff.
Run as many workers as you need: each run executes on one worker at a time.

Audit and operate journals from the command line:

```bash
export PYLINXI_JOURNAL=postgresql://app@db/agents PYLINXI_KEYS_FILE=/run/secrets/pylinxi-keys

pylinxi runs --status failed
pylinxi show EXP-1               # every entry: step, attempts, healing, state changes
pylinxi state EXP-1 --seq 3      # state as of entry 3
pylinxi verify --all             # exit code 1 if any run fails verification
pylinxi delete EXP-1 --yes       # retention and erasure requests
```

See [Going to production](https://karawash-projects.github.io/pylinxi/production/)
for deployment, tracing and a pre-launch checklist, and
[Upgrading from 1.0](https://karawash-projects.github.io/pylinxi/upgrading/) if
you are moving from an earlier release.

## Documentation

Full documentation is available at
**[karawash-projects.github.io/pylinxi](https://karawash-projects.github.io/pylinxi/)**:

- [Getting started](https://karawash-projects.github.io/pylinxi/getting-started/)
- [Concepts and replay guarantees](https://karawash-projects.github.io/pylinxi/concepts/)
- [Self-healing](https://karawash-projects.github.io/pylinxi/self-healing/)
- [Durable waits and signals](https://karawash-projects.github.io/pylinxi/durable-waits/)
- [Journals](https://karawash-projects.github.io/pylinxi/journals/)
- [Security and audit](https://karawash-projects.github.io/pylinxi/security/)
- [Command line](https://karawash-projects.github.io/pylinxi/cli/)
- [API reference](https://karawash-projects.github.io/pylinxi/api/)

Runnable, self-checking examples are in [`examples/`](https://github.com/karawash-projects/pylinxi/tree/main/examples).

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](https://github.com/karawash-projects/pylinxi/blob/main/CONTRIBUTING.md) for the
development setup, checks and guidelines. Planned work is listed in
[ROADMAP.md](https://github.com/karawash-projects/pylinxi/blob/main/ROADMAP.md).

```bash
git clone https://github.com/karawash-projects/pylinxi && cd pylinxi
pip install -e ".[dev]"
python -m unittest discover -s tests -t .
```

## Security

Please report vulnerabilities privately as described in [SECURITY.md](https://github.com/karawash-projects/pylinxi/blob/main/SECURITY.md).
Do not open public issues for security reports.

## License

pylinxi is released under the [MIT License](https://github.com/karawash-projects/pylinxi/blob/main/LICENSE).
