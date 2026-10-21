# pylinxi

[![ci](https://github.com/karawash-projects/pylinxi/actions/workflows/ci.yml/badge.svg)](https://github.com/karawash-projects/pylinxi/actions/workflows/ci.yml)

Durable, self-healing agent flows written as **plain Python**. No graph to wire. The core has zero dependencies; Postgres and OpenTelemetry are optional extras. Python 3.9+.

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
    intent = await classify(message)
    if intent == "refund":                                   # branching is just `if`
        charges, history = await asyncio.gather(lookup(order), past_tickets(order))
        if not await ctx.wait_for("approval", timeout=86400, default=False):
            return "escalated"
        return await refund(order)
    return await answer_faq(message)

r = await support.run("charged twice", "A-1001", run_id="t-1001")    # suspends at approval
r = await support.resume("t-1001", signals={"approval": True})       # hours later, any process
```

```bash
pip install pylinxi                    # core
pip install "pylinxi[postgres,otel]"   # production extras
```

---

## Why

| Gap in graph frameworks | pylinxi |
|---|---|
| **Manual graph wiring** for branching | The flow *is* the code: `if`, `match`, loops, `asyncio.gather`. `ctx.route()` replaces conditional edges in one line. Steps are plain functions outside a flow, so they unit-test without a harness. |
| **Weak real-time self-healing** | One declarative policy per step: retry → validate → **repair** (rewrite inputs between attempts) → fallback → circuit breaker. Crashes heal by **resume**: finished steps replay from the journal. A `Worker` resumes due timers and transient failures fleet-wide. |
| **Heavy state serialization** | Append-only **delta** journal: each entry stores one step's result plus only the state keys that changed. Rebuild any point in time with `state_at()`. Long loops roll over with `continue_as_new()`. |
| **Enterprise security** | HMAC-signed hash chain with key rotation, automatic redaction, never-persisted sensitive steps, per-run locks, retention deletes, idempotency keys, OTel tracing, a `verify` CLI for audits. |

---

## Concepts in 60 seconds

- A **flow** is an `async def f(ctx, ...)` decorated with `@flow`. Run it with `await f.run(...)`.
- A **step** is any function decorated with `@step` (sync, async, or a generator for streaming). Calling it inside a flow journals its result.
- A **journal** stores each run as a signed list of entries. On `resume(run_id)` the flow function runs again from the top; completed steps return their recorded result instantly and execution continues at the first unfinished step.
- **The one rule:** code *outside* steps must be deterministic. Put I/O, LLM calls, randomness and clocks inside steps, or use `ctx.now()` / `ctx.uuid()`. If flow code changes incompatibly between a crash and a resume, pylinxi emits a `divergence` event; use `ctx.patched()` to change code safely.

### Replay guarantees

- **Concurrency.** Tasks created by flow code (`asyncio.gather`, `create_task`, `TaskGroup`) each get their own key scope (`t0/`, `t1/`, …) in creation order, so steps inside concurrent branches replay correctly whichever branch finishes first. When a run suspends, fails or finishes, branches still running are cancelled before it returns, so they can't act twice.
- **Handled failures stay handled.** If flow code catches a `StepFailed` and carries on, resuming re-raises the same failure (wrapping a `ReplayedError`) instead of re-running the step. A failure that ended the run is retried on resume.
- **Nested steps.** A `@step` called from inside another step is a plain call; the outer step's result is the checkpoint.
- **Suspension can't be swallowed.** `Suspended` and `ContinueAsNew` derive from `BaseException`, so `except Exception` in flow code won't catch them.
- **One executor per run.** `run`/`resume` hold a per-run lock (in-process for every journal, cross-process for file/SQLite on POSIX, database locks for Postgres). A second caller gets `RunLocked`.
- **Inputs are journaled in clear** so runs can resume. pylinxi therefore refuses flow inputs, initial state, signals or `continue_as_new` payloads containing secret-looking keys; pass secrets by reference and load them in a `@step(sensitive=True)`.

---

## Self-healing

```python
from pylinxi import Policy, Retry, CircuitBreaker

crm = CircuitBreaker(threshold=5, reset_after=30)         # share one per dependency

@step(policy=Policy(
    retry=Retry(attempts=4, backoff=0.5, factor=2, max_backoff=10, on=(ConnectionError, TimeoutError)),
    timeout=20,                                            # per attempt
    validate=lambda out: "intent" in out,                  # False -> ValidationError -> retried
    repair=lambda err, args, kwargs, attempt: ((args[0] + f"\nFix: {err}",), kwargs),
    fallback=lambda *a, **k: {"intent": "unknown"},        # after retries, or when breaker is open
    breaker=crm,
))
def classify(prompt): ...
```

| Field | Meaning |
|---|---|
| `retry` | `Retry(attempts, backoff, factor, max_backoff, jitter, on)`. Default: 1 attempt. |
| `timeout` | Seconds per attempt. Sync steps run in a thread, so a timed-out thread can't be killed; prefer async clients for hard timeouts. |
| `validate` | `fn(result) -> bool` (may be async). Validation failures are always retried. |
| `repair` | `fn(error, args, kwargs, attempt) -> (args, kwargs) | None` (may be async). Runs between attempts. |
| `fallback` | `fn(*args, **kwargs)` used when attempts are exhausted or the breaker is open. |
| `breaker` | `CircuitBreaker(threshold, reset_after)`; closed → open → half-open. |
| `aggregate` | Streaming steps: `fn(chunks) -> result`. Default joins strings, else keeps the list. |
| `thread` | Run plain sync steps in a worker thread (default `True`). |

Each completed step's journal entry records `attempts`, `healed` (`"retry"` / `"fallback"`) and the errors seen.

### Fleet-level healing: `Worker`

```python
from pylinxi import Worker

worker = Worker(journal, [support, onboarding], retry_failed=True, max_failures=5, backoff=30)
await worker.run_forever(interval=5)          # or: report = await worker.tick()
```

Each tick resumes runs whose timers are due and (optionally) runs that *ended* in an unhandled failure, with exponential backoff and a give-up limit. Failures your flow caught are left alone. One run that errors while resuming is reported in `TickReport.errors` and never blocks the others. Run several workers against a `PostgresJournal`; per-run locks make them skip runs another worker owns.

---

## Flow API (`ctx`)

| Call | What it does |
|---|---|
| `await my_step(...)` | Journaled call of a `@step` function. |
| `await ctx.step(fn, *args, name=, policy=, sensitive=)` | Journaled call of any callable (lambdas, third-party functions). |
| `await ctx.map(fn, items, concurrency=8)` | Parallel fan-out; each item is its own checkpoint. Siblings finish before an error surfaces, so a resume re-runs only failed items. |
| `await ctx.route(label, {"a": step_a, ...}, *args, default=)` | Dispatch by label. |
| `await ctx.wait_for(signal, timeout=None, default=None)` | Wait for `run/resume(..., signals={signal: value})`. Signals are journaled on delivery, so early ones aren't lost and repeats queue in order. With `timeout`, returns `default` once it expires. |
| `await ctx.sleep(seconds, inline_max=1.0)` | Durable sleep. Short waits sleep in-process; longer ones suspend and free the worker. |
| `await ctx.now()` / `await ctx.uuid()` | Clock / random id recorded once, identical on replay. |
| `ctx.patched("change-id")` | `True` for runs that reach this point on new code, `False` for runs already past it. |
| `ctx.continue_as_new(*args, **kwargs)` | End this run; continue as `run_id.g1`, `.g2`… with the current state. |
| `ctx.state` | Dict of flow state. Changes are journaled as deltas at the next checkpoint. |
| `ctx.run_id`, `ctx.generation`, `ctx.replaying` | Run metadata. |
| `pylinxi.idempotency_key()` | Inside a step: `"<run_id>:<step_key>"`, stable across retries and resumes. |

`@step(sensitive=True)` keeps a step's result out of the journal entirely; it is recomputed on resume. Use it for tokens and credentials.

### Running

```python
r = await support.run(*args, run_id="t-1", state={"tier": "gold"}, signals={...}, **kwargs)
r = await support.resume("t-1", signals={"approval": True})
r = support.run_sync(...)                         # outside an event loop

r.status        # "completed" | "suspended" | "failed"
r.output, r.state, r.waiting_for, r.wake_at, r.error, r.events
```

### Streaming

Generator steps stream chunks as events; the journal stores only the aggregate.

```python
@step
async def answer(prompt):
    async for tok in llm.stream(prompt):
        yield tok

@flow
async def chat(ctx, prompt):
    return await answer(prompt)

async for ev in chat.stream("hello", run_id="chat-1"):
    if ev["event"] == "chunk":
        await websocket.send(ev["chunk"])
    elif ev["event"] == "stream_reset":            # a retry restarted the stream
        await websocket.send({"reset": True})
    elif ev["event"] == "result":
        final = ev["result"]                       # RunResult
```

### Events

Pass `on_event=fn` (or a list) to `@flow`, `run`, or `resume`. Every event has `event`, `key`, `run_id`, `flow`:

`run_started`, `started`, `completed`, `replayed`, `attempt_failed`, `repaired`, `fallback`, `breaker_open`, `chunk`, `stream_reset`, `step_failed`, `step_crashed`, `signal_timeout`, `suspended`, `continued_as_new`, `divergence`, `failed`, `crashed`, `finished`.

A failing observer never breaks a run.

---

## Journals

```python
from pylinxi import MemoryJournal, FileJournal, SQLiteJournal, open_journal
from pylinxi.postgres import PostgresJournal

j = open_journal("sqlite:///runs.db", secret={"2026-q3": k1, "2026-q4": k2})
```

| Backend | Use for |
|---|---|
| `MemoryJournal()` | Tests. |
| `FileJournal(dir, fsync=True)` | One JSONL file per run; tolerant of torn writes after a crash. |
| `SQLiteJournal(path)` | Single machine (WAL mode). |
| `PostgresJournal(dsn, table="pylinxi_journal")` | Many workers. Appends run under a per-run advisory lock reading the tail from the database; `run`/`resume` hold a session lock so a second worker gets `RunLocked`. Payloads are `TEXT` so signatures verify byte-for-byte. |

Common methods: `load(run_id)`, `verify(run_id, require_signed=False)`, `state_at(run_id, seq=None)`, `runs()`, `runs_where_last(kind)`, `pending_timers(due_before=None)`, `delete(run_id)`, `close()` (also a context manager).

**Signing and rotation.** `secret=b"..."` or `{key_id: key}`. New entries are signed with `active_kid` (default: the last key). Old entries verify as long as their key id is still configured. `verify(..., require_signed=True)` also rejects unkeyed entries, so an attacker can't rewrite a run as an unkeyed chain.

**Integrity limits.** The chain detects edits, reordering and deletions *inside* a run. Dropping the newest entries can only be detected against an external anchor: store `journal.head(run_id)` elsewhere (an audit log, another database) and check with `verify(run_id, anchor=...)`. `verify` never raises; undecodable data makes it return `False`, and `load` raises `JournalCorrupt`.

**Concurrent writers.** Appends read the chain tail from storage under a storage-level lock (file lock, SQLite `BEGIN IMMEDIATE`, Postgres advisory lock), so any number of `Journal` instances or processes can share one store. A crash mid-write leaves a torn final line that the next append repairs.

**Redaction.** State deltas and metadata are redacted after encoding (so tuples, sets and dataclass fields are covered too). Key names are normalized (`apiKey`, `api-key` → `api_key`) and matched by word: `password`, `secret`, `token`, `api_key`, `access_key`, `private_key`, `auth`, `authorization`, `credential(s)`, `ssn`, `cookie`, `session_id` and similar — so `access_token` matches but `max_tokens` doesn't. Override with `redact_pattern=` (or `None`). Step *results* are not redacted (they are needed for replay); mark such steps `sensitive=True`. Validation errors never include the rejected value.

**Types.** The codec is lossless: tuples, sets, bytes, non-string dict keys, and dicts that happen to contain the internal tag key all round-trip exactly. Custom classes must be registered (`default_codec.register(MyDataclass)`, also pydantic models); unregistered ones raise instead of silently coming back as dicts on replay.

---

## OpenTelemetry

```python
from pylinxi.otel import OTelExporter

@flow(journal=j, on_event=OTelExporter())       # record_errors=False to keep error text out of traces
```

```
pylinxi.run support                  status, waiting_for, events: pylinxi.replayed
  ├─ pylinxi.step classify           attempts=3 healed=retry, events: attempt_failed, repaired
  ├─ pylinxi.step lookup_charges     events: breaker_open, fallback
  └─ pylinxi.step send               ERROR status if it finally failed
```

Uses only the OpenTelemetry API; configure any exporter (OTLP, Jaeger, Datadog, Honeycomb) in your app. Replayed steps appear as events, not spans, so traces show what actually executed.

---

## CLI

```bash
export PYLINXI_JOURNAL=postgresql://app@db/agents PYLINXI_KEYS="2026-q3=...,2026-q4=..."

pylinxi runs --status failed            # completed|continued|failed|sleeping|waiting|open
pylinxi show t-1001                     # every entry: kind, key, tries, healed, changed state, result
pylinxi state t-1001 --seq 3            # time travel
pylinxi verify --all --require-signed   # exit 1 on any failure: use in CI or audits
pylinxi timers --due
pylinxi delete t-1001 --yes             # retention / right-to-erasure
pylinxi --json runs                     # machine-readable
```

---

## Honest comparison with LangGraph

LangGraph is mature and also offers a functional API (`@entrypoint`/`@task`), retry policies, caching and many checkpointers, plus LangSmith and a hosted platform. Choose pylinxi when you want:

- a small surface (two decorators, no graph or reducer schema), zero core dependencies, and code you can read in an afternoon;
- per-step validate → repair → retry → fallback → breaker as one policy;
- delta journals with signing, rotation, redaction and a verify CLI built in;
- durable timers, `patched()` and `continue_as_new()` for long-lived agents.

Choose LangGraph when you need its integrations ecosystem, LangSmith's tracing UI, or a managed runtime.

On certifications: SOC 2 / ISO 27001 apply to organizations and hosted services, not libraries. pylinxi makes audits easier — signed journals, redaction, no network calls, no core dependencies to vet.

---

## Development

```bash
git clone https://github.com/karawash-projects/pylinxi && cd pylinxi
pip install -e ".[dev]"
python -m unittest discover -s tests -t .                            # Postgres tests skip without a DSN
PYLINXI_PG_DSN=postgresql://user@localhost/db python -m unittest discover -s tests -t .
ruff check pylinxi tests examples && mypy pylinxi
python examples/support_agent.py
```

CI runs lint, type checks, and the full suite with a Postgres service on Python 3.9–3.13.

### Releasing

Bump `version` in `pyproject.toml` and `__version__` in `pylinxi/__init__.py`, add a `## X.Y.Z` section to `CHANGELOG.md`, then:

```bash
git tag vX.Y.Z && git push origin vX.Y.Z
```

(or on GitHub: **Releases → Draft a new release → Choose a tag → type `vX.Y.Z` → Create new tag → Publish release**.)

The `release` workflow checks the tag matches the version, runs the tests, builds, publishes to PyPI via trusted publishing (no API token), and creates a GitHub Release with the changelog notes and built files.

MIT licensed.
