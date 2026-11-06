# Going to production

## Pick a journal

| Deployment | Journal |
|---|---|
| Tests | `MemoryJournal()` |
| One machine, one or more processes | `SQLiteJournal("runs.db")` |
| Many workers | `PostgresJournal(dsn)` (`pip install "pylinxi[postgres]"`) |

Always pass `secret=` (ideally `{key_id: key}` from your secret manager) so
journals are signed. See [Security & audit](security.md).

## Run workers

```python
from pylinxi import Worker
from pylinxi.postgres import PostgresJournal

journal = PostgresJournal("postgresql://app@db:5432/agents", secret=KEYS)
worker = Worker(journal, [support, onboarding], retry_failed=True, max_failures=5,
                concurrency=8, on_error=alert)
await worker.run_forever(interval=5)
```

Run as many worker processes as you like against one Postgres database:

- a run executes on exactly one worker at a time (others get `RunLocked` and skip it);
- runs are **fenced**: if a worker's lock connection drops it gets `LockLost` on
  its next write instead of writing alongside the new owner;
- run locks share `lock_connections` (default 4) sessions per journal, so
  concurrency doesn't grow the connection count;
- the Worker resumes due timers, delivered signals, runs left open by a crashed
  worker (`resume_stale_after`, default 60 s), and — with `retry_failed` — runs
  that ended in an unhandled failure, with backoff.

!!! warning
    Session advisory locks need a real session: don't put a transaction-pooling
    proxy (e.g. pgbouncer in transaction mode) in front of pylinxi.

## Make side effects idempotent

pylinxi guarantees a finished step never runs again. A step that was
**mid-flight** when its worker died has to run again, because its result was
never recorded. Pass `idempotency_key()` to external APIs so they dedupe that
case:

```python
@step
def charge(order):
    return stripe.PaymentIntent.create(amount=order.cents, idempotency_key=idempotency_key())
```

## Trace

```python
from pylinxi.otel import OTelExporter

@flow(journal=j, on_event=OTelExporter())       # record_errors=False to keep error text out of traces
```

```text
pylinxi.run support                  status, waiting_for, events: pylinxi.replayed
  ├─ pylinxi.step classify           attempts=3 healed=retry, events: attempt_failed, repaired
  ├─ pylinxi.step lookup_charges     events: breaker_open, fallback
  └─ pylinxi.step send               ERROR status if it finally failed
```

Uses only the OpenTelemetry API; configure any exporter (OTLP, Jaeger, Datadog, Honeycomb) in your app. Replayed steps appear as events, not spans, so traces show what actually executed.

## Deploy while runs are in flight

Suspended runs replay your *new* code. Changes that alter which steps run where a
run has already been are caught: the run stops with `ReplayDivergence` before
running anything new. Make such changes with `ctx.patched()`:

```python
if ctx.patched("fraud-check-v2"):
    await fraud_check(order)      # runs that reach this point on the new code
# runs already past this point keep their old behaviour on replay
```

Give flows a stable identity with `@flow(name="checkout")` so renaming the
function doesn't orphan runs.

## Checklist

- [ ] Signed journal (`secret=`), keys from a secret manager, retired keys kept while their runs exist.
- [ ] Secrets passed by reference and loaded in `@step(sensitive=True)`.
- [ ] `idempotency_key()` on every external side effect.
- [ ] Workers running `run_forever`, with `on_error` wired to alerting.
- [ ] `pylinxi verify --all` in a scheduled audit job.
- [ ] Retention: `pylinxi delete <run> --yes` or `journal.delete()` for expired runs.
- [ ] All workers sharing a journal upgraded together.
