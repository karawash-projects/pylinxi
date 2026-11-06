# Self-healing

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
| `timeout` | Seconds per attempt. Async steps are cancelled. A sync step's thread can't be killed, so before retrying pylinxi waits up to `timeout` more for it: if it finished, its result is used; if not, retrying stops with `AttemptStillRunning` rather than run the step twice at once. |
| `validate` | `fn(result) -> bool` (may be async). Validation failures are always retried. |
| `repair` | `fn(error, args, kwargs, attempt) -> (args, kwargs) | None` (may be async). Runs between attempts. |
| `fallback` | `fn(*args, **kwargs)` used when attempts are exhausted, an error isn't retryable, the repair hook fails, or the breaker is open. |
| `breaker` | `CircuitBreaker(threshold, reset_after)`; closed → open → half-open (exactly one trial call; a cancelled trial gives its slot back). Every failure counts, retryable or not. |
| `aggregate` | Streaming steps: `fn(chunks) -> result`. Default joins strings, else keeps the list. |
| `thread` | Run plain sync steps in a worker thread (default `True`). |
| `executor` | Thread pool for sync steps. Default: a dedicated pylinxi pool, so slow steps never starve asyncio's default executor. |

Errors outside `Retry.on` aren't retried, but still go to the fallback and the breaker, and still surface as `StepFailed`.

Each completed step's journal entry records `attempts` (how many actually ran), `healed` (`"retry"` / `"fallback"`) and the errors seen.

## Fleet-level healing: `Worker`

```python
from pylinxi import Worker

worker = Worker(journal, [support, onboarding], retry_failed=True, max_failures=5, backoff=30,
                concurrency=8, on_error=alert)
await worker.run_forever(interval=5)          # or: report = await worker.tick()
```

Each tick resumes, up to `concurrency` at a time:
- runs whose timers are due;
- runs that received a signal while suspended;
- runs left `open` by a crashed process (last entry older than `resume_stale_after`, default 60 s — a run still executing elsewhere holds its lock and is skipped);
- with `retry_failed`, runs that *ended* in an unhandled failure, with exponential backoff, giving up after `max_failures` consecutive failures without progress. Failures your flow caught are left alone.

One run that errors while resuming is reported in `TickReport.errors` and never blocks the others; tick errors are logged to the `pylinxi` logger, passed to `on_error`, and back off. Run several workers against a `PostgresJournal`; per-run locks make them skip runs another worker owns.
