# Changelog

## 1.1.0 — 2026-11-06

Fixes every finding of the 1.0.0 production-readiness review (43 findings, all reproduced) plus several related bugs found while verifying it. Each has a regression test in `tests/test_review_1_1.py`. See "Upgrading from 1.0" in the README for intended behaviour changes.

### Integrity and security
- **Every `load()` verifies the run's chain** (run id, order, links, signatures) before anything replays; forged, edited or copied rows raise `JournalCorrupt`. With keys configured, unsigned entries are refused (`require_signed`). (#4, #5)
- **Schema v2 entries** bind the run id into the signature and are verified against the stored bytes, so `verify` needs no registered classes and survives codec changes; entries from newer versions with extra fields still load. v1 (1.0) journals stay readable. (#21, #23)
- `verify` returns `False` for a run that doesn't exist; the CLI reports FAIL. (#22)
- `delete()` takes the run's lock, removes every continue-as-new generation and the lock files, and deleted runs can't be recreated by a late write. CLI `delete` does the same (`--only` for one generation). (#10, #24, #32)
- Narrower secret filter: a key is secret when its *last word* is a credential word (`access_token`, `client_secret`, `apiKey`), so `token_budget`, `auth_required`, `pass_count`, `session_id` are allowed. `continue_as_new` checks at the call. (#25)
- CLI: `verify` requires signatures when keys are given (`--allow-unsigned` to migrate); keys from `--key-file` / `$PYLINXI_KEYS_FILE`; `hex:` / `base64:` key encodings; `--key` warns. Error text in journals is capped at 500 characters, with an optional `error_redactor`. (#26)

### Replay correctness
- A failure that ends the run is retried on resume even if a `finally` / `async with` ran steps on the way out; genuinely caught failures still replay as caught. (#1)
- **Strict replay**: code that would run a different step where the journal recorded one stops with `ReplayDivergence` before running anything (`strict=False` to only report it). (#20)
- Unknown run ids raise `UnknownRun` on `resume()`; run ids of another flow raise `FlowMismatch`; different inputs for an existing run raise `RunExists`; new `@flow(name=...)` for a stable identity. (#6, #7, M2)
- A step whose result can't be journaled fails with `ResultNotJournaled` and is never re-run (no double charge); unjournalable state becomes a placeholder instead of losing the step. Built-in support for `datetime`, `date`, `time`, `timedelta`, `Decimal`, `UUID` and registered Enums; registry keyed by module + name. (#8, #14, #15)
- A run that failed earlier no longer reports false divergence, sticks `ctx.replaying`, or breaks `ctx.patched()`. (#12)
- When one branch suspends, the others run on (bounded by `drain_timeout`) and the run wakes at the earliest deadline among them; `gather(wait_for(...), sleep(...))` now schedules its timer. (#13)
- Continue-as-new persists leftover signals and the next generation's inputs in the hand-off, keeps signals when resuming by root id, and finds the newest generation in one read instead of walking the chain (no hop limit). (#16, #17)
- The task factory is re-checked on every run; `asyncio.wait_for` gives the same keys on Python < 3.12 as on 3.12+; a flow started inside a step is its own durable run. (#18, #19, M9)

### Self-healing policies
- Every error — retryable or not, from the step, its validator or its repair hook — goes through the breaker and the fallback and surfaces as `StepFailed`. (#2, M4)
- A cancelled or failed half-open breaker trial can't leave the breaker stuck. (#3)
- A timed-out sync attempt is never overlapped: pylinxi waits for its thread, uses its result if it finished, and otherwise stops retrying (`AttemptStillRunning`). Sync steps run on a dedicated thread pool. (#31)
- `attempts` counts attempts that actually ran, and the real error is kept when the breaker opens mid-retry. (M5)

### Operations
- Journal writes and lock acquisition run off the event loop. (#27)
- Postgres: runs are fenced (a worker whose lock connection drops gets `LockLost` and can't keep writing); run locks are multiplexed over `lock_connections` sessions (default 4) so connections stay bounded; the shared connection reconnects. (#11, #28)
- SQLite and Postgres keep a heads table, so Worker ticks and timer queries don't scan history. (#29)
- Worker: resumes with bounded `concurrency`; picks up runs that received a signal; recovers runs left open by a crash (`resume_stale_after`, default 60 s); counts consecutive failures since the last progress; logs tick errors, calls `on_error`, backs off. (#29, #30)
- No unbounded growth: in-process lock registry, finished branch tasks, lock files. OpenTelemetry spans are never left open by rejected signals. (#32)
- Durable signals: `journal.send_signal()` / `flow.signal()` work while a run is executing; signals to a completed run are reported (`dropped_signals`, event, warning). (M3)
- `ctx.map(concurrency<1)` raises instead of hanging; FileJournal accepts any run id and fsyncs new directories and files; the CLI refuses mistyped journal paths and ambiguous `sqlite://` URLs and reports corrupt runs without a traceback; `RunResult.from_journal` says where `state` came from. (M1, M6, M7, M8)

### Found by an independent re-review of these fixes (also fixed, with tests on every journal backend)
- Journal writes go through a per-run ordered writer that never touches the event loop's task factory: on file, SQLite and Postgres journals, journaling could previously shift branch keys (re-running steps or returning another branch's result), let a cancelled branch's write land after the run's outcome, or reorder state deltas.
- After a failure that ended the run, entries recorded on the way out (`finally`, `__aexit__`, `except: ...; raise`) no longer bind replay: a retry that succeeds can take the success path (no endless `ReplayDivergence`). Only steps allocated *while that failure was being handled* are affected (each entry records the failures `sys.exc_info()` held when its key was allocated): siblings that finished after the failure and steps the flow ran while a branch's failure was pending replay normally, and a cleanup reached again from the same line of code replays instead of running twice.
- When a run ends (failure, completion, continue-as-new) while branches still run, steps already executing finish and are journaled (bounded by `drain_timeout`) before the branches are cancelled, and no new step starts: their side effects are never repeated on resume.
- Durable waits inside an `asyncio.TaskGroup` suspend the run instead of crashing it: a branch about to suspend first lets its siblings reach their own durable waits (or finish the step they are in, bounded by `drain_timeout`), so their deadlines are recorded before the TaskGroup cancels them; the Worker survives exception groups.
- Journal writes are chained on completion: no writer thread waits on another write, so a run with a slow store can't stall (or, past 32 queued writes, deadlock) other runs' writes. A failed write fails every later write of that run instead of letting them land out of order.
- A resume that finds an earlier execution's attempt of a step still running suspends briefly (`PriorAttemptRunning`, re-checked every 5 s) and then uses that attempt's result, instead of failing the step with a catchable, permanently journaled `AttemptStillRunning`.
- FileJournal lock files are named after the run id rather than its storage file, so every instance and process agrees on them; the path cache re-resolves files another instance deleted or re-created and is bounded.
- A signal picked up by a sibling branch while the run was suspending re-executes the run instead of stranding it.
- Only explicit causes (`raise ... from e`) make an earlier, handled failure count as unhandled; a new failure inside an `except` block no longer re-runs the step that block handled.
- The Worker recovers a run that crashed between a step's failure and the run's failure, and gives up correctly when a sensitive step re-runs each attempt.
- Cancelling a run while its lock is being acquired can't leak the lock; FileJournal never mistakes another run's file for its own on case-insensitive filesystems; lock files are re-checked after a concurrent delete; Postgres replaces a lock connection that died while idle.
- A sync step whose thread outlives its caller (timeout, or a branch cancelled while draining) is remembered per run and step for an hour: the next attempt, resume or Worker retry in that process waits for it and reuses its result instead of running the side effect again.
- Names containing `/` keep divergence detection (the scope is stored in each entry).

### Known limitations
- State snapshots cost O(state size) per checkpoint (#33); keep large data out of `ctx.state`.
- A step body already running when its worker loses ownership can't be stopped; use `idempotency_key()` with external APIs.
- pylinxi 1.0 can't read entries written by 1.1: upgrade all workers sharing a journal together.

## 1.0.0 — 2026-10-21

### Added
- **Streaming steps**: sync/async generator steps emit `chunk` events and journal an aggregate (`Policy.aggregate`); retries emit `stream_reset`. `Flow.stream()` yields events live, ending with the `RunResult`.
- **Durable timers**: `ctx.sleep(seconds)` (inline when short, suspends when long) and `ctx.wait_for(signal, timeout=, default=)`. `Journal.pending_timers()` and `RunResult.wake_at` for schedulers.
- **Worker**: resumes due timers and, optionally, failed runs with exponential backoff and a give-up limit. Safe to run many against Postgres.
- **Code evolution**: `ctx.patched(change_id)` lets in-flight runs keep old behaviour after a deploy.
- **Long-lived loops**: `ctx.continue_as_new(...)` rolls over to a fresh journal (`run.g1`, `run.g2`, …); resuming the root id follows the chain.
- **Determinism helpers**: `ctx.now()`, `ctx.uuid()`.
- **Idempotency**: `pylinxi.idempotency_key()` inside a step — stable across retries and resumes, for payment/email/ticketing APIs.
- **Key rotation**: `secret={"k1": ..., "k2": ...}`; entries carry a key id; `verify(require_signed=True)` rejects unkeyed rewrites.
- **Retention**: `Journal.delete(run_id)` on every backend; CLI `pylinxi delete`.
- **CLI**: `pylinxi runs | show | state | verify | timers | delete`, `--json`, `$PYLINXI_JOURNAL`, `$PYLINXI_KEYS`.
- `open_journal(url)` for `memory://`, `file://`, `sqlite:///`, `postgresql://`.
- `Journal.close()` and context-manager support; `py.typed`.

### Fixed (independent review: 27 reproduced issues, each now a regression test)
- **Replay correctness:** steps inside concurrent branches are keyed per branch (deterministic regardless of finishing order); branches still running are cancelled when a run suspends/fails/finishes; caught step failures replay as the same failure instead of re-running; nested step calls no longer shift keys; `patched()` counts sensitive and failed history.
- **Journal integrity:** appends read the tail from storage under storage-level locks (multiple instances/processes per store); torn final lines are repaired instead of hiding later entries; mid-file corruption raises `JournalCorrupt`; `verify` never raises; `head()` + `verify(anchor=)` detect truncation.
- **Run ownership:** in-process and cross-process (file/SQLite) run locks; Postgres execution and append locks use separate 64-bit namespaces.
- **Security:** flow inputs/state/signals/continue payloads with secret-looking keys are rejected instead of journaled in clear; redaction covers tuples and dataclass fields; validation errors never contain the value; redaction matches whole key words (no `max_tokens` false positives).
- **Codec:** lossless for non-string keys and tag-like dicts; unregistered classes raise; malformed payloads raise `JournalCorrupt`.
- **Signals and timers:** signals are journaled on delivery and queue until consumed; suspensions are journaled, so timer status is exact; continue-as-new persists its hand-off (crash-safe), keeps signals, and can't loop on ids ending in `.gN`.
- **Policies:** stream chunks from abandoned attempts are dropped and abandoned generator threads stop; streaming fallbacks get a `stream_reset`; a raising fallback is a step failure; circuit breaker allows exactly one half-open trial and ignores late failures.
- **Worker:** only retries runs that ended in an unhandled failure; per-run errors are reported in `TickReport.errors` instead of aborting the tick.
- **OTel:** continue-as-new generations end their run spans.
- `Suspended`/`ContinueAsNew` now derive from `BaseException`.
- **SQLite:** several processes opening the same new database at once no longer fail with "database is locked" during setup (found by CI).

### Changed
- Plain sync steps now run in a worker thread by default (`Policy(thread=False)` to opt out), so parallel branches and timeouts work with blocking SDKs.
- Failures raised by flow code (not just steps) are journaled, so they show as `failed` in the CLI and recovery queries.
- Start entries record the flow name (used by `Worker`).

## 0.2.0
- `PostgresJournal` (advisory-locked appends, per-run execution lock, `runs(kind=)`).
- `OTelExporter` (run/step spans, healing events, replay events, PII-safe mode).
- Multiple observers per flow; failing observers can't break runs.

## 0.1.0
- Durable flows as plain Python, healing policies (retry/timeout/validate/repair/fallback/breaker), delta journal with hash chain and redaction, memory/file/SQLite backends.
