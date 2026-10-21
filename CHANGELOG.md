# Changelog

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
