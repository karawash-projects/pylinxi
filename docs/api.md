# API reference

Everything below is importable from `pylinxi` directly (Postgres and OpenTelemetry
from `pylinxi.postgres` and `pylinxi.otel`).

## Flows and steps

::: pylinxi.flow.flow
::: pylinxi.flow.step
::: pylinxi.flow.idempotency_key
::: pylinxi.flow.Flow
::: pylinxi.flow.Context
::: pylinxi.flow.RunResult

## Policies

::: pylinxi.policy.Policy
::: pylinxi.policy.Retry
::: pylinxi.policy.CircuitBreaker

## Journals

::: pylinxi.journal.Journal
::: pylinxi.journal.MemoryJournal
::: pylinxi.journal.FileJournal
::: pylinxi.journal.SQLiteJournal
::: pylinxi.postgres.PostgresJournal
::: pylinxi.journal.open_journal
::: pylinxi.journal.Entry
::: pylinxi.journal.Codec

## Worker

::: pylinxi.worker.Worker
::: pylinxi.worker.TickReport

## Exceptions

::: pylinxi.policy.StepFailed
::: pylinxi.policy.ValidationError
::: pylinxi.policy.CircuitOpen
::: pylinxi.policy.AttemptStillRunning
::: pylinxi.policy.PriorAttemptRunning
::: pylinxi.flow.Suspended
::: pylinxi.flow.ContinueAsNew
::: pylinxi.flow.ReplayDivergence
::: pylinxi.flow.ReplayedError
::: pylinxi.flow.ResultNotJournaled
::: pylinxi.flow.FlowMismatch
::: pylinxi.flow.RunExists
::: pylinxi.journal.RunLocked
::: pylinxi.journal.LockLost
::: pylinxi.journal.JournalCorrupt
::: pylinxi.journal.UnknownRun
