# Examples

Each example is a single file that runs on its own (`python examples/<name>.py`),
uses a throwaway journal in a temp directory, and checks its own outcome, so
CI runs them all on every push.

| Example | Shows |
|---|---|
| [`llm_fallback_agent.py`](llm_fallback_agent.py) | An LLM step that validates its output, feeds the parse error back into the prompt (`repair`), retries transient provider errors, falls back to a cheaper model, and shares a circuit breaker. Replay makes no model calls. |
| [`checkout_saga.py`](checkout_saga.py) | Reserve → charge → ship with compensation (refund, release) in plain `except` code. The worker is killed right after the charge; the resume does not charge again. `idempotency_key()` for the payment provider. |
| [`approval_workflow.py`](approval_workflow.py) | Human-in-the-loop: wait for a decision with a deadline, escalate on timeout. The decision arrives from another process via `journal.send_signal()`; a `Worker` resumes the run. Nothing is held in memory while waiting. |
| [`scheduled_digest.py`](scheduled_digest.py) | A job that runs "forever": daily work, durable sleeps, and `ctx.continue_as_new()` to keep each journal short. Every day runs exactly once across worker ticks. |
| [`support_agent.py`](support_agent.py) | A support agent that heals malformed LLM output, survives a crash mid-run, and waits for human approval. |
| [`followup_agent.py`](followup_agent.py) | Streaming steps, a durable follow-up timer, and a background `Worker`. |

In production, swap the temp-dir journal for `SQLiteJournal("runs.db")` on one
machine or `PostgresJournal(dsn)` for many workers, and run
`await Worker(journal, [your_flows]).run_forever()` in a long-lived process.
