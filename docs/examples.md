# Examples

Each example is a single file in
[`examples/`](https://github.com/karawash-projects/pylinxi/tree/main/examples)
that runs on its own and checks its own outcome (CI runs them all).

```bash
git clone https://github.com/karawash-projects/pylinxi && cd pylinxi
python examples/checkout_saga.py
```

| Example | Shows |
|---|---|
| `llm_fallback_agent.py` | Output validation, a `repair` hook that feeds the parse error back into the prompt, retries, fallback to a cheaper model, a shared circuit breaker. Replay makes no model calls. |
| `checkout_saga.py` | Reserve → charge → ship with compensation in plain `except` code. The worker is killed right after charging; the resume doesn't charge again. |
| `approval_workflow.py` | Human approval with a deadline and escalation; the decision arrives from another process via `journal.send_signal()`. |
| `scheduled_digest.py` | A job that runs indefinitely: daily work, durable sleeps, `continue_as_new` to keep journals short. |
| `support_agent.py` | A support agent that heals malformed LLM output, survives a crash, and waits for approval. |
| `followup_agent.py` | Streaming steps, a durable follow-up timer, and a background `Worker`. |
