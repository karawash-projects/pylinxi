# Flow API

| Call | What it does |
|---|---|
| `await my_step(...)` | Journaled call of a `@step` function. |
| `await ctx.step(fn, *args, name=, policy=, sensitive=)` | Journaled call of any callable (lambdas, third-party functions). |
| `await ctx.map(fn, items, concurrency=8)` | Parallel fan-out; each item is its own checkpoint. Siblings finish before an error surfaces, so a resume re-runs only failed items. |
| `await ctx.route(label, {"a": step_a, ...}, *args, default=)` | Dispatch by label. |
| `await ctx.wait_for(signal, timeout=None, default=None)` | Wait for a signal from `flow.signal(run_id, name, value)`, `journal.send_signal(...)` or `resume(..., signals=...)`. Signals are journaled on delivery — also while the run is executing — so early ones aren't lost and repeats queue in order. With `timeout`, returns `default` once it expires. |
| `await ctx.sleep(seconds, inline_max=1.0)` | Durable sleep. Short waits sleep in-process; longer ones suspend and free the worker. |
| `await ctx.now()` / `await ctx.uuid()` | Clock / random id recorded once, identical on replay. |
| `ctx.patched("change-id")` | `True` for runs that reach this point on new code, `False` for runs already past it. |
| `ctx.continue_as_new(*args, **kwargs)` | End this run; continue as `run_id.g1`, `.g2`… with the current state. |
| `ctx.state` | Dict of flow state. Changes are journaled as deltas at the next checkpoint. |
| `ctx.run_id`, `ctx.generation`, `ctx.replaying` | Run metadata. |
| `pylinxi.idempotency_key()` | Inside a step: `"<run_id>:<step_key>"`, stable across retries and resumes. |

`@step(sensitive=True)` keeps a step's result out of the journal entirely; it is recomputed on resume. Use it for tokens and credentials.

## Running

```python
r = await support.run(*args, run_id="t-1", state={"tier": "gold"}, **kwargs)
r = await support.resume("t-1")                    # UnknownRun if it doesn't exist
await support.signal("t-1", "approval", True)      # durable; safe while it's running elsewhere
r = support.run_sync(...)                          # outside an event loop

r.status        # "completed" | "suspended" | "failed"
r.output, r.state, r.waiting_for, r.waiting_for_all, r.wake_at, r.error, r.events
r.dropped_signals   # signals sent to a run that had already completed
r.from_journal      # True when the run had completed earlier and state was rebuilt from the journal
```

## Streaming

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

## Events

Pass `on_event=fn` (or a list) to `@flow`, `run`, or `resume`. Every event has `event`, `key`, `run_id`, `flow`:

`run_started`, `started`, `completed`, `replayed`, `attempt_failed`, `repaired`, `fallback`, `breaker_open`, `late_result`, `chunk`, `stream_reset`, `step_failed`, `step_crashed`, `signal_timeout`, `signals_dropped`, `state_unserializable`, `suspended`, `continued_as_new`, `divergence`, `failed`, `crashed`, `finished`.

A failing observer never breaks a run.
