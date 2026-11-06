# Durable waits & signals

A waiting run holds no process, thread or connection: it is suspended in the
journal and resumed when its deadline passes or its signal arrives — even if
every process restarted in between.

## Sleep

```python
await ctx.sleep(3600)                  # suspends; a Worker resumes it in an hour
await ctx.sleep(0.5)                   # short waits (<= inline_max, default 1 s) sleep in-process
```

## Signals

```python
decision = await ctx.wait_for("decision")                              # wait indefinitely
decision = await ctx.wait_for("decision", timeout=3600, default=None)  # with a deadline
```

Deliver a signal from anywhere:

```python
journal.send_signal(run_id, "decision", "approved")       # just record it; a Worker resumes the run
await my_flow.signal(run_id, "decision", "approved")      # record it and resume now if nobody else is
await my_flow.resume(run_id, signals={"decision": "approved"})
```

Signals are journaled on delivery, so:

- one sent **before** the flow reaches its `wait_for` is not lost;
- one sent **while the run is executing** elsewhere is picked up, not dropped;
- repeated deliveries queue in order;
- signals sent to a run that already completed are reported in `RunResult.dropped_signals`.

## Escalation pattern

```python
decision = await ctx.wait_for("decision", timeout=MANAGER_SLA, default=None)
if decision is None:
    await notify("finance", "escalated")
    decision = await ctx.wait_for("decision", timeout=FINANCE_SLA, default="rejected")
```

See the full [approval workflow example](examples.md).

## Concurrent waits

Waits in parallel branches (`asyncio.gather`, `create_task`, `TaskGroup`) are
merged: the run records every signal any branch waits for and wakes at the
earliest deadline among them.

## Running workers

```python
from pylinxi import Worker
await Worker(journal, [my_flow]).run_forever(interval=5)
```

Each tick resumes runs whose timers are due and runs that received a signal.
See [Self-healing → Worker](self-healing.md#fleet-level-healing-worker).

## Long-lived loops

A flow that runs for months (a chat session, a monitor, a daily job) would
replay an ever-growing journal. Roll it over periodically:

```python
ctx.continue_as_new(next_cursor=cursor)   # ends this run; continues as run_id.g1, .g2, ...
```

State carries over; resuming the original run id follows the chain.
