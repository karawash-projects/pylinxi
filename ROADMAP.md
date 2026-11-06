# Roadmap

This is a statement of direction, not a promise of dates. Priorities follow what
users hit in practice — if something here matters to you (or something missing
does), open an issue and say what you're building.

pylinxi's promise stays the same in every release: **a finished step never runs
twice, and a run survives anything that can kill a process.** Features that would
weaken that don't ship.

## Next: 1.2

**Cheaper checkpoints for large state.** Each checkpoint already *stores* only the
state keys that changed, but finding them serializes all of `ctx.state` (O(state
size) per step). 1.2 will track writes to `ctx.state` (and nested containers) so a
checkpoint costs O(changed keys), keeping 100 MB conversation histories cheap.

**Determinism checks in tests.** A `pylinxi.testing` helper that replays every
journaled run of a flow against the current code and reports
`ReplayDivergence` before you deploy — `pytest`-friendly, usable in CI against a
copy of production journals.

**Run inspection from code.** `Flow.describe(run_id)` returning the run's status,
pending waits, step timings and healing history as data, so dashboards and
support tools don't have to parse entries.

**Cancellation as a first-class operation.** `flow.cancel(run_id, reason)` that a
running or suspended run observes at its next step or wait, with `try/finally`
compensation running exactly once.

## Later

**LangChain / LangGraph interop.** Use any LangChain runnable, tool or chat model
as a pylinxi step (with policies and journaling), and wrap a compiled LangGraph
graph as a single durable step — so teams can adopt pylinxi without rewriting
what already works.

**Dashboard.** A small, self-hosted read-only web UI over a journal: runs by
status, a timeline per run (steps, retries, repairs, fallbacks, waits), state at
any point, and signature verification — the CLI's `runs`/`show`/`state`/`verify`,
visual.

**Schedules.** Cron-style `@flow(schedule="0 9 * * MON-FRI")` driven by the
Worker, built on durable timers and `continue_as_new`, with exactly-once firing
per slot across many workers.

**More journals.** Redis and DynamoDB backends, if there is demand, with the
same guarantees (ordered appends, fencing, signed chains).

**Per-tenant keys.** A key resolver (`secret=lambda run_id: ...`) so multi-tenant
deployments can sign and rotate each tenant's runs with their own key from a KMS.

## Not planned

- A graph DSL. Flows are Python; that's the point.
- Bundled LLM clients or prompt templates. Steps call whatever client you use.
- A hosted service. pylinxi is a library you run in your own infrastructure.

## Known limitations today

- Checkpoint cost is O(state size) (see 1.2 above); keep large blobs out of
  `ctx.state` for now.
- A step body that is already running when its worker loses ownership can't be
  stopped; make external side effects idempotent with `idempotency_key()`.
- Waiting on a still-running earlier attempt of a sync step (after a timeout or
  drain) only works within the process where that attempt runs.
