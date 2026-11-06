# Upgrading from 1.0

1.1 reads and verifies journals written by 1.0, but changes some behaviour on purpose:

- `resume()` of an unknown run raises `UnknownRun`; reusing a run id with another flow raises `FlowMismatch`, or with different inputs `RunExists`. If you renamed a flow function, pass `@flow(name="old_name")`.
- Flows are strict: replay that no longer matches the journal stops with `ReplayDivergence` before running anything new. Use `ctx.patched()`, or `strict=False` to restore 1.0 behaviour.
- With keys configured, `load()` refuses unsigned entries. If you added keys to an existing unsigned journal, pass `require_signed=False` until those runs are gone.
- `Journal.append()` requires a run's first entry to be `start`; `delete()` removes the whole continue-as-new chain and raises `RunLocked` while the run executes.
- Unregistered Enums now raise instead of coming back as plain values: register them.
- The secret filter is narrower (`session_id`, `token_budget` etc. are allowed now).
- Upgrade every worker that shares a journal at once: 1.0 can't read entries written by 1.1.
