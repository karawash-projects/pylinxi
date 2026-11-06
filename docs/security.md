# Security & audit

A journal is a record of what an agent decided and did, so pylinxi treats it as
evidence.

## Signing and rotation

```python
journal = SQLiteJournal("runs.db", secret={"2026-q3": old_key, "2026-q4": new_key})
```

- Entries form a hash chain, each signed with HMAC-SHA256. A signature binds the
  run id, the previous entry and the stored bytes, so entries can't be edited,
  reordered, deleted from the middle, or copied between runs undetected.
- New entries use `active_kid` (default: the last key). Old entries verify as long
  as their key id is still configured.
- With keys configured, `load()` refuses unsigned entries (`require_signed=True`
  by default), so nobody can rewrite a run as an unsigned chain.

## Integrity

Every `load()` verifies the run's chain before replaying it; corrupt or forged
runs raise `JournalCorrupt`. Dropping the *newest* entries can only be detected
against an external anchor:

```python
anchor = journal.head(run_id)            # store elsewhere: audit log, another database
journal.verify(run_id, anchor=anchor)    # False if entries after the anchor were removed
```

## Redaction

State deltas and metadata are redacted after encoding. A key counts as secret when
its last word is a credential word (`password`, `secret`, `token`, `api_key`,
`access_key`, `private_key`, `auth`, `authorization`, `credential(s)`, `ssn`,
`cookie(s)`, `bearer`), after normalizing `apiKey`/`api-key` to `api_key`. So
`access_token` and `client_secret` match; `max_tokens` and `session_id` don't.
Override with `redact_pattern=`. Error text is capped and can be filtered with
`error_redactor=`.

## Secrets never enter the journal by accident

Flow inputs, initial state, signals and `continue_as_new` payloads are journaled
in clear (runs must be able to resume), so pylinxi **refuses** them when they
contain secret-looking keys. Pass a reference instead and load the secret in a
sensitive step, whose result is never journaled and is recomputed on resume:

```python
@step(sensitive=True)
def api_token(ref):
    return secret_store.read(ref)
```

## Retention and erasure

```python
journal.delete(run_id)                   # every generation of the run, plus its lock files
```

or `pylinxi delete <run_id> --yes`. Deletion takes the run's lock, so it raises
`RunLocked` while the run executes.

## Audits from the command line

```bash
pylinxi verify --all          # exit code 1 on any failure
pylinxi show t-1001           # every entry: kind, step, attempts, healed, changed state
pylinxi state t-1001 --seq 3  # state as of entry 3
```

## Certifications

SOC 2 and ISO 27001 apply to organizations and hosted services, not libraries.
pylinxi makes audits easier: signed journals, redaction, no network calls of its
own, and no core dependencies to vet.

## Reporting a vulnerability

See [SECURITY.md](https://github.com/karawash-projects/pylinxi/blob/main/SECURITY.md).
