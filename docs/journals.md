# Journals

```python
from pylinxi import MemoryJournal, FileJournal, SQLiteJournal, open_journal
from pylinxi.postgres import PostgresJournal

j = open_journal("sqlite:///runs.db", secret={"2026-q3": k1, "2026-q4": k2})
```

| Backend | Use for |
|---|---|
| `MemoryJournal()` | Tests. |
| `FileJournal(dir, fsync=True)` | One JSONL file per run; any run id (unusual or mixed-case ids get a hashed file name, so case-insensitive filesystems never mix two runs); tolerant of torn writes after a crash. |
| `SQLiteJournal(path)` | Single machine, several processes (WAL mode). |
| `PostgresJournal(dsn, table="pylinxi_journal", lock_connections=4)` | Many workers. A second worker gets `RunLocked`. Runs are **fenced**: a run's entries are written through the connection that holds its lock, so a worker whose lock connection drops gets `LockLost` instead of writing alongside the new owner. Run locks are multiplexed over `lock_connections` sessions, so concurrency doesn't grow the connection count; the shared connection reconnects after a drop. Don't put a transaction-pooling proxy (pgbouncer in transaction mode) in front of it. |

Common methods: `load(run_id)`, `verify(run_id, require_signed=False, anchor=None)`, `state_at(run_id, seq=None)`, `runs()`, `heads(kinds)`, `runs_where_last(kind)`, `pending_timers(due_before=None)`, `send_signal(run_id, name, value)`, `delete(run_id)`, `close()` (also a context manager). SQLite and Postgres keep a heads table, so scheduling queries don't scan history.

**Signing and rotation.** `secret=b"..."` or `{key_id: key}`. New entries are signed with `active_kid` (default: the last key); each signature binds the run id, the previous entry and the stored bytes. Old entries verify as long as their key id is still configured, so keep retired keys for as long as their runs may be loaded. With keys configured, `load()` refuses unsigned entries (`require_signed`, default on; set `False` while migrating a journal written before you added keys), and `verify(..., require_signed=True)` does the same for audits.

**Integrity.** Every `load()` checks the chain before the entries are used (`verify_on_load`, default on). It detects edits, reordering, deletions inside a run, and rows copied from another run. Dropping the newest entries can only be detected against an external anchor: store `journal.head(run_id)` elsewhere (an audit log, another database) and check with `verify(run_id, anchor=...)`. `verify` checks the stored bytes, so it needs none of your registered classes; it never raises, and returns `False` for a run that doesn't exist.

**Concurrent writers.** Appends read the chain tail from storage under a storage-level lock (file lock, SQLite `BEGIN IMMEDIATE`, Postgres advisory lock), so any number of `Journal` instances or processes can share one store. A run's first entry must be its start entry, so a deleted run can't be recreated by a late write. A crash mid-write leaves a torn final line that the next append cuts off; a complete final line that only lost its newline is kept.

**Deletion.** `delete(run_id)` takes the run's lock (raising `RunLocked` while it executes) and removes every generation of its continue-as-new chain plus its lock files; `delete(run_id, chain=False)` removes just one.

**Redaction.** State deltas and metadata are redacted after encoding (so tuples, sets and dataclass fields are covered too). Key names are normalized (`apiKey`, `api-key` → `api_key`) and a key counts as secret when its **last word** is a credential word: `password`, `secret`, `token`, `api_key`, `access_key`, `private_key`, `auth`, `authorization`, `credential(s)`, `ssn`, `cookie(s)`, `bearer`. So `access_token`, `client_secret` and `apiKey` match, while `token_budget`, `max_tokens`, `auth_required`, `pass_count` and `session_id` don't. Override with `redact_pattern=` (or `None`). Step *results* are not redacted (they are needed for replay); mark such steps `sensitive=True`. Error text is capped at 500 characters and can be filtered with `error_redactor=`; validation errors never include the rejected value.

**Types.** The codec is lossless: tuples, sets, bytes, `datetime`, `date`, `time`, `timedelta`, `Decimal`, `UUID`, non-string dict keys, and dicts that happen to contain the internal tag key all round-trip exactly. Custom classes and Enums must be registered (`default_codec.register(MyDataclass)`, also pydantic models); they're identified by module and name, so two `Config` classes in different modules never collide. Unregistered ones raise instead of silently coming back as something else on replay.
