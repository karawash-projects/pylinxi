"""Postgres journal for multi-worker deployments.  pip install "pylinxi[postgres]"

Guarantees beyond the single-process journals:
- Every append runs in a transaction holding a per-run advisory lock and reads the
  chain tail from the database, so the hash chain stays linear with many writers.
- `lock(run_id)` takes a session advisory lock for the whole execution; a second
  worker trying to run/resume the same run gets RunLocked instead of duplicating
  side effects. The lock is released automatically if the worker dies.
- Fencing: while a run executes, its entries are written through the very
  connection that holds its lock. If that connection drops (failover, idle
  timeout, network blip) the lock is gone -- and so is this worker's ability to
  write: the next write raises LockLost, so a worker that lost ownership can't
  keep writing alongside the new owner.
- Connections are bounded: run locks are multiplexed over a small, fixed set of
  lock connections (`lock_connections`, default 4) -- a session can hold many
  advisory locks -- so concurrency doesn't grow the connection count. Total
  connections per journal = lock_connections + 1. The shared connection
  reconnects automatically after a drop.
- A heads table indexes each run's newest entry, so Worker ticks and timer queries
  don't scan history.
- Payloads are stored as TEXT (not JSONB) so signatures verify byte-for-byte.

Session advisory locks need a real session: don't put a transaction-pooling proxy
(e.g. pgbouncer in transaction mode) between pylinxi and Postgres.
"""
from __future__ import annotations

import contextlib
import json
import logging
import threading
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, TypeVar

from .journal import Entry, Head, Journal, LockLost, RunLocked

try:
    import psycopg
except ImportError as _e:  # pragma: no cover
    raise ImportError('PostgresJournal needs psycopg: pip install "psycopg[binary]"') from _e

log = logging.getLogger("pylinxi")
T = TypeVar("T")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS {t} (
    run_id     TEXT        NOT NULL,
    seq        INTEGER     NOT NULL,
    kind       TEXT        NOT NULL,
    key        TEXT        NOT NULL,
    payload    TEXT        NOT NULL,
    sig        TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS {t}_kind_idx ON {t} (kind, created_at);
CREATE TABLE IF NOT EXISTS {t}_heads (
    run_id  TEXT PRIMARY KEY,
    seq     INTEGER NOT NULL,
    kind    TEXT NOT NULL,
    key     TEXT NOT NULL,
    ts      DOUBLE PRECISION NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS {t}_heads_kind_idx ON {t}_heads (kind);
"""

_EXEC_SEED, _APPEND_SEED = 0x4C49_4E58_0001, 0x4C49_4E58_0002   # separate lock namespaces


class _LockConn:
    __slots__ = ("conn", "lock", "runs")

    def __init__(self, conn: Any):
        self.conn, self.lock = conn, threading.Lock()
        self.runs: Set[str] = set()

    @property
    def healthy(self) -> bool:
        return not self.conn.closed and not getattr(self.conn, "broken", False)


class PostgresJournal(Journal):
    def __init__(self, conninfo: str, table: str = "pylinxi_journal", lock_connections: int = 4, **kw: Any):
        super().__init__(**kw)
        if not table.replace("_", "").isalnum():
            raise ValueError("table name must be alphanumeric/underscore")
        if lock_connections < 1:
            raise ValueError("lock_connections must be >= 1")
        self.conninfo, self.table = conninfo, table
        self.lock_connections = lock_connections
        self._conn = psycopg.connect(conninfo, autocommit=True)
        self._conn_lock = threading.Lock()
        self._lconns: List[_LockConn] = []
        self._lconns_lock = threading.Lock()
        self._owners: Dict[str, _LockConn] = {}           # run_id -> the session holding its lock
        self._shared(lambda c: c.execute(_SCHEMA.format(t=table)))
        self._backfill_heads()

    def _lock_ns(self) -> str:
        return f"pg:{self.conninfo}:{self.table}"

    # -- connections
    def _shared(self, fn: Callable[[Any], T]) -> T:
        """Run fn on the shared connection, reconnecting once if it dropped."""
        with self._conn_lock:
            try:
                return fn(self._conn)
            except psycopg.OperationalError:
                if not self._conn.closed and not getattr(self._conn, "broken", False):
                    raise
                log.warning("pylinxi: shared Postgres connection dropped; reconnecting")
                with contextlib.suppress(Exception):
                    self._conn.close()
                self._conn = psycopg.connect(self.conninfo, autocommit=True)
                return fn(self._conn)

    def _pick(self) -> _LockConn:
        """The least-busy healthy lock connection, opening one while below the limit."""
        with self._lconns_lock:
            for lc in [x for x in self._lconns if not x.healthy]:
                self._lconns.remove(lc)
                with contextlib.suppress(Exception):
                    lc.conn.close()
            if len(self._lconns) < self.lock_connections:
                lc = _LockConn(psycopg.connect(self.conninfo, autocommit=True))
                self._lconns.append(lc)
                return lc
            return min(self._lconns, key=lambda x: len(x.runs))

    def _backfill_heads(self) -> None:
        """Index journals written by pylinxi 1.0 (which had no heads table)."""
        t = self.table

        def go(c: Any) -> None:
            if c.execute(f"SELECT 1 FROM {t}_heads LIMIT 1").fetchone():
                return
            c.execute(f"INSERT INTO {t}_heads (run_id, seq, kind, key, ts, payload) "
                      f"SELECT DISTINCT ON (run_id) run_id, seq, kind, key, "
                      f"extract(epoch from created_at), payload FROM {t} ORDER BY run_id, seq DESC "
                      f"ON CONFLICT (run_id) DO NOTHING")
        self._shared(go)

    # -- writes
    def _write(self, c: Any, e: Entry) -> None:
        t = self.table
        with c.transaction():
            c.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))", (f"{t}:{e.run_id}", _APPEND_SEED))
            row = c.execute(f"SELECT seq, sig FROM {t} WHERE run_id=%s ORDER BY seq DESC LIMIT 1",
                            (e.run_id,)).fetchone()
            payload = self._seal(e, row[0] + 1 if row else 0, row[1] if row else "")
            c.execute(f"INSERT INTO {t} (run_id, seq, kind, key, payload, sig) VALUES (%s,%s,%s,%s,%s,%s)",
                      (e.run_id, e.seq, e.kind, e.key, payload, e.sig))
            c.execute(f"INSERT INTO {t}_heads (run_id, seq, kind, key, ts, payload) VALUES (%s,%s,%s,%s,%s,%s) "
                      f"ON CONFLICT (run_id) DO UPDATE SET seq=EXCLUDED.seq, kind=EXCLUDED.kind, "
                      f"key=EXCLUDED.key, ts=EXCLUDED.ts, payload=EXCLUDED.payload",
                      (e.run_id, e.seq, e.kind, e.key, e.ts, payload))

    def _append_locked(self, e: Entry) -> None:
        owner = self._owners.get(e.run_id)
        if owner is None:
            self._shared(lambda c: self._write(c, e))
            return
        with owner.lock:                                  # fenced: write through the lock's session
            if not owner.healthy:
                raise LockLost(f"lost the lock on run {e.run_id!r} (its database connection dropped); "
                               f"another worker may now own it")
            try:
                self._write(owner.conn, e)
            except psycopg.OperationalError as ex:
                if not owner.healthy:
                    raise LockLost(f"lost the lock on run {e.run_id!r} (its database connection dropped); "
                                   f"another worker may now own it") from ex
                raise

    # -- reads
    def _rows(self, run_id: str) -> List[Tuple[str, str]]:
        return self._shared(lambda c: [(p, s) for p, s in c.execute(
            f"SELECT payload, sig FROM {self.table} WHERE run_id=%s ORDER BY seq", (run_id,)).fetchall()])

    def last_row(self, run_id: str) -> Optional[Tuple[str, str]]:
        row = self._shared(lambda c: c.execute(
            f"SELECT payload, sig FROM {self.table} WHERE run_id=%s ORDER BY seq DESC LIMIT 1", (run_id,)).fetchone())
        return (row[0], row[1]) if row else None

    def heads(self, kinds: Optional[Sequence[str]] = None) -> List[Head]:
        sql, params = f"SELECT run_id, seq, kind, key, ts, payload FROM {self.table}_heads", []
        if kinds is not None:
            sql += " WHERE kind = ANY(%s)"
            params = [list(kinds)]
        rows = self._shared(lambda c: c.execute(sql, params).fetchall())
        return [Head(r, s, k, ky, float(ts), p) for r, s, k, ky, ts, p in rows]

    def runs(self, kind: Optional[str] = None) -> Iterable[str]:
        """All run ids, or only runs whose latest entry has this kind."""
        if kind is not None:
            return [h.run_id for h in self.heads([kind])]
        return [r[0] for r in self._shared(lambda c: c.execute(
            f"SELECT DISTINCT run_id FROM {self.table}").fetchall())]

    def pending_timers(self, due_before: Optional[float] = None) -> List[Tuple[str, float]]:
        out = []
        for h in self.heads(["suspend"]):
            res = json.loads(h.payload).get("result")
            wake = res.get("wake_at") if isinstance(res, dict) else None
            if isinstance(wake, (int, float)) and (due_before is None or wake <= due_before):
                out.append((h.run_id, float(wake)))
        return sorted(out, key=lambda t: t[1])

    # -- ownership
    @contextlib.contextmanager
    def lock(self, run_id: str) -> Iterator[None]:
        with super().lock(run_id):                        # in-process guard: sessions re-enter advisory locks
            args = (f"{self.table}:{run_id}", _EXEC_SEED)
            for attempt in range(2):
                lc = self._pick()
                try:
                    with lc.lock:
                        row = lc.conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, %s))",
                                              args).fetchone()
                    break
                except psycopg.OperationalError:
                    if lc.healthy or attempt:              # a real error, or a second dead connection
                        raise
                    log.warning("pylinxi: idle Postgres lock connection had dropped; using a new one")
            if row is None or not row[0]:
                raise RunLocked(f"run {run_id!r} is being executed by another worker")
            lc.runs.add(run_id)
            self._owners[run_id] = lc
            try:
                yield
            finally:
                self._owners.pop(run_id, None)
                lc.runs.discard(run_id)
                if lc.healthy:
                    try:
                        with lc.lock:
                            lc.conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, %s))", args)
                    except Exception as ex:  # noqa: BLE001 -- never mask the run's own outcome
                        log.warning("pylinxi: could not release the lock on run %r: %r", run_id, ex)

    def _delete(self, run_id: str) -> None:
        def go(c: Any) -> None:
            with c.transaction():
                c.execute(f"DELETE FROM {self.table} WHERE run_id=%s", (run_id,))
                c.execute(f"DELETE FROM {self.table}_heads WHERE run_id=%s", (run_id,))
        owner = self._owners.get(run_id)
        if owner is not None:
            with owner.lock:
                go(owner.conn)
        else:
            self._shared(go)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()
        with self._lconns_lock:
            for lc in self._lconns:
                with contextlib.suppress(Exception):
                    lc.conn.close()
            self._lconns.clear()
