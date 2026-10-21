"""Postgres journal for multi-worker deployments.  pip install "pylinxi[postgres]"

Guarantees beyond the single-process journals:
- Every append runs in a transaction holding a per-run advisory lock and reads the
  chain tail from the database, so the hash chain stays linear with many writers.
- `lock(run_id)` takes a session advisory lock for the whole execution; a second
  worker trying to run/resume the same run gets RunLocked instead of duplicating
  side effects. The lock is released automatically if the worker dies.
- Execution and append locks live in separate namespaces (64-bit hashes with
  different seeds), so no run id can block another run's appends.
- Payloads are stored as TEXT (not JSONB) so signatures verify byte-for-byte;
  `kind` and `key` are broken out into indexed columns.
"""
from __future__ import annotations

import contextlib
import threading
from typing import Any, Iterable, Iterator, List, Optional, Tuple

from .journal import Entry, Journal, RunLocked

try:
    import psycopg
except ImportError as _e:  # pragma: no cover
    raise ImportError('PostgresJournal needs psycopg: pip install "psycopg[binary]"') from _e

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
"""

_EXEC_SEED, _APPEND_SEED = 0x4C49_4E58_0001, 0x4C49_4E58_0002   # separate lock namespaces


class PostgresJournal(Journal):
    def __init__(self, conninfo: str, table: str = "pylinxi_journal", **kw: Any):
        super().__init__(**kw)
        if not table.replace("_", "").isalnum():
            raise ValueError("table name must be alphanumeric/underscore")
        self.conninfo, self.table = conninfo, table
        self._conn = psycopg.connect(conninfo, autocommit=True)
        self._conn_lock = threading.Lock()
        with self._conn_lock:
            self._conn.execute(_SCHEMA.format(t=table))

    def _append_locked(self, e: Entry) -> None:
        with self._conn_lock, self._conn.transaction():
            self._conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
                               (f"{self.table}:{e.run_id}", _APPEND_SEED))
            row = self._conn.execute(
                f"SELECT seq, sig FROM {self.table} WHERE run_id=%s ORDER BY seq DESC LIMIT 1",
                (e.run_id,)).fetchone()
            payload = self._seal(e, row[0] + 1 if row else 0, row[1] if row else "")
            self._conn.execute(f"INSERT INTO {self.table} (run_id, seq, kind, key, payload, sig) "
                               f"VALUES (%s,%s,%s,%s,%s,%s)",
                               (e.run_id, e.seq, e.kind, e.key, payload, e.sig))

    def _read(self, run_id: str) -> List[Entry]:
        with self._conn_lock:
            rows = self._conn.execute(
                f"SELECT payload, sig FROM {self.table} WHERE run_id=%s ORDER BY seq", (run_id,)).fetchall()
        return [self._entry(p, s) for p, s in rows]

    @contextlib.contextmanager
    def lock(self, run_id: str) -> Iterator[None]:
        conn = psycopg.connect(self.conninfo, autocommit=True)   # session lock lives with this connection
        args = (f"{self.table}:{run_id}", _EXEC_SEED)
        try:
            row = conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, %s))", args).fetchone()
            if row is None or not row[0]:
                raise RunLocked(f"run {run_id!r} is being executed by another worker")
            try:
                yield
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, %s))", args)
        finally:
            conn.close()

    def _last_rows(self, kind: Optional[str], key: Optional[str]) -> List[Tuple[str, str, str, str]]:
        sql = (f"SELECT run_id, kind, key, payload FROM (SELECT DISTINCT ON (run_id) run_id, kind, key, payload "
               f"FROM {self.table} ORDER BY run_id, seq DESC) t WHERE TRUE")
        params: list = []
        if kind is not None:
            sql += " AND kind=%s"
            params.append(kind)
        if key is not None:
            sql += " AND key=%s"
            params.append(key)
        with self._conn_lock:
            return self._conn.execute(sql, params).fetchall()

    def runs(self, kind: Optional[str] = None) -> Iterable[str]:
        """All run ids, or only runs whose latest entry has this kind."""
        if kind is not None:
            return [r[0] for r in self._last_rows(kind, None)]
        with self._conn_lock:
            return [r[0] for r in self._conn.execute(f"SELECT DISTINCT run_id FROM {self.table}").fetchall()]

    def runs_where_last(self, kind: str, key: Optional[str] = None) -> List[str]:
        return [r[0] for r in self._last_rows(kind, key)]

    def pending_timers(self, due_before: Optional[float] = None) -> List[Tuple[str, float]]:
        """Runs suspended with a deadline, in one query."""
        out = []
        for rid, _, _, payload in self._last_rows("suspend", None):
            wake = self.codec.loads(payload)["result"].get("wake_at")
            if wake is not None and (due_before is None or wake <= due_before):
                out.append((rid, float(wake)))
        return sorted(out, key=lambda t: t[1])

    def _delete(self, run_id: str) -> None:
        with self._conn_lock:
            self._conn.execute(f"DELETE FROM {self.table} WHERE run_id=%s", (run_id,))

    def close(self) -> None:
        self._conn.close()
