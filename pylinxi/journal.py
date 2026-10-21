"""Append-only, delta-based run journal with a tamper-evident hash chain and redaction.

Each entry stores only what one step produced (its result + the state keys that
changed since the previous entry) -- never a full state snapshot. Entries are
chained: sig = HMAC(key, prev_sig || canonical(entry)), or plain SHA-256 when no
key is configured, so edits, reordering and deletions *inside* the chain are
detectable. Truncating the newest entries is only detectable against an external
anchor: store `head(run_id)` somewhere else and pass it to `verify(anchor=...)`.

Appends are serialized at the storage level (file locks, SQLite IMMEDIATE
transactions, Postgres advisory locks), so several Journal instances or processes
can safely share one store.
"""
from __future__ import annotations

import base64
import binascii
import contextlib
import dataclasses
import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union, cast

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows: in-process locking only
    fcntl = None  # type: ignore[assignment]


class RunLocked(Exception):
    """Another worker currently owns this run."""


class JournalCorrupt(Exception):
    """Stored data can't be decoded: tampering, a bad manual edit, or disk damage."""


# ---------------------------------------------------------------- codec
_TAG = "__t__"


class Codec:
    """Lossless JSON codec.

    Round-trips str/int/float/bool/None, lists, dicts (including non-string keys and
    dicts that happen to contain the tag key), tuples, sets, bytes, and classes you
    `register()` (dataclasses, pydantic models, or plain classes). Unregistered
    custom objects raise TypeError instead of silently changing type on replay.
    """

    def __init__(self) -> None:
        self._types: Dict[str, Type] = {}

    def register(self, cls: Type) -> Type:
        self._types[cls.__qualname__] = cls
        return cls

    def encode(self, o: Any) -> Any:
        if isinstance(o, (str, int, float, bool)) or o is None:
            return o
        if isinstance(o, list):
            return [self.encode(v) for v in o]
        if isinstance(o, dict):
            if all(isinstance(k, str) for k in o):
                inner = {k: self.encode(v) for k, v in o.items()}
                return {_TAG: "dict", "v": inner} if _TAG in o else inner
            return {_TAG: "map", "v": [[self.encode(k), self.encode(v)] for k, v in o.items()]}
        if isinstance(o, tuple):
            return {_TAG: "tuple", "v": [self.encode(v) for v in o]}
        if isinstance(o, (set, frozenset)):
            return {_TAG: "set", "v": [self.encode(v) for v in sorted(o, key=repr)]}
        if isinstance(o, (bytes, bytearray)):
            return {_TAG: "bytes", "v": base64.b64encode(bytes(o)).decode()}
        name = type(o).__qualname__
        if name in self._types:
            if dataclasses.is_dataclass(o):
                data = {f.name: getattr(o, f.name) for f in dataclasses.fields(o)}
            elif hasattr(o, "model_dump"):
                data = o.model_dump()
            else:
                data = dict(vars(o))
            return {_TAG: name, "v": self.encode(data)}
        raise TypeError(f"cannot journal a {name}; register it with pylinxi.default_codec.register({name}) "
                        f"so it round-trips with the same type on replay")

    def decode(self, o: Any) -> Any:
        if isinstance(o, list):
            return [self.decode(v) for v in o]
        if isinstance(o, dict):
            if _TAG not in o:
                return {k: self.decode(v) for k, v in o.items()}
            if set(o) != {_TAG, "v"}:
                raise JournalCorrupt(f"malformed tagged value: {sorted(o)}")
            t, v = o[_TAG], cast(Any, o["v"])
            if t == "dict":
                return {k: self.decode(x) for k, x in v.items()}
            if t == "map":
                return {self.decode(k): self.decode(x) for k, x in v}
            if t == "tuple":
                return tuple(self.decode(x) for x in v)
            if t == "set":
                return {self.decode(x) for x in v}
            if t == "bytes":
                try:
                    return base64.b64decode(v, validate=True)
                except (binascii.Error, ValueError) as e:
                    raise JournalCorrupt("invalid bytes payload") from e
            if t in self._types:
                return self._types[t](**self.decode(v))
            raise JournalCorrupt(f"unknown type tag {t!r} (register the class before loading)")
        return o

    def dumps(self, o: Any) -> str:
        return json.dumps(self.encode(o), sort_keys=True, separators=(",", ":"))

    def loads(self, s: str) -> Any:
        return self.decode(json.loads(s))


default_codec = Codec()


# ---------------------------------------------------------------- redaction
DEFAULT_REDACT = re.compile(
    r"(^|_)(pass|passwd|password|passphrase|secret|token|api_?key|access_?key|private_?key|"
    r"authorization|auth|credentials?|ssn|cookie|session_?id)(_|$)", re.I)
REDACTED = "[REDACTED]"


def _norm_key(k: str) -> str:
    """camelCase / kebab-case / dotted -> snake_case, so `apiKey` and `api-key` match."""
    return re.sub(r"[-.\s]+", "_", re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", k)).lower()


def is_secret_key(k: Any, pattern: Optional[re.Pattern] = DEFAULT_REDACT) -> bool:
    return pattern is not None and isinstance(k, str) and bool(pattern.search(_norm_key(k)))


def redact(o: Any, pattern: Optional[re.Pattern] = DEFAULT_REDACT) -> Any:
    """Replace values under secret-looking keys. Works on plain or *encoded* trees
    (walks dicts and lists), so applying it after Codec.encode also covers tuples,
    sets and registered dataclasses."""
    if pattern is None:
        return o
    if isinstance(o, dict):
        return {k: (REDACTED if is_secret_key(k, pattern) else redact(v, pattern)) for k, v in o.items()}
    if isinstance(o, list):
        return [redact(v, pattern) for v in o]
    return o


def find_secret_keys(o: Any, pattern: Optional[re.Pattern] = DEFAULT_REDACT, path: str = "") -> List[str]:
    """Paths of secret-looking keys anywhere in an encoded tree."""
    found: List[str] = []
    if pattern is None:
        return found
    if isinstance(o, dict):
        for k, v in o.items():
            p = f"{path}.{k}" if path else str(k)
            if is_secret_key(k, pattern):
                found.append(p)
            else:
                found += find_secret_keys(v, pattern, p)
    elif isinstance(o, list):
        for i, v in enumerate(o):
            found += find_secret_keys(v, pattern, f"{path}[{i}]")
    return found


# ---------------------------------------------------------------- entries
@dataclass
class Entry:
    run_id: str
    key: str              # e.g. "classify#0", "t1/fetch#0" (inside a concurrent branch)
    kind: str             # start|inbox|step|failed|signal|timer|marker|suspend|done
    result: Any = None
    delta: Dict[str, Any] = field(default_factory=dict)   # changed state keys
    removed: List[str] = field(default_factory=list)      # deleted state keys
    meta: Dict[str, Any] = field(default_factory=dict)
    persisted: bool = True         # False => result withheld (sensitive step)
    seq: int = -1
    ts: float = field(default_factory=time.time)
    prev: str = ""
    kid: str = ""                  # signing key id ("" = unkeyed SHA-256 chain)
    sig: str = ""

    def body(self) -> dict:
        # shallow on purpose: asdict() would flatten nested dataclasses in `result`
        return {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name != "sig"}


_FIELDS = {f.name for f in dataclasses.fields(Entry)} - {"sig"}

# process-wide registry of held runs, so two Journal instances on one store conflict too
_held: Dict[Tuple[str, str], threading.Lock] = {}
_held_guard = threading.Lock()


class Journal:
    """Base journal. Subclasses implement _append_locked, _read, runs and _delete."""

    def __init__(self, secret: Union[None, bytes, Dict[str, bytes]] = None,
                 codec: Codec = default_codec,
                 redact_pattern: Optional[re.Pattern] = DEFAULT_REDACT,
                 active_kid: Optional[str] = None):
        """secret: one HMAC key, or {key_id: key} for rotation. New entries are signed
        with `active_kid` (default: the last key in the dict); old entries keep
        verifying as long as their key id is still present."""
        if isinstance(secret, (bytes, bytearray)):
            secret = {"default": bytes(secret)}
        self.keys: Dict[str, bytes] = dict(secret or {})
        if active_kid is not None and active_kid not in self.keys:
            raise ValueError(f"active_kid {active_kid!r} not in keys")
        self.active_kid = active_kid or (list(self.keys)[-1] if self.keys else "")
        self.codec, self.redact_pattern = codec, redact_pattern
        self._lock = threading.Lock()

    # -- signing
    def _sign(self, prev: str, payload: str, kid: str) -> Optional[str]:
        msg = (prev + payload).encode()
        if kid:
            key = self.keys.get(kid)
            return hmac.new(key, msg, hashlib.sha256).hexdigest() if key else None
        return hashlib.sha256(msg).hexdigest()

    def _serialize(self, e: Entry) -> str:
        return self.codec.dumps(e.body())

    def _seal(self, e: Entry, seq: int, prev: str) -> str:
        """Assign position in the chain and sign. Returns the canonical payload."""
        e.seq, e.prev, e.kid = seq, prev, self.active_kid
        payload = self._serialize(e)
        e.sig = self._sign(prev, payload, e.kid) or ""
        return payload

    def redact_value(self, v: Any) -> Any:
        if self.redact_pattern is None:
            return v
        return self.codec.decode(redact(self.codec.encode(v), self.redact_pattern))

    def append(self, e: Entry) -> Entry:
        e.delta = self.redact_value(e.delta)
        e.meta = self.redact_value(e.meta)
        self._append_locked(e)
        return e

    def _append_locked(self, e: Entry) -> None:
        """Atomically: read the stored tail, seal e after it, write e."""
        raise NotImplementedError

    def _entry(self, payload: str, sig: str) -> Entry:
        try:
            data = self.codec.loads(payload)
            if not isinstance(data, dict) or set(data) != _FIELDS:
                raise JournalCorrupt("entry has unexpected fields")
            return Entry(**data, sig=sig)
        except JournalCorrupt:
            raise
        except Exception as ex:  # noqa: BLE001
            raise JournalCorrupt(f"undecodable entry: {ex}") from ex

    # -- run ownership
    def _lock_ns(self) -> str:
        """Identity of the underlying store (runs with the same id in different stores don't conflict)."""
        return f"mem:{id(self)}"

    def _lock_file(self, run_id: str) -> Optional[Path]:
        return None

    @contextlib.contextmanager
    def lock(self, run_id: str) -> Iterator[None]:
        """Exclusive ownership of a run while it executes; raises RunLocked otherwise.
        Covers other tasks/threads in this process, and other processes for file-backed
        stores (POSIX). PostgresJournal overrides this with database locks."""
        k = (self._lock_ns(), run_id)
        with _held_guard:
            lk = _held.setdefault(k, threading.Lock())
        if not lk.acquire(blocking=False):
            raise RunLocked(f"run {run_id!r} is already executing")
        fd = None
        try:
            path = self._lock_file(run_id)
            if path is not None and fcntl is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    os.close(fd)
                    fd = None
                    raise RunLocked(f"run {run_id!r} is executing in another process") from None
            yield
        finally:
            if fd is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
            lk.release()

    # -- queries
    def load(self, run_id: str) -> List[Entry]:
        return self._read(run_id)

    def head(self, run_id: str) -> Optional[str]:
        """Signature of the newest entry: store it elsewhere as a truncation anchor."""
        es = self._read(run_id)
        return es[-1].sig if es else None

    def verify(self, run_id: str, require_signed: bool = False, anchor: Optional[str] = None) -> bool:
        """Check order, links and signatures of the whole chain. Never raises.

        require_signed -- unkeyed (plain SHA-256) entries fail too; use when keys are
                          configured so a run can't be rewritten as an unkeyed chain.
        anchor         -- a sig previously returned by head(); fails if it's missing,
                          which detects truncation of newer entries.
        """
        try:
            prev, seen_anchor = "", anchor is None
            for i, e in enumerate(self._read(run_id)):
                if e.seq != i or e.prev != prev or (require_signed and not e.kid):
                    return False
                expected = self._sign(prev, self._serialize(e), e.kid)
                if expected is None or not hmac.compare_digest(e.sig, expected):
                    return False
                prev = e.sig
                seen_anchor = seen_anchor or hmac.compare_digest(e.sig, anchor or "")
            return seen_anchor
        except Exception:  # noqa: BLE001
            return False

    def runs_where_last(self, kind: str, key: Optional[str] = None) -> List[str]:
        """Run ids whose most recent entry has this kind (and key, if given)."""
        out = []
        for rid in self.runs():
            try:
                es = self._read(rid)
            except JournalCorrupt:
                continue
            if es and es[-1].kind == kind and (key is None or es[-1].key == key):
                out.append(rid)
        return out

    def pending_timers(self, due_before: Optional[float] = None) -> List[Tuple[str, float]]:
        """[(run_id, wake_at)] for runs currently suspended with a deadline
        (ctx.sleep, wait_for(timeout=)), soonest first. Pass due_before=time.time()
        for the ones ready to resume."""
        out = []
        for rid in self.runs_where_last("suspend"):
            wake = self._read(rid)[-1].result.get("wake_at")
            if wake is not None and (due_before is None or wake <= due_before):
                out.append((rid, float(wake)))
        return sorted(out, key=lambda t: t[1])

    def state_at(self, run_id: str, seq: Optional[int] = None) -> dict:
        """Rebuild flow state as of entry `seq` (time travel) by folding deltas."""
        state: dict = {}
        for e in self._read(run_id):
            if seq is not None and e.seq > seq:
                break
            state.update(e.delta)
            for k in e.removed:
                state.pop(k, None)
        return state

    def delete(self, run_id: str) -> None:
        """Permanently remove a run (retention policies, right-to-erasure requests)."""
        with self._lock:
            self._delete(run_id)

    def close(self) -> None:
        """Release connections/handles. Safe to call more than once."""

    def __enter__(self) -> Journal:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- backend hooks
    def runs(self) -> Iterable[str]:
        raise NotImplementedError

    def _read(self, run_id: str) -> List[Entry]:
        raise NotImplementedError

    def _delete(self, run_id: str) -> None:
        raise NotImplementedError


class MemoryJournal(Journal):
    def __init__(self, **kw: Any):
        super().__init__(**kw)
        self._data: Dict[str, List[Tuple[str, str]]] = {}

    def _append_locked(self, e: Entry) -> None:
        with self._lock:
            rows = self._data.setdefault(e.run_id, [])
            prev_seq = json.loads(rows[-1][0])["seq"] if rows else -1
            payload = self._seal(e, prev_seq + 1, rows[-1][1] if rows else "")
            rows.append((payload, e.sig))

    def _read(self, run_id: str) -> List[Entry]:
        with self._lock:
            rows = list(self._data.get(run_id, []))
        return [self._entry(p, s) for p, s in rows]

    def runs(self) -> Iterable[str]:
        with self._lock:
            return [r for r, rows in self._data.items() if rows]

    def _delete(self, run_id: str) -> None:
        self._data.pop(run_id, None)


class FileJournal(Journal):
    """One JSONL file per run. Appends hold an exclusive file lock, repair a torn
    final line left by a crash, and fsync (optional). POSIX gives cross-process
    safety; on Windows locking is per-process only."""

    def __init__(self, directory: str, fsync: bool = True, **kw: Any):
        super().__init__(**kw)
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync

    def _path(self, run_id: str) -> Path:
        if not re.fullmatch(r"[\w.-]+", run_id) or run_id in (".", ".."):
            raise ValueError("run_id may only contain letters, digits, _ . -")
        return self.dir / f"{run_id}.jsonl"

    def _lock_ns(self) -> str:
        return f"file:{self.dir.resolve()}"

    def _lock_file(self, run_id: str) -> Optional[Path]:
        return self.dir / ".locks" / f"{self._path(run_id).stem}.lock"

    @staticmethod
    def _tail_and_repair(f: IO[bytes]) -> Tuple[int, str]:
        """Return (seq, sig) of the last complete line; truncate a torn final line."""
        size = f.seek(0, os.SEEK_END)
        if size == 0:
            return -1, ""
        block, pos, buf = 65536, size, b""
        while pos > 0:
            step = min(block, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
            if buf.count(b"\n") >= 2 or pos == 0:
                break
        if not buf.endswith(b"\n"):                      # torn write from a crash
            cut = buf.rfind(b"\n")
            new_size = pos + cut + 1 if cut >= 0 else pos
            f.truncate(new_size)
            buf = buf[: cut + 1] if cut >= 0 else b""
            if not buf and new_size > 0:                 # torn line was longer than our window
                f.seek(0)
                buf = f.read()
        lines = [ln for ln in buf.split(b"\n") if ln.strip()]
        if not lines:
            return -1, ""
        rec = json.loads(lines[-1])
        return int(json.loads(rec["p"])["seq"]), str(rec["s"])

    def _append_locked(self, e: Entry) -> None:
        with self._lock, open(self._path(e.run_id), "a+b") as f:
            if fcntl is not None:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                seq, prev = self._tail_and_repair(f)
                payload = self._seal(e, seq + 1, prev)
                f.seek(0, os.SEEK_END)
                f.write((json.dumps({"p": payload, "s": e.sig}) + "\n").encode())
                f.flush()
                if self.fsync:
                    os.fsync(f.fileno())
            finally:
                if fcntl is not None:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _read(self, run_id: str) -> List[Entry]:
        p = self._path(run_id)
        if not p.exists():
            return []
        text = p.read_text(encoding="utf-8")
        lines = text.split("\n")
        torn = lines.pop() if not text.endswith("\n") else None   # possibly torn final line
        out = []
        for n, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
                out.append(self._entry(rec["p"], rec["s"]))
            except JournalCorrupt:
                raise
            except Exception as ex:  # noqa: BLE001
                raise JournalCorrupt(f"{p.name}: line {n} is not a valid entry") from ex
        if torn and torn.strip():
            try:
                rec = json.loads(torn)
                out.append(self._entry(rec["p"], rec["s"]))
            except Exception:  # noqa: BLE001 -- crash mid-write; repaired on the next append
                pass
        return out

    def runs(self) -> Iterable[str]:
        return [p.stem for p in self.dir.glob("*.jsonl") if p.stat().st_size > 0]

    def _delete(self, run_id: str) -> None:
        self._path(run_id).unlink(missing_ok=True)


class SQLiteJournal(Journal):
    """SQLite store (WAL). Appends run in IMMEDIATE transactions, so several
    processes can share the database file."""

    def __init__(self, path: str = ":memory:", **kw: Any):
        super().__init__(**kw)
        self.path = path
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._db.execute("PRAGMA busy_timeout=30000")
        self._setup()

    def _setup(self, attempts: int = 50) -> None:
        """Enable WAL and create the table. Switching journal mode needs an exclusive
        lock and SQLite doesn't always wait for it, so several processes opening the
        same file at once can see "database is locked": retry with backoff."""
        for i in range(attempts):
            try:
                mode = self._db.execute("PRAGMA journal_mode").fetchone()
                if self.path != ":memory:" and (mode is None or str(mode[0]).lower() != "wal"):
                    self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("CREATE TABLE IF NOT EXISTS pylinxi_journal (run_id TEXT, seq INTEGER, "
                                 "payload TEXT, sig TEXT, PRIMARY KEY (run_id, seq))")
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e) and "busy" not in str(e) or i == attempts - 1:
                    raise
                time.sleep(min(0.01 * 2 ** i, 0.5))

    def _lock_ns(self) -> str:
        return f"mem:{id(self)}" if self.path == ":memory:" else f"sqlite:{os.path.abspath(self.path)}"

    def _lock_file(self, run_id: str) -> Optional[Path]:
        if self.path == ":memory:":
            return None
        digest = hashlib.sha256(run_id.encode()).hexdigest()[:32]
        return Path(os.path.abspath(self.path) + ".locks") / f"{digest}.lock"

    def _append_locked(self, e: Entry) -> None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute("SELECT seq, sig FROM pylinxi_journal WHERE run_id=? "
                                       "ORDER BY seq DESC LIMIT 1", (e.run_id,)).fetchone()
                payload = self._seal(e, row[0] + 1 if row else 0, row[1] if row else "")
                self._db.execute("INSERT INTO pylinxi_journal VALUES (?,?,?,?)", (e.run_id, e.seq, payload, e.sig))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def _read(self, run_id: str) -> List[Entry]:
        with self._lock:
            rows = self._db.execute("SELECT payload, sig FROM pylinxi_journal WHERE run_id=? ORDER BY seq",
                                    (run_id,)).fetchall()
        return [self._entry(p, s) for p, s in rows]

    def runs(self) -> Iterable[str]:
        with self._lock:
            return [r[0] for r in self._db.execute("SELECT DISTINCT run_id FROM pylinxi_journal")]

    def _delete(self, run_id: str) -> None:
        self._db.execute("DELETE FROM pylinxi_journal WHERE run_id=?", (run_id,))

    def close(self) -> None:
        self._db.close()


def open_journal(url: str, **kw: Any) -> Journal:
    """Open a journal from a URL:
    memory://   file:///abs/dir   file:rel/dir   sqlite:///rel.db   sqlite:////abs.db   postgresql://...
    """
    if url.startswith("memory:"):
        return MemoryJournal(**kw)
    if url.startswith("file:"):
        return FileJournal(url[len("file://"):] if url.startswith("file://") else url[len("file:"):], **kw)
    if url.startswith("sqlite:"):
        if url.startswith("sqlite:////"):
            path = "/" + url[len("sqlite:////"):]
        elif url.startswith("sqlite:///"):
            path = url[len("sqlite:///"):]
        else:
            path = url[len("sqlite:"):]
        return SQLiteJournal(path or ":memory:", **kw)
    if url.startswith(("postgres://", "postgresql://")):
        from .postgres import PostgresJournal
        return PostgresJournal(url, **kw)
    raise ValueError(f"unsupported journal url: {url!r}")
