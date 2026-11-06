"""Append-only, delta-based run journal with a tamper-evident hash chain and redaction.

Each entry stores only what one step produced (its result + the state keys that
changed since the previous entry) -- never a full state snapshot.

Integrity (schema v2):
- every entry is chained to the previous one and bound to its run id:
  sig = HMAC(key, "pylinxi/2" | run_id | prev_sig | stored_payload), or SHA-256 when
  no key is configured. Signatures cover the stored bytes, so verification never
  re-encodes anything and works without your registered classes.
- every `load()` checks the chain (run id, order, links, signatures) before the
  entries are used, so forged or copied rows are refused before they can replay.
  With keys configured, unsigned entries are refused too (`require_signed`).
- truncating the newest entries is only detectable against an external anchor:
  store `head(run_id)` somewhere else and pass it to `verify(anchor=...)`.
Schema v1 entries written by pylinxi 1.0 still load and verify.

Appends are serialized at the storage level (file locks, SQLite IMMEDIATE
transactions, Postgres advisory locks), so several Journal instances or processes
can safely share one store. A run's first entry must be its `start` entry, so a
deleted run can't be recreated by a late write.
"""
from __future__ import annotations

import base64
import binascii
import contextlib
import dataclasses
import datetime as _dt
import decimal
import enum
import hashlib
import hmac
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid as _uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Type, Union, cast

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows: in-process locking only
    fcntl = None  # type: ignore[assignment]

log = logging.getLogger("pylinxi")

SCHEMA_VERSION = 2


class RunLocked(Exception):
    """Another worker currently owns this run."""


class LockLost(RunLocked):
    """This worker lost ownership of the run while executing it (for example its
    database lock connection dropped); it must stop, another worker may take over."""


class JournalCorrupt(Exception):
    """Stored data can't be trusted: tampering, rows copied from another run, a bad
    manual edit, a missing signing key, or disk damage."""


class UnknownRun(LookupError):
    """No run with this id exists in the journal (never started, or deleted)."""


# ---------------------------------------------------------------- codec
_TAG = "__t__"


class Codec:
    """Lossless JSON codec.

    Round-trips str/int/float/bool/None, lists, dicts (including non-string keys and
    dicts that happen to contain the tag key), tuples, sets, bytes, datetime, date,
    time, timedelta, Decimal and UUID out of the box, plus classes you `register()`:
    dataclasses, model classes with `model_dump()`, plain classes with a __dict__,
    and Enums.
    Registered classes are identified by module + qualified name, so two classes
    called `Config` in different modules never collide. Unregistered custom objects
    (and unregistered Enums) raise TypeError instead of silently changing type on replay.
    """

    def __init__(self) -> None:
        self._by_name: Dict[str, Type] = {}        # "module.qualname" -> class
        self._by_qual: Dict[str, List[Type]] = {}  # bare qualname -> classes (tags written by 1.0)
        self._names: Dict[Type, str] = {}          # class -> "module.qualname"

    def register(self, cls: Type) -> Type:
        """Register a class (usable as a decorator). Registering a new class under the
        same module + name (e.g. after a reload) replaces the old one."""
        if not isinstance(cls, type):
            raise TypeError("register() takes a class")
        name = f"{cls.__module__}.{cls.__qualname__}"
        old = self._by_name.get(name)
        if old is not None and old is not cls:
            self._names.pop(old, None)
        self._by_name[name], self._names[cls] = cls, name
        same = [c for c in self._by_qual.get(cls.__qualname__, [])
                if f"{c.__module__}.{c.__qualname__}" != name]
        self._by_qual[cls.__qualname__] = same + [cls]
        return cls

    def _resolve(self, tag: str) -> Type:
        cls = self._by_name.get(tag)
        if cls is not None:
            return cls
        cands = self._by_qual.get(tag, [])
        if len(cands) == 1:
            return cands[0]
        if len(cands) > 1:
            raise JournalCorrupt(f"type tag {tag!r} (written by pylinxi 1.0) matches several registered "
                                 f"classes: {sorted(self._names[c] for c in cands)}")
        raise JournalCorrupt(f"unknown type tag {tag!r} (register the class before loading)")

    def encode(self, o: Any) -> Any:
        t = type(o)
        if o is None or t in (str, int, float, bool):
            return o
        if isinstance(o, enum.Enum):
            name = self._names.get(t)
            if name is None:
                raise TypeError(f"cannot journal enum {t.__qualname__}; register it with "
                                f"pylinxi.default_codec.register({t.__qualname__}) so it comes back as the "
                                f"same enum on replay")
            return {_TAG: "enum", "v": [name, self.encode(o.value)]}
        name = self._names.get(t)
        if name is not None:
            if dataclasses.is_dataclass(o):
                data = {f.name: getattr(o, f.name) for f in dataclasses.fields(o) if f.init}
            elif hasattr(o, "model_dump"):
                data = o.model_dump()
            elif hasattr(o, "__dict__"):
                data = dict(vars(o))
            else:
                raise TypeError(f"cannot journal {name}: registered classes must be dataclasses, "
                                f"model classes with model_dump(), Enums or have a __dict__")
            return {_TAG: name, "v": self.encode(data)}
        if isinstance(o, list):
            return [self.encode(v) for v in o]
        if isinstance(o, dict):
            if all(type(k) is str for k in o):
                inner = {k: self.encode(v) for k, v in o.items()}
                return {_TAG: "dict", "v": inner} if _TAG in o else inner
            return {_TAG: "map", "v": [[self.encode(k), self.encode(v)] for k, v in o.items()]}
        if isinstance(o, tuple):
            return {_TAG: "tuple", "v": [self.encode(v) for v in o]}
        if isinstance(o, (set, frozenset)):
            return {_TAG: "set", "v": [self.encode(v) for v in sorted(o, key=repr)]}
        if isinstance(o, (bytes, bytearray)):
            return {_TAG: "bytes", "v": base64.b64encode(bytes(o)).decode()}
        if isinstance(o, _dt.datetime):
            return {_TAG: "datetime", "v": o.isoformat()}
        if isinstance(o, _dt.date):
            return {_TAG: "date", "v": o.isoformat()}
        if isinstance(o, _dt.time):
            return {_TAG: "time", "v": o.isoformat()}
        if isinstance(o, _dt.timedelta):
            return {_TAG: "timedelta", "v": [o.days, o.seconds, o.microseconds]}
        if isinstance(o, decimal.Decimal):
            return {_TAG: "decimal", "v": str(o)}
        if isinstance(o, _uuid.UUID):
            return {_TAG: "uuid", "v": str(o)}
        for base in (bool, int, float, str):          # subclasses of builtins: keep the value
            if isinstance(o, base):
                return base(o)
        tn = t.__qualname__
        raise TypeError(f"cannot journal a {tn}; register it with pylinxi.default_codec.register({tn}) "
                        f"so it round-trips with the same type on replay")

    def decode(self, o: Any) -> Any:
        if isinstance(o, list):
            return [self.decode(v) for v in o]
        if not isinstance(o, dict):
            return o
        if _TAG not in o:
            return {k: self.decode(v) for k, v in o.items()}
        if set(o) != {_TAG, "v"}:
            raise JournalCorrupt(f"malformed tagged value: {sorted(o)}")
        t, v = o[_TAG], cast(Any, o["v"])
        try:
            if t == "dict":
                return {k: self.decode(x) for k, x in v.items()}
            if t == "map":
                return {self.decode(k): self.decode(x) for k, x in v}
            if t == "tuple":
                return tuple(self.decode(x) for x in v)
            if t == "set":
                return {self.decode(x) for x in v}
            if t == "bytes":
                return base64.b64decode(v, validate=True)
            if t == "datetime":
                return _dt.datetime.fromisoformat(v)
            if t == "date":
                return _dt.date.fromisoformat(v)
            if t == "time":
                return _dt.time.fromisoformat(v)
            if t == "timedelta":
                return _dt.timedelta(days=v[0], seconds=v[1], microseconds=v[2])
            if t == "decimal":
                return decimal.Decimal(v)
            if t == "uuid":
                return _uuid.UUID(v)
            if t == "enum":
                return self._resolve(v[0])(self.decode(v[1]))
        except JournalCorrupt:
            raise
        except (binascii.Error, ValueError, TypeError, KeyError, IndexError, AttributeError,
                decimal.InvalidOperation) as e:
            raise JournalCorrupt(f"invalid {t!r} payload: {e}") from e
        return self._resolve(t)(**self.decode(v))

    def dumps(self, o: Any) -> str:
        return json.dumps(self.encode(o), sort_keys=True, separators=(",", ":"))

    def loads(self, s: str) -> Any:
        return self.decode(json.loads(s))


default_codec = Codec()


# ---------------------------------------------------------------- redaction
# A key counts as secret when its LAST word is a credential word, so `access_token`,
# `client_secret` and `apiKey` match while `token_budget`, `max_tokens`,
# `auth_required`, `pass_count` and `session_id` don't.
DEFAULT_REDACT = re.compile(
    r"(^|_)(password|passwd|passphrase|secret|token|api_?key|access_?key|secret_?key|private_?key|"
    r"authorization|auth|credentials?|ssn|cookies?|bearer)$", re.I)
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
    kind: str             # start|inbox|step|failed|signal|timer|marker|suspend|done|link
    result: Any = None
    delta: Dict[str, Any] = field(default_factory=dict)   # changed state keys
    removed: List[str] = field(default_factory=list)      # deleted state keys
    meta: Dict[str, Any] = field(default_factory=dict)
    persisted: bool = True         # False => result withheld (sensitive step)
    seq: int = -1
    ts: float = field(default_factory=time.time)
    prev: str = ""
    kid: str = ""                  # signing key id ("" = unkeyed SHA-256 chain)
    v: int = SCHEMA_VERSION        # entry schema version (1 = written by pylinxi 1.0)
    sig: str = ""

    def body(self) -> dict:
        # shallow on purpose: asdict() would flatten nested dataclasses in `result`
        return {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name != "sig"}


_FIELDS = {f.name for f in dataclasses.fields(Entry)} - {"sig"}


@dataclass
class Head:
    """A run's newest entry, as stored (used by schedulers; not chain-verified)."""
    run_id: str
    seq: int
    kind: str
    key: str
    ts: float
    payload: str

    def raw(self) -> dict:
        return cast(dict, json.loads(self.payload))


def _head(run_id: str, payload: str) -> Optional[Head]:
    try:
        r = json.loads(payload)
        return Head(run_id, int(r["seq"]), str(r["kind"]), str(r["key"]), float(r.get("ts", 0.0)), payload)
    except (ValueError, KeyError, TypeError):
        return None


# process-wide registry of held runs, so two Journal instances on one store conflict too
_held: Dict[Tuple[str, str], List[Any]] = {}       # key -> [lock, users]
_held_guard = threading.Lock()


class Journal:
    """Base journal. Backends implement _rows, _append_locked, runs and _delete."""

    blocking = True   # does real I/O: flows run appends in a worker thread, off the event loop

    def __init__(self, secret: Union[None, bytes, Dict[str, bytes]] = None,
                 codec: Codec = default_codec,
                 redact_pattern: Optional[re.Pattern] = DEFAULT_REDACT,
                 active_kid: Optional[str] = None,
                 verify_on_load: bool = True,
                 require_signed: Optional[bool] = None,
                 error_redactor: Optional[Callable[[str], str]] = None):
        """secret: one HMAC key, or {key_id: key} for rotation. New entries are signed
        with `active_kid` (default: the last key in the dict); old entries keep
        verifying as long as their key id is still present.

        verify_on_load -- check every run's chain and signatures each time it's loaded
                          (default). Forged, copied or edited rows raise JournalCorrupt.
        require_signed -- refuse unsigned entries on load; defaults to True whenever
                          keys are configured. Set False while migrating a journal
                          that was written before you added keys.
        error_redactor -- optional fn(str) -> str applied to error text before it is
                          journaled (e.g. to mask card numbers or emails).
        """
        if isinstance(secret, (bytes, bytearray)):
            secret = {"default": bytes(secret)}
        self.keys: Dict[str, bytes] = dict(secret or {})
        if active_kid is not None and active_kid not in self.keys:
            raise ValueError(f"active_kid {active_kid!r} not in keys")
        self.active_kid = active_kid or (list(self.keys)[-1] if self.keys else "")
        self.codec, self.redact_pattern = codec, redact_pattern
        self.verify_on_load = verify_on_load
        self.require_signed = bool(self.keys) if require_signed is None else require_signed
        self.error_redactor = error_redactor
        self._lock = threading.Lock()

    # -- signing
    @staticmethod
    def _msg(run_id: str, prev: str, payload: str, v: int) -> bytes:
        if v >= 2:
            return b"\x00".join([b"pylinxi/2", run_id.encode(), prev.encode(), payload.encode()])
        return (prev + payload).encode()                    # schema v1 (pylinxi 1.0)

    def _sign(self, msg: bytes, kid: str) -> Optional[str]:
        if kid:
            key = self.keys.get(kid)
            return hmac.new(key, msg, hashlib.sha256).hexdigest() if key else None
        return hashlib.sha256(msg).hexdigest()

    def _serialize(self, e: Entry) -> str:
        return self.codec.dumps(e.body())

    def _seal(self, e: Entry, seq: int, prev: str) -> str:
        """Assign the entry's position in the chain and sign it. Returns the payload."""
        if seq == 0 and e.kind != "start":
            raise UnknownRun(f"run {e.run_id!r} does not exist (never started, or deleted)")
        e.seq, e.prev, e.kid, e.v = seq, prev, self.active_kid, SCHEMA_VERSION
        payload = self._serialize(e)
        e.sig = self._sign(self._msg(e.run_id, prev, payload, e.v), e.kid) or ""
        return payload

    def redact_value(self, v: Any) -> Any:
        if self.redact_pattern is None:
            return v
        return self.codec.decode(redact(self.codec.encode(v), self.redact_pattern))

    def redact_error(self, text: str, limit: int = 500) -> str:
        text = text if len(text) <= limit else text[: limit - 1] + "…"
        if self.error_redactor is not None:
            try:
                text = self.error_redactor(text)
            except Exception:  # noqa: BLE001 -- a broken redactor must not lose the entry
                text = "[error text withheld: error_redactor failed]"
        return text

    def prepared_delta(self, encoded: Dict[str, Any]) -> Dict[str, Any]:
        """A delta built from already-encoded values (state keys -> Codec.encode trees):
        redacted and decoded in one pass, into fresh objects that share nothing with the
        caller's live state. Flows build deltas this way on the event loop thread, so a
        write on a worker thread never touches objects flow code may still change."""
        tree = redact(encoded, self.redact_pattern) if self.redact_pattern is not None else encoded
        return {k: self.codec.decode(v) for k, v in tree.items()}

    def append(self, e: Entry) -> Entry:
        if not getattr(e, "_prepared", False):        # see prepared_delta()
            e.delta = self.redact_value(e.delta)
        e.meta = self.redact_value(e.meta)
        self._append_locked(e)
        return e

    # -- reading and checking
    def _check(self, run_id: str, rows: Sequence[Tuple[str, str]], signatures: bool,
               require_signed: bool) -> List[dict]:
        """Check a run's stored rows: they belong to `run_id`, form an unbroken chain,
        and (if `signatures`) every signature matches the stored bytes."""
        prev, raws = "", []
        for i, (payload, sig) in enumerate(rows):
            try:
                raw = json.loads(payload)
            except ValueError as ex:
                raise JournalCorrupt(f"run {run_id!r}: entry {i} is not valid JSON") from ex
            if not isinstance(raw, dict):
                raise JournalCorrupt(f"run {run_id!r}: entry {i} is not an object")
            if raw.get("run_id") != run_id:
                raise JournalCorrupt(f"run {run_id!r}: entry {i} belongs to run {raw.get('run_id')!r} "
                                     f"(rows copied from another run?)")
            if raw.get("seq") != i or raw.get("prev") != prev:
                raise JournalCorrupt(f"run {run_id!r}: chain broken at entry {i}")
            if signatures:
                kid = str(raw.get("kid") or "")
                if require_signed and not kid:
                    raise JournalCorrupt(f"run {run_id!r}: entry {i} is not signed")
                expected = self._sign(self._msg(run_id, prev, payload, int(raw.get("v", 1))), kid)
                if expected is None:
                    raise JournalCorrupt(f"run {run_id!r}: entry {i} is signed with unknown key id {kid!r}")
                if not hmac.compare_digest(str(sig), expected):
                    raise JournalCorrupt(f"run {run_id!r}: signature mismatch at entry {i}")
            raws.append(raw)
            prev = str(sig)
        return raws

    def _decode_entry(self, raw: dict, sig: str) -> Entry:
        try:
            data = self.codec.decode(raw)
            known = {k: v for k, v in data.items() if k in _FIELDS}   # ignore fields added later
            known.setdefault("v", 1)
            return Entry(**known, sig=sig)
        except JournalCorrupt:
            raise
        except Exception as ex:  # noqa: BLE001
            raise JournalCorrupt(f"undecodable entry: {ex}") from ex

    def load(self, run_id: str) -> List[Entry]:
        """All entries of a run, oldest first, after checking the chain (and, by
        default, every signature). Raises JournalCorrupt if anything doesn't check out."""
        rows = self._rows(run_id)
        raws = self._check(run_id, rows, self.verify_on_load, self.require_signed and self.verify_on_load)
        return [self._decode_entry(raw, sig) for raw, (_, sig) in zip(raws, rows)]

    def exists(self, run_id: str) -> bool:
        return self.last_row(run_id) is not None

    def last_row(self, run_id: str) -> Optional[Tuple[str, str]]:
        """The run's newest stored (payload, sig) row, cheaply (not chain-verified)."""
        rows = self._rows(run_id)
        return rows[-1] if rows else None

    def head(self, run_id: str) -> Optional[str]:
        """Signature of the newest entry: store it elsewhere as a truncation anchor."""
        rows = self._rows(run_id)
        return str(rows[-1][1]) if rows else None

    def verify(self, run_id: str, require_signed: bool = False, anchor: Optional[str] = None) -> bool:
        """Check the whole chain against the stored bytes. Never raises; False for a
        run that doesn't exist.

        require_signed -- unkeyed (plain SHA-256) entries fail too; use when keys are
                          configured so a run can't be rewritten as an unkeyed chain.
        anchor         -- a sig previously returned by head(); fails if it's missing,
                          which detects truncation of newer entries.
        """
        try:
            rows = self._rows(run_id)
            if not rows:
                return False
            self._check(run_id, rows, True, require_signed)
            return anchor is None or any(hmac.compare_digest(str(s), anchor) for _, s in rows)
        except Exception:  # noqa: BLE001
            return False

    # -- scheduling queries
    def heads(self, kinds: Optional[Sequence[str]] = None) -> List[Head]:
        """The newest entry of every run (optionally only those of these kinds)."""
        out = []
        for rid in self.runs():
            rows = self._rows(rid)
            h = _head(rid, rows[-1][0]) if rows else None
            if h is not None and (kinds is None or h.kind in kinds):
                out.append(h)
        return out

    def runs_where_last(self, kind: str, key: Optional[str] = None) -> List[str]:
        """Run ids whose most recent entry has this kind (and key, if given)."""
        return [h.run_id for h in self.heads([kind]) if key is None or h.key == key]

    def pending_timers(self, due_before: Optional[float] = None) -> List[Tuple[str, float]]:
        """[(run_id, wake_at)] for runs currently suspended with a deadline
        (ctx.sleep, wait_for(timeout=)), soonest first. Pass due_before=time.time()
        for the ones ready to resume."""
        out = []
        for h in self.heads(["suspend"]):
            res = h.raw().get("result")
            wake = res.get("wake_at") if isinstance(res, dict) else None
            if isinstance(wake, (int, float)) and (due_before is None or wake <= due_before):
                out.append((h.run_id, float(wake)))
        return sorted(out, key=lambda t: t[1])

    def state_at(self, run_id: str, seq: Optional[int] = None) -> dict:
        """Rebuild flow state as of entry `seq` (time travel) by folding deltas."""
        state: dict = {}
        for e in self.load(run_id):
            if seq is not None and e.seq > seq:
                break
            state.update(e.delta)
            for k in e.removed:
                state.pop(k, None)
        return state

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
            slot = _held.setdefault(k, [threading.Lock(), 0])
            slot[1] += 1
        fd = None
        acquired = slot[0].acquire(blocking=False)
        try:
            if not acquired:
                raise RunLocked(f"run {run_id!r} is already executing")
            path = self._lock_file(run_id)
            if path is not None and fcntl is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                for _ in range(5):
                    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        os.close(fd)
                        fd = None
                        raise RunLocked(f"run {run_id!r} is executing in another process") from None
                    try:                              # still the file at that path? (delete unlinks them)
                        same = os.fstat(fd).st_ino == os.stat(str(path)).st_ino
                    except OSError:
                        same = False
                    if same:
                        break
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    fd = None
                else:
                    raise RunLocked(f"run {run_id!r}: its lock file keeps changing")
            yield
        finally:
            if fd is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
            if acquired:
                slot[0].release()
            with _held_guard:
                slot[1] -= 1
                if slot[1] <= 0 and _held.get(k) is slot:
                    del _held[k]

    # -- signals, deletion
    def _raw(self, run_id: str) -> List[dict]:
        out = []
        for p, _ in self._rows(run_id):
            try:
                out.append(json.loads(p))
            except ValueError:
                out.append({})
        return out

    def _successor(self, run_id: str) -> Tuple[bool, Optional[str]]:
        """(done?, the next generation's id if it continued as new)."""
        for r in self._raw(run_id):
            if r.get("kind") == "done":
                meta = r.get("meta")
                nxt = meta.get("continued_as") if isinstance(meta, dict) else None
                return True, nxt if isinstance(nxt, str) else None
        return False, None

    def chain(self, run_id: str) -> List[str]:
        """Every generation of the continue-as-new chain `run_id` belongs to, root first."""
        raws = self._raw(run_id)
        start = next((r for r in raws if r.get("kind") == "start"), None)
        res = start.get("result") if start else None
        root = res.get("root") if isinstance(res, dict) else None
        rid: Optional[str] = root if isinstance(root, str) else run_id
        ids: List[str] = []
        while rid and rid not in ids:
            if not self._rows(rid):
                break
            ids.append(rid)
            rid = self._successor(rid)[1]
        if run_id not in ids and self._rows(run_id):
            ids.append(run_id)
        return ids

    def send_signal(self, run_id: str, name: str, value: Any = True) -> str:
        """Durably deliver a signal without executing the run (safe while it runs).

        The signal is appended to the run's inbox; a waiting `ctx.wait_for(name)` gets
        it on the next resume (a Worker resumes such runs automatically). Signals to a
        run that continued as new go to its newest generation. Returns that run id.
        Raises UnknownRun, or ValueError if the run already completed."""
        found = find_secret_keys(self.codec.encode({name: value}), self.redact_pattern)
        if found:
            raise ValueError(f"signal contains secret-looking keys {found}; signals are journaled in clear")
        rid, seen = run_id, set()
        while True:
            if not self._rows(rid):
                raise UnknownRun(f"run {rid!r} does not exist")
            done, nxt = self._successor(rid)
            if not done:
                break
            if not nxt or nxt in seen:
                raise ValueError(f"run {rid!r} has already completed; signal {name!r} not delivered")
            seen.add(rid)
            rid = nxt
        self.append(Entry(rid, "inbox", "inbox", {name: [value]}))
        return rid

    def delete(self, run_id: str, chain: bool = True) -> List[str]:
        """Permanently remove a run -- by default with every generation of its
        continue-as-new chain -- for retention policies and erasure requests.

        Takes each run's lock first, so it raises RunLocked (and deletes nothing) while
        any of them is executing. Lock files are removed too. Returns the deleted ids."""
        ids = self.chain(run_id) if chain else ([run_id] if self._rows(run_id) else [])
        with contextlib.ExitStack() as stack:
            for rid in ids:
                stack.enter_context(self.lock(rid))
            with self._lock:
                for rid in ids:
                    self._delete(rid)
            for rid in ids:                     # while still held: a waiter re-checks the inode
                p = self._lock_file(rid)
                if p is not None:
                    with contextlib.suppress(OSError):
                        p.unlink()
        return ids

    def close(self) -> None:
        """Release connections/handles. Safe to call more than once."""

    def __enter__(self) -> Journal:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- backend hooks
    def runs(self) -> Iterable[str]:
        raise NotImplementedError

    def _rows(self, run_id: str) -> List[Tuple[str, str]]:
        """A run's stored (payload, sig) rows in sequence order."""
        raise NotImplementedError

    def _append_locked(self, e: Entry) -> None:
        """Atomically: read the stored tail, seal e after it, write e."""
        raise NotImplementedError

    def _delete(self, run_id: str) -> None:
        raise NotImplementedError


class MemoryJournal(Journal):
    blocking = False

    def __init__(self, **kw: Any):
        super().__init__(**kw)
        self._data: Dict[str, List[Tuple[str, str]]] = {}

    def _append_locked(self, e: Entry) -> None:
        with self._lock:
            rows = self._data.get(e.run_id, [])
            payload = self._seal(e, len(rows), rows[-1][1] if rows else "")
            self._data.setdefault(e.run_id, []).append((payload, e.sig))

    def _rows(self, run_id: str) -> List[Tuple[str, str]]:
        with self._lock:
            return list(self._data.get(run_id, []))

    def last_row(self, run_id: str) -> Optional[Tuple[str, str]]:
        with self._lock:
            rows = self._data.get(run_id)
            return rows[-1] if rows else None

    def runs(self) -> Iterable[str]:
        with self._lock:
            return [r for r, rows in self._data.items() if rows]

    def _delete(self, run_id: str) -> None:
        self._data.pop(run_id, None)


_SAFE_ID = re.compile(r"[a-z0-9_.-]{1,100}")
_WIN_RESERVED = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}


def _fsync_dir(path: Path) -> None:
    with contextlib.suppress(OSError, AttributeError):
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class FileJournal(Journal):
    """One JSONL file per run. Appends hold an exclusive file lock, repair a torn
    final line left by a crash, and fsync (optional). POSIX gives cross-process
    safety; on Windows locking is per-process only.

    Any run id works: lowercase ids made of [a-z0-9_.-] are stored as `<id>.jsonl`;
    others get a readable prefix plus a hash (`ticket-a1~3f2c….jsonl`), so ids that
    differ only by case never share a file on case-insensitive filesystems."""

    def __init__(self, directory: str, fsync: bool = True, create: bool = True, **kw: Any):
        super().__init__(**kw)
        self.dir = Path(directory)
        if not create and not self.dir.is_dir():
            raise FileNotFoundError(f"journal directory {str(self.dir)!r} does not exist")
        if not self.dir.is_dir():
            self.dir.mkdir(parents=True, exist_ok=True)
            if fsync:
                _fsync_dir(self.dir.parent)
        self.fsync = fsync
        self._paths: Dict[str, Path] = {}

    def _owner(self, p: Path) -> Optional[str]:
        """Run id recorded in a file's first entry (None if empty/unreadable)."""
        try:
            with open(p, "rb") as f:
                first = f.readline()
            return str(json.loads(json.loads(first)["p"])["run_id"]) if first.strip() else None
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def _path(self, run_id: str) -> Path:
        """A run's file. Resolved once and cached. A plain `<id>.jsonl` is only used if
        it really belongs to this run id: on case-insensitive filesystems `ABC.jsonl` and
        `abc.jsonl` are the same file, so ownership is checked, never assumed."""
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")
        cached = self._paths.get(run_id)
        if cached is not None and cached.exists():    # re-resolved if another instance moved/deleted it
            return cached
        slug = re.sub(r"[^a-z0-9_.-]+", "-", run_id.lower()).strip(".-")[:40] or "run"
        hashed = self.dir / f"{slug}~{hashlib.sha256(run_id.encode()).hexdigest()[:24]}.jsonl"
        plain = self.dir / f"{run_id}.jsonl"
        plain_ok = (len(run_id.encode()) <= 200 and re.fullmatch(r"[\w.-]+", run_id) is not None
                    and run_id not in (".", ".."))
        safe = (plain_ok and _SAFE_ID.fullmatch(run_id) is not None
                and run_id.split(".")[0] not in _WIN_RESERVED)
        path = hashed
        if not hashed.exists() and plain_ok:
            exists = False
            with contextlib.suppress(OSError):
                exists = plain.exists() and plain.stat().st_size > 0
            if exists:
                if self._owner(plain) == run_id:         # 1.0 naming, really this run's file
                    path = plain
            elif safe:
                path = plain
        if path.exists():                               # only settled names are cached
            if len(self._paths) >= 4096:
                self._paths.clear()
            self._paths[run_id] = path
        return path

    def _lock_ns(self) -> str:
        return f"file:{self.dir.resolve()}"

    def _lock_file(self, run_id: str) -> Optional[Path]:
        # named after the run id itself, not its storage file, so every instance and
        # process agrees on it whatever file name they resolved
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")
        return self.dir / ".locks" / f"{hashlib.sha256(run_id.encode()).hexdigest()[:32]}.lock"

    @staticmethod
    def _valid_record(line: bytes) -> bool:
        try:
            rec = json.loads(line)
            json.loads(rec["p"])
            return isinstance(rec.get("s"), str)
        except (ValueError, KeyError, TypeError):
            return False

    @classmethod
    def _tail_and_repair(cls, f: IO[bytes]) -> Tuple[int, str]:
        """Return (seq, sig) of the last entry. A final line missing only its newline is
        completed; a genuinely torn line (crash mid-write) is cut off."""
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
        if not buf.endswith(b"\n"):
            cut = buf.rfind(b"\n")
            tail = buf[cut + 1:] if cut >= 0 else buf
            if (cut >= 0 or pos == 0) and cls._valid_record(tail):
                f.seek(0, os.SEEK_END)
                f.write(b"\n")                          # complete entry, newline lost: keep it
                buf += b"\n"
            else:                                       # torn write from a crash
                new_size = pos + cut + 1 if cut >= 0 else pos
                f.truncate(new_size)
                buf = buf[: cut + 1] if cut >= 0 else b""
                if not buf and new_size > 0:            # torn line was longer than our window
                    f.seek(0)
                    buf = f.read()
        lines = [ln for ln in buf.split(b"\n") if ln.strip()]
        if not lines:
            return -1, ""
        rec = json.loads(lines[-1])
        return int(json.loads(rec["p"])["seq"]), str(rec["s"])

    def _append_locked(self, e: Entry) -> None:
        p = self._path(e.run_id)
        created = not p.exists()
        with self._lock, open(p, "a+b") as f:
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
        if created and self.fsync:
            _fsync_dir(self.dir)

    def _rows_of(self, p: Path) -> List[Tuple[str, str]]:
        if not p.exists():
            return []
        text = p.read_text(encoding="utf-8")
        lines = text.split("\n")
        last = lines.pop() if not text.endswith("\n") else None   # final line without newline
        out = []
        for n, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
                out.append((str(rec["p"]), str(rec["s"])))
            except (ValueError, KeyError, TypeError) as ex:
                raise JournalCorrupt(f"{p.name}: line {n} is not a valid entry") from ex
        if last and last.strip() and self._valid_record(last.encode()):
            rec = json.loads(last)                       # complete; the next append adds its newline
            out.append((str(rec["p"]), str(rec["s"])))
        return out

    def _rows(self, run_id: str) -> List[Tuple[str, str]]:
        return self._rows_of(self._path(run_id))

    def _tail_record(self, p: Path) -> Optional[Tuple[str, str]]:
        try:
            with open(p, "rb") as f:
                size = f.seek(0, os.SEEK_END)
                f.seek(max(0, size - 65536))
                lines = [ln for ln in f.read().split(b"\n") if ln.strip()]
        except OSError:
            return None
        for ln in reversed(lines):
            if self._valid_record(ln):
                rec = json.loads(ln)
                return str(rec["p"]), str(rec["s"])
        if size <= 65536:
            return None
        rows = self._rows_of(p)                       # very long last line: read the whole file
        return rows[-1] if rows else None

    def last_row(self, run_id: str) -> Optional[Tuple[str, str]]:
        return self._tail_record(self._path(run_id))

    def _id_of(self, p: Path) -> Optional[str]:
        return p.stem if "~" not in p.stem else self._owner(p)

    def runs(self) -> Iterable[str]:
        out = []
        for p in self.dir.glob("*.jsonl"):
            if p.stat().st_size > 0:
                rid = self._id_of(p)
                if rid is not None:
                    out.append(rid)
        return out

    def heads(self, kinds: Optional[Sequence[str]] = None) -> List[Head]:
        out = []
        for p in self.dir.glob("*.jsonl"):
            rec = self._tail_record(p)
            if rec is None:
                continue
            with contextlib.suppress(ValueError, AttributeError):
                h = _head(str(json.loads(rec[0]).get("run_id")), rec[0])
                if h is not None and (kinds is None or h.kind in kinds):
                    out.append(h)
        return out

    def _delete(self, run_id: str) -> None:
        self._path(run_id).unlink(missing_ok=True)
        self._paths.pop(run_id, None)


class SQLiteJournal(Journal):
    """SQLite store (WAL). Appends run in IMMEDIATE transactions, so several
    processes can share the database file. A heads table indexes each run's newest
    entry, so scheduling queries don't scan history."""

    def __init__(self, path: str = ":memory:", create: bool = True, **kw: Any):
        super().__init__(**kw)
        self.path = path
        if not create and path != ":memory:" and not os.path.exists(path):
            raise FileNotFoundError(f"journal database {path!r} does not exist")
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._db.execute("PRAGMA busy_timeout=30000")
        self._setup()

    def _setup(self, attempts: int = 50) -> None:
        """Enable WAL and create the tables. Switching journal mode needs an exclusive
        lock and SQLite doesn't always wait for it, so several processes opening the
        same file at once can see "database is locked": retry with backoff."""
        for i in range(attempts):
            try:
                mode = self._db.execute("PRAGMA journal_mode").fetchone()
                if self.path != ":memory:" and (mode is None or str(mode[0]).lower() != "wal"):
                    self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("CREATE TABLE IF NOT EXISTS pylinxi_journal (run_id TEXT, seq INTEGER, "
                                 "payload TEXT, sig TEXT, PRIMARY KEY (run_id, seq))")
                self._db.execute("CREATE TABLE IF NOT EXISTS pylinxi_heads (run_id TEXT PRIMARY KEY, "
                                 "seq INTEGER, kind TEXT, key TEXT, ts REAL, payload TEXT)")
                self._db.execute("CREATE INDEX IF NOT EXISTS pylinxi_heads_kind ON pylinxi_heads (kind)")
                self._backfill_heads()
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e) and "busy" not in str(e) or i == attempts - 1:
                    raise
                time.sleep(min(0.01 * 2 ** i, 0.5))

    def _backfill_heads(self) -> None:
        """Index journals written by pylinxi 1.0 (which had no heads table)."""
        if self._db.execute("SELECT 1 FROM pylinxi_heads LIMIT 1").fetchone():
            return
        if not self._db.execute("SELECT 1 FROM pylinxi_journal LIMIT 1").fetchone():
            return
        self._db.execute("BEGIN IMMEDIATE")
        try:
            rows = self._db.execute(
                "SELECT j.run_id, j.payload FROM pylinxi_journal j JOIN (SELECT run_id, MAX(seq) m "
                "FROM pylinxi_journal GROUP BY run_id) x ON j.run_id = x.run_id AND j.seq = x.m").fetchall()
            for rid, payload in rows:
                h = _head(rid, payload)
                if h is not None:
                    self._db.execute("INSERT OR IGNORE INTO pylinxi_heads VALUES (?,?,?,?,?,?)",
                                     (rid, h.seq, h.kind, h.key, h.ts, payload))
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

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
                self._db.execute("INSERT INTO pylinxi_heads VALUES (?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE "
                                 "SET seq=excluded.seq, kind=excluded.kind, key=excluded.key, ts=excluded.ts, "
                                 "payload=excluded.payload", (e.run_id, e.seq, e.kind, e.key, e.ts, payload))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def _rows(self, run_id: str) -> List[Tuple[str, str]]:
        with self._lock:
            return [(p, s) for p, s in self._db.execute(
                "SELECT payload, sig FROM pylinxi_journal WHERE run_id=? ORDER BY seq", (run_id,))]

    def last_row(self, run_id: str) -> Optional[Tuple[str, str]]:
        with self._lock:
            row = self._db.execute("SELECT payload, sig FROM pylinxi_journal WHERE run_id=? "
                                   "ORDER BY seq DESC LIMIT 1", (run_id,)).fetchone()
        return (row[0], row[1]) if row else None

    def heads(self, kinds: Optional[Sequence[str]] = None) -> List[Head]:
        sql, params = "SELECT run_id, seq, kind, key, ts, payload FROM pylinxi_heads", []
        if kinds is not None:
            sql += f" WHERE kind IN ({','.join('?' * len(kinds))})"
            params = list(kinds)
        with self._lock:
            return [Head(r, s, k, ky, float(ts or 0), p) for r, s, k, ky, ts, p in self._db.execute(sql, params)]

    def runs(self) -> Iterable[str]:
        with self._lock:
            return [r[0] for r in self._db.execute("SELECT DISTINCT run_id FROM pylinxi_journal")]

    def _delete(self, run_id: str) -> None:
        self._db.execute("DELETE FROM pylinxi_journal WHERE run_id=?", (run_id,))
        self._db.execute("DELETE FROM pylinxi_heads WHERE run_id=?", (run_id,))

    def close(self) -> None:
        self._db.close()


def open_journal(url: str, create: bool = True, **kw: Any) -> Journal:
    """Open a journal from a URL:
    memory://   file:///abs/dir   file:rel/dir   sqlite:///rel.db   sqlite:////abs.db   postgresql://...

    create=False refuses to create a missing file or directory (the CLI uses this, so
    a mistyped path is an error rather than an empty journal).
    """
    if url.startswith("memory:"):
        return MemoryJournal(**kw)
    if url.startswith("file:"):
        path = url[len("file://"):] if url.startswith("file://") else url[len("file:"):]
        return FileJournal(path, create=create, **kw)
    if url.startswith("sqlite:"):
        if url.startswith("sqlite:////"):
            path = "/" + url[len("sqlite:////"):]
        elif url.startswith("sqlite:///"):
            path = url[len("sqlite:///"):]
        elif url.startswith("sqlite://"):
            raise ValueError(f"ambiguous sqlite URL {url!r}: use sqlite:///relative.db or sqlite:////absolute.db")
        else:
            path = url[len("sqlite:"):]
        return SQLiteJournal(path or ":memory:", create=create, **kw)
    if url.startswith(("postgres://", "postgresql://")):
        from .postgres import PostgresJournal
        return PostgresJournal(url, **kw)
    raise ValueError(f"unsupported journal url: {url!r}")
