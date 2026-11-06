"""Durable flows written as plain Python.

Branching is ordinary `if`/`match`/loops; there is no graph to wire. Every call
to a @step inside a running flow is journaled, so a crashed or suspended run
resumes by replaying the flow function: completed steps return their recorded
result instantly, and execution continues at the first unfinished step.

Rule: flow code (outside steps) must be deterministic -- put I/O, randomness,
clocks and LLM calls inside steps (or use ctx.now() / ctx.uuid()). If code changes
so that replay no longer matches the journal, a strict flow (the default) stops
with ReplayDivergence *before* running anything new; use ctx.patched() to change
code safely.

Concurrency: tasks created by flow code (asyncio.gather, create_task, TaskGroup)
each get their own key scope ("t0/", "t1/", ...) assigned in creation order, so
steps inside concurrent branches replay deterministically regardless of which
branch finishes first. When one branch suspends, the others keep running (up to
`drain_timeout`) until they also suspend or finish, and the run wakes at the
earliest deadline among them. When a run fails or finishes while branches still
run, steps already executing finish and are journaled (bounded by `drain_timeout`),
no new step starts, and the remaining branches are cancelled before the run returns.
"""
from __future__ import annotations

import asyncio
import builtins
import concurrent.futures
import contextlib
import contextvars
import functools
import hashlib
import inspect
import json
import logging
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from .journal import Entry, Journal, MemoryJournal, RunLocked, UnknownRun, find_secret_keys
from .policy import Policy, PriorAttemptRunning, StepFailed, run_with_policy

log = logging.getLogger("pylinxi")

Observer = Callable[[dict], None]

_current: contextvars.ContextVar[Optional[Context]] = contextvars.ContextVar("pylinxi_ctx", default=None)
_step_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("pylinxi_step", default=None)
_scope: contextvars.ContextVar[Optional[_Scope]] = contextvars.ContextVar("pylinxi_scope", default=None)

# kinds whose keys flow code reaches again on replay
_KEYED = ("step", "failed", "signal", "timer", "marker")
_INDEXED = ("step", "failed", "signal", "timer")   # entries that record their allocation index


class Suspended(BaseException):
    """Raised inside a flow when it must pause (signal and/or timer).

    A BaseException (like CancelledError) so `except Exception` in flow code
    can't swallow it by accident."""

    def __init__(self, signal: Optional[str] = None, wake_at: Optional[float] = None):
        what = f"signal {signal!r}" if signal else "timer"
        super().__init__(f"waiting for {what}" + (f" until {wake_at:.0f}" if wake_at else ""))
        self.signal, self.wake_at = signal, wake_at


class ContinueAsNew(BaseException):
    """Raised by ctx.continue_as_new(); ends this run and starts a fresh journal."""

    def __init__(self, args: tuple, kwargs: dict, state: dict):
        super().__init__("continue as new")
        self.args_, self.kwargs, self.state = args, kwargs, state


class ReplayDivergence(BaseException):
    """Replay no longer matches the journal: the flow's code changed in a way that
    would run a different step at a point where the run already recorded one.
    Raised before the new step runs, so nothing new happens. A BaseException so
    `except Exception` in flow code can't swallow it. Use ctx.patched() for
    intentional changes, or strict=False to only report it."""


class ReplayedError(Exception):
    """Stands in for the original exception when a caught step failure is replayed."""


class ResultNotJournaled(Exception):
    """A step ran but its result can't be journaled (unsupported type). It is not run
    again on resume -- that could repeat its side effect -- so replay raises this."""


class FlowMismatch(ValueError):
    """The run id belongs to a different flow."""


class RunExists(ValueError):
    """run() was given inputs for a run id that already exists with different inputs."""


def idempotency_key() -> str:
    """Stable key for the step currently executing ("<run_id>:<step_key>").

    Identical across retries and resumes, so pass it to external APIs (payments,
    email, ticketing) to make side effects exactly-once from their side.
    """
    k = _step_key.get()
    if k is None:
        raise RuntimeError("idempotency_key() is only available inside a running step")
    return k


# ---------------------------------------------------------------- @step
def step(fn: Optional[Callable] = None, *, policy: Optional[Policy] = None,
         name: Optional[str] = None, sensitive: bool = False) -> Any:
    """Mark a function as a durable, self-healing step.

    Works on sync/async functions and sync/async generators (streamed steps).
    Inside a running flow, calling it is checkpointed automatically.
    Called from inside another step, or outside any flow, it is a plain call
    (the outer step's result is the checkpoint).
    sensitive=True keeps the result out of the journal; it is recomputed on resume.
    """
    def deco(f: Callable) -> Callable:
        f_name = name or f.__name__

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            ctx = _current.get()
            if ctx is None or _step_key.get() is not None:
                return f(*args, **kwargs)
            return ctx._invoke(f, args, kwargs, policy, f_name, sensitive)

        wrapper.__pylinxi_step__ = True  # type: ignore[attr-defined]
        return wrapper

    return deco(fn) if fn is not None else deco


# ---------------------------------------------------------------- scopes & task tracking
class _Scope:
    __slots__ = ("prefix", "ctx", "counters", "children", "total", "err")

    def __init__(self, prefix: str, ctx: Context, err: frozenset = frozenset()):
        self.prefix, self.ctx = prefix, ctx
        self.counters: Dict[str, int] = {}
        self.children = 0
        self.total = 0          # keys allocated in this scope so far (allocation index)
        self.err = err          # step failures being handled where this branch was created

    def child(self) -> _Scope:
        n = self.children
        self.children += 1
        return _Scope(f"{self.prefix}t{n}/", self.ctx, self.err | _handling())


_writer_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
_writer_guard = threading.Lock()


def _writers() -> concurrent.futures.ThreadPoolExecutor:
    """Threads that write journal entries for blocking journals (file, SQLite, Postgres)."""
    global _writer_pool
    with _writer_guard:
        if _writer_pool is None:
            _writer_pool = concurrent.futures.ThreadPoolExecutor(max_workers=32, thread_name_prefix="pylinxi-journal")
        return _writer_pool


def _handling() -> frozenset:
    """Keys of the step failures propagating or being handled at this point of the
    flow code: inside an `except`, a `finally` or an `__aexit__` that a StepFailed
    is passing through, sys.exc_info() holds it (directly, as a cause/context, or in
    an exception group)."""
    exc = sys.exc_info()[1]
    if exc is None:
        return frozenset()
    keys: Set[str] = set()
    seen: Set[int] = set()
    todo: List[Any] = [exc]
    while todo:
        x = todo.pop()
        if x is None or id(x) in seen:
            continue
        seen.add(id(x))
        if isinstance(x, StepFailed):
            keys.add(x.key)
        todo += [x.__cause__, x.__context__]
        todo += list(getattr(x, "exceptions", ()) or ())
    return frozenset(keys)


def _scope_of(e: Entry) -> str:
    sc = e.meta.get("sc") if isinstance(e.meta, dict) else None
    if isinstance(sc, str):
        return sc
    return e.key.rsplit("/", 1)[0] + "/" if "/" in e.key else ""      # entries written before 1.1


def _from_wait_for() -> bool:
    """Python < 3.12: asyncio.wait_for wraps a plain coroutine in a task. 3.12+ awaits
    it directly, so to give the same keys on every version that task must not open a
    new branch scope."""
    f: Optional[Any] = sys._getframe(2)
    for _ in range(8):
        if f is None:
            return False
        if f.f_code.co_name == "wait_for" and f.f_code.co_filename.replace("\\", "/").endswith("asyncio/tasks.py"):
            return True
        f = f.f_back
    return False


def _install_task_factory(loop: asyncio.AbstractEventLoop) -> None:
    """Give every task created by *flow code* a child key scope, and track it so the
    run can drain or cancel stragglers. Tasks created inside steps or outside flows
    are untouched. Re-checked on every run, so an app that later replaces the loop's
    task factory is wrapped again rather than silently breaking branch keys."""
    prev = loop.get_task_factory()
    if getattr(prev, "_pylinxi", False):
        return

    def factory(lp: asyncio.AbstractEventLoop, coro: Any, **kw: Any) -> asyncio.Future:
        parent = _scope.get()

        def make() -> asyncio.Future:
            if prev is not None:
                return prev(lp, coro, **kw)
            return asyncio.Task(coro, loop=lp, **kw)

        if parent is None or not parent.ctx._active or _step_key.get() is not None:
            return make()
        if sys.version_info < (3, 12) and _from_wait_for():
            task = make()                                  # same scope, like 3.12+
            parent.ctx._track(task)
            return task
        child = parent.child()
        if kw.get("context") is not None:              # explicit context (3.11+)
            ctx_copy = kw["context"].copy()
            ctx_copy.run(_scope.set, child)
            kw["context"] = ctx_copy
            task = make()
        else:
            token = _scope.set(child)
            try:
                task = make()
            finally:
                _scope.reset(token)
        parent.ctx._track(task)
        return task

    factory._pylinxi = True  # type: ignore[attr-defined]
    loop.set_task_factory(factory)


def _step_failures(e: BaseException) -> Set[str]:
    """Keys of the StepFailed exceptions a run ended in: the exception itself, members of
    exception groups, and explicit causes (`raise X from e`). Implicit __context__ is not
    followed: a new failure inside the `except` that handled an earlier one doesn't make
    the earlier one unhandled."""
    keys: Set[str] = set()
    seen: Set[int] = set()
    todo = [e]
    while todo:
        x = todo.pop()
        if x is None or id(x) in seen:
            continue
        seen.add(id(x))
        if isinstance(x, StepFailed):
            keys.add(x.key)
        todo += [x.__cause__]  # type: ignore[list-item]
        todo += list(getattr(x, "exceptions", ()) or ())
    return keys


_BaseExceptionGroup: Any = getattr(builtins, "BaseExceptionGroup", None)   # Python 3.11+
_ExceptionGroup: Any = getattr(builtins, "ExceptionGroup", None)


def _leaves(e: BaseException) -> List[BaseException]:
    subs = getattr(e, "exceptions", None)
    if _BaseExceptionGroup is not None and isinstance(e, _BaseExceptionGroup) and subs:
        return [leaf for x in subs for leaf in _leaves(x)]
    return [e]


def _unwrap_group(g: BaseException) -> BaseException:
    """A TaskGroup wraps what its children raise in an exception group. Turn one made of
    suspensions (and the cancellations they caused) into a single suspension, so a
    durable wait inside a TaskGroup suspends the run instead of crashing it."""
    if _BaseExceptionGroup is None or not isinstance(g, _BaseExceptionGroup):
        return g
    leaves = _leaves(g)
    for x in leaves:
        if isinstance(x, ReplayDivergence):
            return x
    errors = [x for x in leaves if isinstance(x, Exception) and not isinstance(x, asyncio.CancelledError)]
    if errors:
        return errors[0] if len(errors) == 1 else _ExceptionGroup("flow failed", errors)
    for x in leaves:
        if isinstance(x, ContinueAsNew):
            return x
    susp = [x for x in leaves if isinstance(x, Suspended)]
    if not susp:
        return g
    wakes = [x.wake_at for x in susp if x.wake_at is not None]
    merged = Suspended(next((x.signal for x in susp if x.signal), None), min(wakes) if wakes else None)
    merged.parts = susp  # type: ignore[attr-defined]
    return merged


# ---------------------------------------------------------------- context
class Context:
    """Handed to every flow as its first argument."""

    def __init__(self, run_id: str, journal: Journal, state: dict, history: List[Entry],
                 emit: Callable[..., None], generation: int = 0, strict: bool = True):
        self.run_id, self.journal, self.state = run_id, journal, state
        self.generation = generation
        self.emit = emit
        self.strict = strict
        self._root = _Scope("", self)
        self._active = True
        self._tasks: Set[asyncio.Future] = set()
        self._finished_tasks: List[asyncio.Future] = []
        self._inflight: Set[asyncio.Future] = set()
        self._last_write: Optional[concurrent.futures.Future] = None   # tail of this run's ordered writes
        self._armed: Dict[str, float] = {}       # timers set in this execution whose wait hasn't ended
        self._awaited: List[str] = []            # signals being waited for right now (any branch)
        self._busy: Dict[Any, int] = {}          # task -> durable-wait bookkeeping it is doing right now
        self._cut_short = False                  # a sibling was still mid-step when a branch suspended
        self._root_task: Any = None              # the flow function's own task
        self._end_deadline: Optional[float] = None   # one drain_timeout budget for ending/suspending
        self.drain_timeout = 10.0
        self._in_step: Dict[Any, int] = {}       # task -> steps it is executing right now
        self._closing = False                    # the run is ending: no new step may start
        self._tags: Dict[str, List[str]] = {}    # key -> step failures being handled when it was allocated
        self._tagged_with: Set[str] = set()
        self._arghash: Dict[str, str] = {}       # tagged key -> hash of the call's arguments
        self._sup_cache: Dict[str, Entry] = {}   # superseded outcomes, replayed only for identical calls
        start = next((e for e in history if e.kind == "start"), None)
        # identifies this exact run (journal + incarnation), for the abandoned-attempt registry
        self._incarnation = f"{journal._lock_ns()}|{run_id}|{start.sig if start else ''}"

        self._cache: Dict[str, Entry] = {}       # key -> successful outcome (persisted)
        self._failed: Dict[str, Entry] = {}      # key -> last failure with no later success
        self._lost: Set[str] = set()             # ran, but result couldn't be journaled
        self._caught: Set[str] = set()           # failures the flow handled (replay re-raises them)
        self._history_keys: Set[str] = set()     # every keyed entry the flow will reach again
        self._history_index: Dict[Tuple[str, int], str] = {}   # (scope, allocation index) -> key
        self._consumed: Set[str] = set()
        self._alloc: Dict[str, Tuple[str, int]] = {}   # key -> (scope, allocation index)
        self._pending_failures: Dict[str, str] = {}   # key -> scope prefix where it was raised
        self._inbox: Dict[str, List[Any]] = {}
        self._inbox_seqs: Set[int] = set()
        self._signals_used: Dict[str, int] = {}
        self._diverged: Set[str] = set()
        self._halt: Optional[ReplayDivergence] = None   # strict divergence: nothing new may run
        self._state_warned: Set[str] = set()
        superseded = self._superseded(history)
        for e in history:
            self._absorb(e, e.seq in superseded)
        self._snap = self._snapshot()

    @staticmethod
    def _superseded(history: List[Entry]) -> Set[int]:
        """Entries the flow allocated while handling a step failure that then ended the
        run (cleanup in a `finally`, `__aexit__`, `except: ...; raise`): their key was
        tagged at allocation with the failures sys.exc_info() held. When the failed
        step is retried and succeeds, the flow takes another path, so these entries
        no longer bind replay order -- but they still replay if the flow reaches the
        same key again (the side effect happened once). Entries allocated outside error
        handling -- siblings that finished after the failure, steps the flow ran while
        a branch's failure was pending -- are never superseded."""
        uncaught = {e.key[len("uncaught:"):] for e in history
                    if e.kind == "marker" and e.key.startswith("uncaught:")}
        if not uncaught:
            return set()
        out: Set[int] = set()
        for e in history:
            tags = e.meta.get("pf") if e.kind in _INDEXED and isinstance(e.meta, dict) else None
            if isinstance(tags, list) and uncaught.intersection(tags):
                out.add(e.seq)
        return out

    def _absorb(self, e: Entry, superseded: bool = False) -> None:
        if e.kind == "inbox":
            if e.seq not in self._inbox_seqs:
                self._inbox_seqs.add(e.seq)
                for name, vals in (e.result or {}).items():
                    self._inbox.setdefault(name, []).extend(vals)
            return
        if e.kind == "marker" and e.key.startswith("caught:"):
            self._caught.add(e.key[len("caught:"):])
            return
        if e.kind == "marker" and e.key.startswith("uncaught:"):
            self._caught.discard(e.key[len("uncaught:"):])
            return
        if e.kind not in _KEYED or e.key == "flow":   # run-level failures aren't replayed
            return
        if superseded:
            # off the replay order (no divergence check, not "pending"), but a step
            # reached again under the same key replays instead of running twice
            if e.kind != "failed" and e.persisted:
                self._sup_cache.setdefault(e.key, e)
            if e.kind == "signal" and not e.meta.get("timed_out"):
                n = e.meta.get("signal", "")
                self._signals_used[n] = self._signals_used.get(n, 0) + 1
            return
        self._history_keys.add(e.key)
        idx = e.meta.get("i") if isinstance(e.meta, dict) else None
        if e.kind in _INDEXED and isinstance(idx, int):
            self._history_index.setdefault((_scope_of(e), idx), e.key)
        if e.kind == "failed":
            self._failed[e.key] = e
            if e.meta.get("lost_result"):
                self._lost.add(e.key)
        else:
            self._failed.pop(e.key, None)
            self._caught.discard(e.key)
            if e.persisted:
                self._cache[e.key] = e
        if e.kind == "signal" and not e.meta.get("timed_out"):
            n = e.meta.get("signal", "")
            self._signals_used[n] = self._signals_used.get(n, 0) + 1

    # -- internals
    def _track(self, task: asyncio.Future) -> None:
        self._tasks.add(task)

        def done(t: asyncio.Future) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self._finished_tasks.append(t)       # keep only tasks whose outcome matters
        task.add_done_callback(done)

    @contextlib.contextmanager
    def _busy_in(self) -> Iterator[None]:
        me = asyncio.current_task()
        self._busy[me] = self._busy.get(me, 0) + 1
        try:
            yield
        finally:
            left = self._busy.get(me, 1) - 1
            if left:
                self._busy[me] = left
            else:
                self._busy.pop(me, None)

    async def _quiesce(self) -> None:
        """A branch is about to suspend. A TaskGroup cancels its siblings the moment it
        does, so first let them get to their own durable wait (or finish the step they
        are in) -- otherwise their deadlines and progress would never be recorded and
        the run could wake too late, or never. Bounded by drain_timeout."""
        me = asyncio.current_task()
        if me not in self._tasks:                  # the flow's own task: the run drains instead
            return

        def busy() -> List[Any]:
            others = set(self._tasks)
            if self._root_task is not None:
                others.add(self._root_task)        # the flow body itself may be inside a step
            return [t for t in others
                    if t is not me and not t.done() and (t in self._in_step or t in self._busy)]

        await asyncio.sleep(0)                     # let just-created siblings start
        await asyncio.sleep(0)
        while True:
            left = self._remaining()
            b = busy()
            if not b:
                await asyncio.sleep(0)
                if not busy():
                    return
                continue
            if left <= 0:
                self._cut_short = True             # a sibling is still mid-step: make sure we come back
                return
            await asyncio.wait(b, timeout=min(0.02, left))

    def _remaining(self) -> float:
        """Seconds left of the single drain_timeout budget shared by everything that
        happens while the run suspends or ends (quiesce, drain, finishing steps)."""
        if self._end_deadline is None:
            self._end_deadline = time.monotonic() + self.drain_timeout
        return max(0.0, self._end_deadline - time.monotonic())

    def _scope_now(self) -> _Scope:
        s = _scope.get()
        return s if s is not None and s.ctx is self else self._root

    def _key(self, name: str, call: Optional[Tuple[tuple, dict]] = None) -> str:
        if self._halt is not None:
            raise ReplayDivergence(str(self._halt))
        scope = self._scope_now()
        self._note_progress(scope.prefix)
        n = scope.counters.get(name, 0)
        scope.counters[name] = n + 1
        i = scope.total
        scope.total += 1
        key = f"{scope.prefix}{name}#{n}"
        self._alloc[key] = (scope.prefix, i)
        tags = scope.err | _handling()
        if tags:
            self._tags[key] = sorted(tags)
            self._tagged_with |= tags
            h = self._hash_call(call)
            if h is not None:
                self._arghash[key] = h
        sup = self._sup_cache.pop(key, None)
        if sup is not None and key not in self._cache:
            h = self._arghash.get(key) if tags else self._hash_call(call)
            if h is not None and sup.meta.get("ah") == h:
                self._cache[key] = sup           # the identical call (e.g. a `finally` cleanup) already ran
        recorded = self._history_index.get((scope.prefix, i))
        if recorded is not None and recorded != key and key not in self._history_keys:
            self._divergence(key, recorded)
        return key

    def _hash_call(self, call: Optional[Tuple[tuple, dict]]) -> Optional[str]:
        """Fingerprint of a step call's arguments (None if they can't be serialized)."""
        if call is None:
            return None
        try:
            raw = self.journal.codec.dumps([list(call[0]), call[1]])
        except Exception:  # noqa: BLE001 -- unfingerprintable: such a step is simply run again
            return None
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _divergence(self, key: str, recorded: str) -> None:
        if key in self._diverged:
            return
        self._diverged.add(key)
        msg = (f"replay diverged in run {self.run_id!r}: the code now calls {key!r} where the journal "
               f"recorded {recorded!r}")
        self.emit("divergence", key, recorded=recorded, strict=self.strict)
        log.warning("%s", msg)
        if self.strict:
            # latched: even if flow code swallows this (gather(return_exceptions=True), a broad
            # `except BaseException`), no further step runs and the run fails with it
            self._halt = ReplayDivergence(msg + "; nothing new was run. Use ctx.patched() for intentional "
                                                "changes, or flow(strict=False) to only report this.")
            raise self._halt

    def _note_progress(self, prefix: str) -> None:
        """Flow code in `prefix` kept going, so failures raised in it (or in branches
        below it) look caught: journal that, so replay re-raises instead of re-running.
        If the run then ends in that same failure after all (e.g. the "progress" was a
        cleanup step in a `finally`), the run retracts the marker (`uncaught:`)."""
        for k, raised_in in list(self._pending_failures.items()):
            if raised_in.startswith(prefix):
                del self._pending_failures[k]
                self._caught.add(k)
                self._record(f"caught:{k}", "marker", True, {})

    def _uncatch(self, keys: Set[str]) -> None:
        for k in keys:
            self._pending_failures.pop(k, None)
            if k in self._caught or k in self._tagged_with:
                self._caught.discard(k)
                self._record(f"uncaught:{k}", "marker", True, {})

    def _snapshot(self) -> Dict[str, str]:
        dumps = self.journal.codec.dumps
        snap = {}
        for k, v in self.state.items():
            try:
                snap[k] = dumps(v)
            except Exception as ex:  # noqa: BLE001 -- state is rebuilt by replay; deltas are informational
                snap[k] = f"\x00unserializable:{type(v).__name__}:{id(v)}"
                if k not in self._state_warned:
                    self._state_warned.add(k)
                    self.emit("state_unserializable", k, error=repr(ex)[:200])
                    log.warning("run %r: state key %r holds a %s that can't be journaled; its deltas are "
                                "recorded as a placeholder", self.run_id, k, type(v).__name__)
        return snap

    def _diff(self) -> Tuple[dict, list]:
        new = self._snapshot()
        encoded: Dict[str, Any] = {}
        for k, v in new.items():
            if self._snap.get(k) != v:
                encoded[k] = (f"[UNSERIALIZABLE {type(self.state[k]).__name__}]"
                              if v.startswith("\x00unserializable:") else json.loads(v))
        removed = [k for k in self._snap if k not in new]
        self._snap = new
        # decoded (and redacted) copies, made now on the loop thread: the writer thread
        # must never encode live objects that flow code may still be changing
        return self.journal.prepared_delta(encoded), removed

    def _entry(self, key: str, kind: str, result: Any, meta: dict, persisted: bool = True) -> Entry:
        delta, removed = self._diff()
        if kind in _INDEXED and key in self._alloc:
            sc, i = self._alloc[key]
            meta = {**meta, "i": i, "sc": sc}
            if key in self._tags:
                meta["pf"] = self._tags[key]
                if key in self._arghash:
                    meta["ah"] = self._arghash[key]
        if persisted and self.journal.blocking and result is not None:
            try:                                   # same for the result: freeze it before handing it off
                result = self.journal.codec.loads(self.journal.codec.dumps(result))
            except Exception:  # noqa: BLE001 -- left as is: the append reports why it can't be journaled
                pass
        e = Entry(self.run_id, key, kind, result if persisted else None, delta, removed, meta, persisted)
        e._prepared = True  # type: ignore[attr-defined]
        return e

    def _submit(self, e: Entry) -> concurrent.futures.Future:
        """Queue an entry on this run's ordered writer: entries are written in the order
        they were built, one after another, from a worker thread (never through the
        event loop's task factory, so journaling can't disturb branch keys)."""
        prev, journal = self._last_write, self.journal
        out: concurrent.futures.Future = concurrent.futures.Future()

        def write() -> None:
            if not out.set_running_or_notify_cancel():
                return
            try:
                out.set_result(journal.append(e))
            except BaseException as ex:  # noqa: BLE001 -- surfaced to whoever awaits the write
                out.set_exception(ex)

        def go(p: Optional[concurrent.futures.Future] = None) -> None:
            if p is not None and not p.cancelled() and p.exception() is not None:
                # an earlier write failed: don't journal later entries out of order
                if out.set_running_or_notify_cancel():
                    out.set_exception(p.exception())
                return
            try:
                _writers().submit(write)
            except RuntimeError:                     # interpreter shutting down
                write()

        # Chained on completion: no writer thread ever waits for another write, so a
        # slow run can't occupy the pool and stall other runs' writes.
        if prev is None:
            go()
        else:
            prev.add_done_callback(go)
        self._last_write = out
        return out

    def _record(self, key: str, kind: str, result: Any, meta: dict, persisted: bool = True) -> None:
        """Journal an entry from synchronous code (markers). Ordered with every other
        write; the next step waits for it before running (see _barrier)."""
        e = self._entry(key, kind, result, meta, persisted)
        if not self.journal.blocking:
            self.journal.append(e)
        else:
            self._submit(e)

    async def _arecord(self, key: str, kind: str, result: Any, meta: dict, persisted: bool = True) -> None:
        """Journal an entry; blocking journals write from a worker thread so a slow
        disk or database never stalls the event loop. The write finishes even if the
        awaiting task is cancelled, and the run waits for it before it ends."""
        e = self._entry(key, kind, result, meta, persisted)
        if not self.journal.blocking:
            self.journal.append(e)
            return
        fut = asyncio.wrap_future(self._submit(e))
        self._inflight.add(fut)
        fut.add_done_callback(self._inflight.discard)
        await asyncio.shield(fut)

    async def _barrier(self) -> None:
        """Wait until everything queued so far is durable (raises if a write failed)."""
        cf = self._last_write
        if cf is not None:
            await asyncio.shield(asyncio.wrap_future(cf)) if not cf.done() else cf.result()

    async def _settle(self) -> None:
        cf = self._last_write
        if cf is not None and not cf.done():
            await asyncio.wait([asyncio.wrap_future(cf)])
        while self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)

    def _replay(self, key: str) -> Optional[Entry]:
        if key in self._history_keys:
            self._consumed.add(key)
        e = self._cache.get(key)
        if e is not None:
            self._snap = self._snapshot()
        return e

    @property
    def replaying(self) -> bool:
        """True while recorded history remains that the flow has not reached yet."""
        return bool(self._history_keys - self._consumed)

    def _err(self, e: BaseException) -> str:
        return self.journal.redact_error(repr(e))

    # -- steps
    def step(self, fn: Callable, *args: Any, policy: Optional[Policy] = None,
             name: Optional[str] = None, sensitive: bool = False, **kwargs: Any) -> Awaitable[Any]:
        """Run any callable as a checkpointed step. The key is allocated *now*
        (synchronously), so steps launched together with asyncio.gather replay
        deterministically. (Use @step if your function has parameters named
        name/policy/sensitive.)"""
        return self._invoke(fn, args, kwargs, policy, name, sensitive)

    def _invoke(self, fn: Callable, args: tuple, kwargs: dict, policy: Optional[Policy],
                name: Optional[str], sensitive: bool) -> Awaitable[Any]:
        if _step_key.get() is not None:              # called from inside a step: plain call
            async def inline() -> Any:
                res = fn(*args, **kwargs)
                return await res if inspect.isawaitable(res) else res
            return inline()
        key = self._key(name or str(getattr(fn, "__name__", "step")),
                        None if sensitive else (args, kwargs))
        return self._run(key, fn, args, kwargs, policy or Policy(), sensitive)

    async def _run(self, key: str, fn: Callable, args: tuple, kwargs: dict,
                   policy: Policy, sensitive: bool) -> Any:
        cached = self._replay(key)
        if cached is not None:
            self.emit("replayed", key)
            return cached.result
        failed = self._failed.get(key)
        if failed is not None and (key in self._lost or key in self._caught):
            self.emit("replayed", key, failed=True)
            cls = ResultNotJournaled if key in self._lost else ReplayedError
            raise StepFailed(key, int(failed.meta.get("attempts", 1)),
                             cls(str(failed.meta.get("error", "step failed"))))
        await self._barrier()                     # earlier markers are durable before a side effect
        if self._closing:                         # the run already ended (failed/finished): stop here
            raise asyncio.CancelledError()
        me = asyncio.current_task()
        self._in_step[me] = self._in_step.get(me, 0) + 1      # busy until its outcome is journaled
        try:
            return await self._execute_step(key, fn, args, kwargs, policy, sensitive)
        finally:
            left = self._in_step.get(me, 1) - 1
            if left:
                self._in_step[me] = left
            else:
                self._in_step.pop(me, None)

    async def _execute_step(self, key: str, fn: Callable, args: tuple, kwargs: dict,
                            policy: Policy, sensitive: bool) -> Any:
        self.emit("started", key)
        ident = f"{self.run_id}:{key}"
        token: Optional[contextvars.Token] = _step_key.set(ident)
        try:
            result, meta = await run_with_policy(key, fn, args, kwargs, policy, self.emit,
                                                 f"{self._incarnation}|{key}")
        except PriorAttemptRunning as p:
            # an earlier execution's attempt of this step is still running: its outcome is
            # unknown, so neither fail the step nor run it again -- come back for its result
            self.emit("attempt_pending", key, retry_at=p.retry_at)
            raise Suspended(None, p.retry_at) from None
        except StepFailed as e:
            if token is not None:
                _step_key.reset(token)
                token = None
            await self._arecord(key, "failed", None, {"error": self._err(e.error), "attempts": e.attempts})
            self._pending_failures[key] = self._scope_now().prefix
            self.emit("step_failed", key, error=repr(e.error)[:500], attempts=e.attempts)
            raise
        except BaseException as e:
            self.emit("step_crashed", key, error=repr(e)[:500])
            raise
        finally:
            if token is not None:
                _step_key.reset(token)
        if not sensitive:
            try:
                self.journal.codec.dumps(result)
            except Exception as ex:  # noqa: BLE001 -- ran, but can't be recorded: never run it again
                err = ResultNotJournaled(f"step {key!r} ran but its {type(result).__name__} result can't be "
                                         f"journaled ({ex}); it won't be re-run on resume. Register the type "
                                         f"with pylinxi.default_codec.register().")
                await self._arecord(key, "failed", None, {"error": self._err(err), "attempts": meta["attempts"],
                                                          "lost_result": True})
                self._lost.add(key)
                self.emit("step_failed", key, error=repr(err)[:500], attempts=meta["attempts"])
                raise StepFailed(key, meta["attempts"], err) from ex
        await self._arecord(key, "step", result, meta, persisted=not sensitive)
        self.emit("completed", key, **{k: v for k, v in meta.items() if k != "errors"})
        return result

    async def now(self) -> float:
        """Deterministic wall clock: recorded on first run, identical on replay."""
        return float(await self._invoke(time.time, (), {}, Policy(thread=False), "pylinxi.now", False))

    async def uuid(self) -> str:
        """Deterministic random id: recorded on first run, identical on replay."""
        return str(await self._invoke(lambda: uuid.uuid4().hex, (), {}, Policy(thread=False), "pylinxi.uuid", False))

    # -- time and signals
    async def _timer(self, key: str, seconds: float) -> float:
        """Arm a durable timer. Armed timers count for the run's wake-up even if their
        branch is cancelled before it can suspend (e.g. by a TaskGroup)."""
        cached = self._replay(key)
        if cached is not None:
            self._armed[key] = float(cached.result["wake_at"])
            return self._armed[key]
        wake_at = time.time() + seconds
        self._armed[key] = wake_at
        with self._busy_in():
            await self._arecord(key, "timer", {"wake_at": wake_at}, {"seconds": seconds})
        return wake_at

    async def sleep(self, seconds: float, inline_max: float = 1.0) -> None:
        """Durable sleep. Short waits (<= inline_max s) sleep in-process; longer ones
        suspend the run and free the worker. A Worker (or journal.pending_timers())
        resumes it once due; resuming early simply suspends again."""
        key = self._key("sleep")
        wake_at = await self._timer(key, seconds)
        remaining = wake_at - time.time()
        if remaining <= 0:
            self._armed.pop(key, None)
            return
        if remaining <= inline_max:
            with self._busy_in():
                await asyncio.sleep(remaining)
            self._armed.pop(key, None)
            return
        await self._quiesce()
        raise Suspended(None, wake_at)

    def _take_signal(self, signal: str) -> Tuple[bool, Any]:
        used = self._signals_used.get(signal, 0)
        queue = self._inbox.get(signal, [])
        if used < len(queue):
            self._signals_used[signal] = used + 1
            return True, queue[used]
        return False, None

    async def _refresh_inbox(self) -> None:
        """Pick up signals delivered (journal.send_signal) while this run executes."""
        entries = (await asyncio.to_thread(self.journal.load, self.run_id) if self.journal.blocking
                   else self.journal.load(self.run_id))
        for e in entries:
            if e.kind == "inbox":
                self._absorb(e)

    async def wait_for(self, signal: str, timeout: Optional[float] = None, default: Any = None) -> Any:
        """Wait for `signal`, delivered via flow.run/resume(..., signals={signal: value})
        or journal.send_signal(run_id, signal, value).

        Signals are journaled when delivered, so one sent before the flow reaches
        this point is not lost; repeated deliveries queue up in order.
        With timeout (seconds), returns `default` if nothing arrives by then."""
        key = self._key(f"signal:{signal}")
        tkey = self._key(f"timeout:{signal}") if timeout is not None else None
        cached = self._replay(key)
        if cached is not None:
            if tkey:
                self._replay(tkey)
            return cached.result
        got, value = self._take_signal(signal)
        if not got:
            self._awaited.append(signal)          # counts for the suspension even if cancelled now
            with self._busy_in():
                await self._refresh_inbox()
            got, value = self._take_signal(signal)
            if got:
                self._awaited.remove(signal)
        if got:
            if tkey:
                self._replay(tkey)
                self._armed.pop(tkey, None)
            await self._arecord(key, "signal", value, {"signal": signal})
            return value
        if tkey is None:
            await self._quiesce()
            raise Suspended(signal)
        wake_at = await self._timer(tkey, timeout)  # type: ignore[arg-type]
        if time.time() >= wake_at:
            self._armed.pop(tkey, None)
            await self._arecord(key, "signal", default, {"signal": signal, "timed_out": True})
            self.emit("signal_timeout", key)
            return default
        await self._quiesce()
        raise Suspended(signal, wake_at)

    def _unused_signals(self) -> Dict[str, List[Any]]:
        return {n: q[self._signals_used.get(n, 0):] for n, q in self._inbox.items()
                if len(q) > self._signals_used.get(n, 0)}

    # -- evolution
    def patched(self, change_id: str) -> bool:
        """Safely change flow code while runs are in flight.

        Returns True for runs that reach this point on the new code, False for runs
        that were already past it (they keep their old behaviour on replay):

        ```python
        if ctx.patched("v2-extra-check"):
            await new_check()
        ```
        """
        key = f"patch:{change_id}"
        self._note_progress(self._scope_now().prefix)
        if key in self._history_keys or key in self._cache:
            self._replay(key)
            return True
        if self.replaying:          # run started before this code existed
            return False
        self._record(key, "marker", True, {})
        self._cache[key] = Entry(self.run_id, key, "marker", True)
        return True

    def continue_as_new(self, *args: Any, **kwargs: Any) -> None:
        """End this run and continue in a fresh journal with new inputs and the current
        state. Use in long-lived loops (chat sessions, monitors) to keep replay short.
        Inputs and state are journaled in clear for the next generation, so they may
        not contain secret-looking keys (checked here, at the call)."""
        self._note_progress(self._scope_now().prefix)
        state = dict(self.state)
        found = find_secret_keys(self.journal.codec.encode({"args": list(args), "kwargs": kwargs, "state": state}),
                                 self.journal.redact_pattern)
        if found:
            raise ValueError(f"continue_as_new inputs/state contain secret-looking keys {found}; they would be "
                             f"journaled in clear. Keep secrets out of ctx.state (load them in a "
                             f"@step(sensitive=True)), or configure the journal's redact_pattern.")
        raise ContinueAsNew(args, kwargs, state)

    # -- composition
    async def route(self, label: Any, routes: Mapping[Any, Callable], *args: Any,
                    default: Optional[Callable] = None, **kwargs: Any) -> Any:
        """Dispatch to a step by label -- a one-liner replacement for conditional edges."""
        fn = routes.get(label, default)
        if fn is None:
            raise KeyError(f"no route for {label!r}; known: {list(routes)}")
        if getattr(fn, "__pylinxi_step__", False):
            return await fn(*args, **kwargs)
        return await self._invoke(fn, args, kwargs, None, None, False)

    async def map(self, fn: Callable, items: Iterable[Any], concurrency: int = 8) -> List[Any]:
        """Fan out fn over items in parallel (bounded), each item its own checkpoint."""
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError(f"concurrency must be a positive integer, got {concurrency!r}")
        sem = asyncio.Semaphore(concurrency)
        is_step = getattr(fn, "__pylinxi_step__", False)
        calls = [fn(x) if is_step else self._invoke(fn, (x,), {}, None, None, False) for x in items]

        async def bounded(c: Awaitable[Any]) -> Any:
            async with sem:
                return await c
        # Let siblings finish (and checkpoint) before surfacing the first error,
        # so a resume only re-runs the items that actually failed.
        results = await asyncio.gather(*(bounded(c) for c in calls), return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException):
                raise r
        return list(results)


# ---------------------------------------------------------------- flow
@dataclass
class RunResult:
    """Outcome of run()/resume().

    state -- the flow's live state when this call executed the flow; when the run had
             already completed earlier, it is rebuilt from the journal instead (secret
             keys redacted) and `from_journal` is True.
    """
    run_id: str
    status: str                       # completed | suspended | failed
    output: Any = None
    state: dict = field(default_factory=dict)
    waiting_for: Optional[str] = None  # first signal waited on, if any
    wake_at: Optional[float] = None    # unix time the earliest timer is due, if any
    error: Optional[BaseException] = None
    events: List[dict] = field(default_factory=list)
    waiting_for_all: List[str] = field(default_factory=list)   # every signal waited on
    dropped_signals: List[str] = field(default_factory=list)   # signals sent to an already-completed run
    from_journal: bool = False


@dataclass
class _Next:
    """Internal: hop to the next run in a continue-as-new chain."""
    run_id: str
    args: tuple
    kwargs: dict
    state: dict
    generation: int
    root: str
    signals: Optional[Dict[str, List[Any]]]     # name -> queued values (delivered by the caller)
    carry: Optional[Dict[str, List[Any]]] = None   # unconsumed signals of the previous generation
    explicit: bool = False                      # caller passed inputs (only for the first hop)


_AGAIN = object()   # internal: a waited-for signal is already queued -- execute once more


class Flow:
    def __init__(self, fn: Callable[..., Awaitable[Any]], journal: Optional[Journal] = None,
                 on_event: Union[None, Observer, List[Observer]] = None,
                 strict: bool = True, drain_timeout: float = 10.0, name: Optional[str] = None):
        """name          -- the flow's identity in the journal (default: the function's
                            name). Runs belong to one flow; set a stable name if you
                            might rename the function.
        strict        -- stop with ReplayDivergence before running a new step where
                            the journal recorded a different one (default). False only
                            reports a `divergence` event.
        drain_timeout -- when one branch suspends, how long (seconds) other branches
                         may keep running to their own suspension point or end."""
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("@flow functions must be async def")
        self.fn, self.journal = fn, journal or MemoryJournal()
        self.name: str = name or fn.__name__
        self.strict, self.drain_timeout = strict, drain_timeout
        self.observers: List[Observer] = (
            [] if on_event is None else list(on_event) if isinstance(on_event, (list, tuple)) else [on_event])
        functools.update_wrapper(cast(Callable[..., Any], self), fn)

    def _reject_secrets(self, what: str, **values: Any) -> None:
        if self.journal.redact_pattern is None:
            return
        found = find_secret_keys(self.journal.codec.encode(values), self.journal.redact_pattern)
        if found:
            raise ValueError(
                f"{what} contain secret-looking keys {found}. Inputs are journaled so runs can "
                f"resume; pass secrets by reference and load them in a @step(sensitive=True), "
                f"or configure the journal with redact_pattern=None.")

    async def _io(self, fn: Callable, *args: Any) -> Any:
        if self.journal.blocking:
            return await asyncio.to_thread(fn, *args)
        return fn(*args)

    async def _enter(self, cm: Any) -> None:
        """Acquire a run lock off the event loop. If we're cancelled while the thread is
        still acquiring it, release it as soon as it's taken -- never leak the lock."""
        if not self.journal.blocking:
            cm.__enter__()
            return
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(None, cm.__enter__)
        try:
            await asyncio.shield(fut)
        except asyncio.CancelledError:
            def release(f: asyncio.Future) -> None:
                if not f.cancelled() and f.exception() is None:
                    loop.run_in_executor(None, cm.__exit__, None, None, None)
            fut.add_done_callback(release)
            raise

    async def run(self, *args: Any, run_id: Optional[str] = None, state: Optional[dict] = None,
                  signals: Optional[Mapping[str, Any]] = None,
                  on_event: Optional[Observer] = None, _must_exist: bool = False,
                  **kwargs: Any) -> RunResult:
        """Start a run, or continue it if `run_id` already has history.

        For an existing run, inputs are taken from the journal: passing different
        args/kwargs/state raises RunExists, and a run id that belongs to another flow
        raises FlowMismatch. Use resume() to continue a run that must already exist."""
        _install_task_factory(asyncio.get_running_loop())
        observers = self.observers + ([on_event] if on_event else [])
        events: List[dict] = []
        rid = run_id or uuid.uuid4().hex
        queued = {k: [v] for k, v in signals.items()} if signals else None
        if queued:
            self._reject_secrets("signals", signals=queued)
            self.journal.codec.dumps(queued)               # unjournalable signal: fail before anything runs
        explicit = bool(args or kwargs or state is not None)
        nxt: Any = _Next(rid, args, kwargs, dict(state or {}), 0, rid, queued, explicit=explicit)
        visited: Set[str] = set()
        again = 0
        pending: Optional[RunResult] = None
        token = _step_key.set(None)                       # a flow started inside a step is its own run
        try:
            while True:
                def emit(event: str, key: str = "", _rid: str = nxt.run_id, **data: Any) -> None:
                    ev = {"event": event, "key": key, "run_id": _rid, "flow": self.name, **data}
                    events.append(ev)
                    for obs in observers:
                        try:
                            obs(ev)
                        except Exception:  # noqa: BLE001 -- observability must never break a run
                            pass

                cm = self.journal.lock(nxt.run_id)         # RunLocked if another worker owns it
                try:
                    await self._enter(cm)                  # taking a lock may do I/O: keep it off the loop
                except RunLocked:
                    if pending is not None:                # someone else took over after our suspension
                        return pending
                    raise
                try:
                    out = await self._execute(nxt, events, emit, must_exist=_must_exist and not visited)
                except BaseException:
                    if not await self._io(cm.__exit__, *sys.exc_info()):
                        raise
                    raise                                  # pragma: no cover - lock never suppresses
                else:
                    await self._io(cm.__exit__, None, None, None)
                if isinstance(out, tuple) and out[0] is _AGAIN:
                    pending = out[1]
                    if again >= 5:                         # signals keep racing in: report as suspended
                        return pending
                    again += 1
                    nxt = _Next(nxt.run_id, (), {}, {}, nxt.generation, nxt.root, None)
                    continue
                pending = None
                if isinstance(out, RunResult):
                    return out
                visited.add(nxt.run_id)
                if out.run_id in visited:
                    raise RuntimeError(f"continue-as-new chain loops back to {out.run_id!r}")
                nxt = out
        finally:
            _step_key.reset(token)

    def _latest(self, root: str, current: str) -> Optional[str]:
        """Newest generation, from the root's newest link entry: one row read,
        however long the chain."""
        row = self.journal.last_row(root)
        try:
            raw = json.loads(row[0]) if row else {}
        except ValueError:
            return None
        res = raw.get("result") if raw.get("kind") == "link" else None
        latest = res.get("latest") if isinstance(res, dict) else None
        return latest if isinstance(latest, str) and latest != current and self.journal.exists(latest) else None

    async def _execute(self, n: _Next, events: List[dict], emit: Callable[..., None],
                       must_exist: bool = False) -> Any:
        run_id = n.run_id
        if not n.explicit:
            latest = await self._io(self._latest, run_id, run_id)
            if latest is not None:                      # a long-finished root: skip straight to the newest
                return _Next(latest, (), {}, {}, 0, run_id, n.signals)
        history = await self._io(self.journal.load, run_id)
        start = next((e for e in history if e.kind == "start"), None)
        if start is not None and start.result.get("flow", self.name) != self.name:
            raise FlowMismatch(f"run {run_id!r} belongs to flow {start.result.get('flow')!r}, not {self.name!r}")
        if start is None and must_exist:
            raise UnknownRun(f"run {run_id!r} does not exist (never started, or deleted)")
        if start is not None and n.explicit:
            codec = self.journal.codec
            given = codec.dumps({"args": list(n.args), "kwargs": n.kwargs, "state": n.state})
            stored = codec.dumps({"args": start.result["args"], "kwargs": start.result["kwargs"],
                                  "state": start.result.get("state", {})})
            if given != stored:
                raise RunExists(f"run {run_id!r} already exists with different inputs; use resume({run_id!r}) "
                                f"to continue it, or a new run id")

        done = next((e for e in history if e.kind == "done"), None)
        if done is not None:
            nxt_id = done.meta.get("continued_as")
            if nxt_id:
                root = start.result.get("root", run_id) if start is not None else run_id
                latest = self._latest(root, run_id)
                if latest is not None and latest != nxt_id:
                    return _Next(latest, (), {}, {}, 0, root, n.signals)
                c = done.result
                return _Next(nxt_id, tuple(c["args"]), c["kwargs"], c["state"], c["generation"], c["root"],
                             n.signals, carry=c.get("signals") or None)
            dropped = sorted(n.signals or {})
            if dropped:
                emit("signals_dropped", signals=dropped)
                log.warning("run %r already completed; signals %s were not delivered", run_id, dropped)
            state = await self._io(self.journal.state_at, run_id)
            return RunResult(run_id, "completed", done.result, state, events=events,
                             dropped_signals=dropped, from_journal=True)

        if start is None:
            self._reject_secrets("flow inputs", args=list(n.args), kwargs=n.kwargs, state=n.state)
            start = Entry(run_id, "start", "start",
                          {"args": list(n.args), "kwargs": n.kwargs, "state": n.state, "generation": n.generation,
                           "root": n.root, "flow": self.name, "py": f"{sys.version_info[0]}.{sys.version_info[1]}"},
                          dict(n.state))
            await self._io(self.journal.append, start)
            history = [start]
            if n.generation > 0:                          # root -> newest generation pointer
                try:
                    await self._io(self.journal.append, Entry(n.root, "link", "link",
                                                              {"latest": run_id, "generation": n.generation}))
                except UnknownRun:                        # root deleted: chain still works by hops
                    pass
            inbox: Dict[str, List[Any]] = {}
            for src in (n.carry, n.signals):
                for k, vals in (src or {}).items():
                    inbox.setdefault(k, []).extend(vals)
            if inbox:
                history.append(await self._io(self.journal.append, Entry(run_id, "inbox", "inbox", inbox)))
            emit("run_started", generation=n.generation)
        else:
            if n.signals:
                history.append(await self._io(self.journal.append, Entry(run_id, "inbox", "inbox",
                                                                         dict(n.signals))))
            emit("run_started", resumed=True, completed=sum(1 for e in history if e.kind == "step"))
        s = start.result
        args, kwargs = tuple(s["args"]), s["kwargs"]
        generation, root = int(s.get("generation", 0)), s.get("root", run_id)
        if generation > 0:
            fwd = await self._io(self._forward_signals, run_id, root, generation, history)
            if fwd is not None:
                history.append(fwd)

        ctx = Context(run_id, self.journal, dict(s.get("state", {})), history, emit, generation, self.strict)
        ctx.drain_timeout = self.drain_timeout
        ctx._root_task = asyncio.current_task()
        token, scope_token = _current.set(ctx), _scope.set(ctx._root)
        try:
            try:
                try:
                    output = await self.fn(ctx, *args, **kwargs)
                    ctx._note_progress("")                # returning normally = failures were handled
                except BaseException as g:
                    if _BaseExceptionGroup is not None and isinstance(g, _BaseExceptionGroup):
                        raise _unwrap_group(g) from g     # TaskGroup: suspensions/failures inside it
                    raise
            except Suspended:
                await self._drain(ctx)
                raise
            finally:
                ending = sys.exc_info()[1]
                try:
                    if not isinstance(ending, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                        await self._finish_steps(ctx)
                finally:
                    try:
                        await self._cancel_branches(ctx)
                    finally:
                        # even if the caller cancels us here, nothing may journal after we return
                        settle = asyncio.ensure_future(ctx._settle())
                        try:
                            await asyncio.shield(settle)
                        except asyncio.CancelledError:
                            await asyncio.wait([settle])
                            raise
        except Suspended as sp:
            if ctx._halt is not None:
                return await self._fail(ctx, ctx._halt, emit, events)
            return await self._suspend(ctx, sp, emit, events)
        except ContinueAsNew as c:
            if ctx._halt is not None:
                return await self._fail(ctx, ctx._halt, emit, events)
            new_id = f"{root}.g{generation + 1}"
            if new_id == run_id:  # pragma: no cover - defensive
                raise RuntimeError("continue-as-new id collision") from None
            leftover = ctx._unused_signals()
            try:
                await ctx._arecord("done", "done", {"args": list(c.args_), "kwargs": c.kwargs, "state": c.state,
                                                    "generation": generation + 1, "root": root,
                                                    "signals": leftover},
                                    {"continued_as": new_id, "inbox_seen": sorted(ctx._inbox_seqs)})
            except Exception as e:  # noqa: BLE001
                return await self._fail(ctx, e, emit, events)
            emit("continued_as_new", next_run_id=new_id)
            return _Next(new_id, c.args_, c.kwargs, c.state, generation + 1, root, None, carry=leftover or None)
        except ReplayDivergence as d:
            return await self._fail(ctx, d, emit, events)
        except Exception as e:  # noqa: BLE001
            return await self._fail(ctx, e, emit, events)
        except BaseException as e:               # crash/cancel: close spans, then propagate
            emit("crashed", error=repr(e)[:500])
            raise
        finally:
            ctx._active = False
            _scope.reset(scope_token)
            _current.reset(token)

        if ctx._halt is not None:                 # a divergence was swallowed by flow code
            return await self._fail(ctx, ctx._halt, emit, events)
        ignored = [t for t in ctx._finished_tasks if not t.cancelled() and isinstance(t.exception(), Suspended)]
        if ignored:
            # e.g. asyncio.gather(..., return_exceptions=True) around a durable wait: the run
            # completes without having waited
            emit("suspension_ignored", count=len(ignored))
            log.warning("run %r completed although %d branch(es) suspended and flow code ignored it "
                        "(asyncio.gather(..., return_exceptions=True) around ctx.wait_for/ctx.sleep?)",
                        ctx.run_id, len(ignored))
        stale = ctx._history_keys - ctx._consumed
        if stale:
            emit("divergence", stale=sorted(stale))   # recorded steps the new code never reached
        try:
            await ctx._arecord("done", "done", output, {})
        except Exception as e:  # noqa: BLE001 -- e.g. an output the codec can't journal
            return await self._fail(ctx, e, emit, events)
        emit("finished")
        return RunResult(run_id, "completed", output, ctx.state, events=events)

    def _forward_signals(self, run_id: str, root: str, generation: int, history: List[Entry]) -> Optional[Entry]:
        """Signals that reached the previous generation while it was handing over (sent
        between its last inbox read and its `done` entry, or just after it) would be
        stranded there: move them into this generation's inbox, exactly once. Runs while
        this generation holds its run lock; forwarded entries are recorded by sequence."""
        prev = root if generation == 1 else f"{root}.g{generation - 1}"
        try:
            prev_entries = self.journal.load(prev)
        except UnknownRun:
            return None
        done = next((e for e in prev_entries if e.kind == "done" and e.meta.get("continued_as") == run_id), None)
        if done is None:
            return None
        seen = done.meta.get("inbox_seen")
        already = {q for e in history if e.kind == "inbox" and e.meta.get("forwarded_from") == prev
                   for q in e.meta.get("seqs", [])}
        missed = [e for e in prev_entries if e.kind == "inbox" and e.seq not in already
                  and (e.seq > done.seq or (isinstance(seen, list) and e.seq not in seen))]
        if not missed:
            return None
        inbox: Dict[str, List[Any]] = {}
        for e in missed:
            for k, vals in (e.result or {}).items():
                inbox.setdefault(k, []).extend(vals)
        return self.journal.append(Entry(run_id, "inbox", "inbox", inbox, {}, [],
                                         {"forwarded_from": prev, "seqs": [e.seq for e in missed]}))

    async def _suspend(self, ctx: Context, sp: Suspended, emit: Callable[..., None],
                       events: List[dict]) -> Any:
        """Record the suspension at the earliest deadline among all branches."""
        await asyncio.sleep(0)                  # let done-callbacks of just-finished branches run
        done_tasks = list(ctx._finished_tasks) + [t for t in ctx._tasks if t.done() and not t.cancelled()]
        found: List[BaseException] = [sp] + list(getattr(sp, "parts", []))
        for t in done_tasks:
            x = None if t.cancelled() else t.exception()
            if isinstance(x, Suspended) and all(x is not y for y in found):
                found.append(x)
        signals: List[str] = []
        wakes: List[float] = []
        for x in found:
            if isinstance(x, Suspended):
                if x.signal and x.signal not in signals:
                    signals.append(x.signal)
                if x.wake_at is not None:
                    wakes.append(x.wake_at)
        wakes += list(ctx._armed.values())     # timers armed by branches that were cancelled first
        if ctx._cut_short:                     # a sibling was still mid-step (TaskGroup may have cut it):
            wakes.append(time.time() + 1.0)    # resume soon so it gets there and its deadline counts
        for name in ctx._awaited:              # ...and signals they were waiting for
            if name not in signals:
                signals.append(name)
        wake_at = min(wakes) if wakes else None
        before = set(ctx._inbox_seqs)
        await ctx._arecord("suspend", "suspend", {"signal": signals[0] if signals else None, "signals": signals,
                                                  "wake_at": wake_at}, {})
        await ctx._settle()
        await ctx._refresh_inbox()
        result = RunResult(ctx.run_id, "suspended", None, ctx.state, signals[0] if signals else None, wake_at,
                           events=events, waiting_for_all=signals)
        # A signal being waited for is already queued -- it arrived while suspending, or a
        # sibling branch picked it up during the drain: execute again instead of sleeping on it.
        if (ctx._inbox_seqs - before) or set(signals) & set(ctx._unused_signals()):
            return (_AGAIN, result)
        emit("suspended", waiting_for=signals[0] if signals else None, wake_at=wake_at, signals=signals)
        return result

    async def _fail(self, ctx: Context, e: BaseException, emit: Callable[..., None], events: List[dict]) -> RunResult:
        ctx._uncatch(_step_failures(e))        # the run ended in these failures: they weren't handled
        meta: Dict[str, Any] = {"error": ctx._err(e)}
        if isinstance(e, StepFailed):
            meta["step"] = e.key
        if isinstance(e, ReplayDivergence):
            meta["divergence"] = True
        try:
            await ctx._arecord("flow", "failed", None, meta)
        except Exception:  # noqa: BLE001 -- state itself may be unserializable
            await self._io(self.journal.append, Entry(ctx.run_id, "flow", "failed", None, {}, [], meta))
        emit("failed", error=repr(e)[:500])
        return RunResult(ctx.run_id, "failed", None, ctx.state, error=e, events=events)

    async def _drain(self, ctx: Context) -> None:
        """One branch suspended: let the others run on (bounded) to their own
        suspension point or end, so their progress is saved and their deadlines seen."""
        pending = [t for t in ctx._tasks if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=ctx._remaining())

    async def _finish_steps(self, ctx: Context) -> None:
        """The run is ending while branches still run. Steps already executing have
        (or will have) done their side effect: let them finish and journal their
        result, so a resume replays them instead of running them again. No branch may
        start a new step from here on, so this only ever waits for steps already
        running: they get their own grace period (drain_timeout), even when draining
        used up the shared budget."""
        ctx._closing = True
        busy = [t for t in ctx._tasks if not t.done() and t in ctx._in_step]
        if busy:
            await asyncio.wait(busy, timeout=max(ctx._remaining(), self.drain_timeout))

    @staticmethod
    async def _cancel_branches(ctx: Context) -> None:
        """Cancel branches still running so they can't journal after the run returns."""
        ctx._active = False
        pending = [t for t in ctx._tasks if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for t in ctx._finished_tasks:          # silence "exception was never retrieved" warnings
            if not t.cancelled():
                t.exception()

    async def resume(self, run_id: str, signals: Optional[Mapping[str, Any]] = None,
                     on_event: Optional[Observer] = None) -> RunResult:
        """Continue an existing run. Raises UnknownRun if `run_id` doesn't exist."""
        return await self.run(run_id=run_id, signals=signals, on_event=on_event, _must_exist=True)

    async def signal(self, run_id: str, name: str, value: Any = True, resume: bool = True) -> Optional[RunResult]:
        """Deliver a signal durably. Safe while the run is executing elsewhere: the
        signal is journaled first, then (resume=True) the run is resumed if nobody
        else is executing it. Returns the resume result, or None if it was busy."""
        target = await self._io(self.journal.send_signal, run_id, name, value)
        if not resume:
            return None
        try:
            return await self.resume(target)
        except RunLocked:
            return None          # the executing worker picks the signal up itself

    async def stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[dict]:
        """Run (or resume, with run_id=) and yield events live -- including "chunk"
        events from streaming steps. The last item is {"event": "result", "result": RunResult}.

            async for ev in agent.stream("hi", run_id="chat-1"):
                if ev["event"] == "chunk": await websocket.send(ev["chunk"])
        """
        q: asyncio.Queue = asyncio.Queue()
        task = asyncio.ensure_future(self.run(*args, on_event=q.put_nowait, **kwargs))
        try:
            while True:
                getter = asyncio.ensure_future(q.get())
                done, _ = await asyncio.wait({getter, task}, return_when=asyncio.FIRST_COMPLETED)
                if getter in done:
                    yield getter.result()
                    continue
                getter.cancel()
                break
            while not q.empty():
                yield q.get_nowait()
            yield {"event": "result", "result": task.result()}
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    def run_sync(self, *args: Any, **kwargs: Any) -> RunResult:
        return asyncio.run(self.run(*args, **kwargs))


def flow(fn: Optional[Callable] = None, *, journal: Optional[Journal] = None,
         on_event: Union[None, Observer, List[Observer]] = None,
         strict: bool = True, drain_timeout: float = 10.0, name: Optional[str] = None) -> Any:
    """Decorate an `async def f(ctx, ...)` to make it a durable flow."""
    def deco(f: Callable) -> Flow:
        return Flow(f, journal, on_event, strict=strict, drain_timeout=drain_timeout, name=name)
    return deco(fn) if fn is not None else deco
