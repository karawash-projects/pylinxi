"""Durable flows written as plain Python.

Branching is ordinary `if`/`match`/loops; there is no graph to wire. Every call
to a @step inside a running flow is journaled, so a crashed or suspended run
resumes by replaying the flow function: completed steps return their recorded
result instantly, and execution continues at the first unfinished step.

Rule: flow code (outside steps) must be deterministic -- put I/O, randomness,
clocks and LLM calls inside steps (or use ctx.now() / ctx.uuid()).

Concurrency: tasks created by flow code (asyncio.gather, create_task, TaskGroup)
each get their own key scope ("t0/", "t1/", ...) assigned in creation order, so
steps inside concurrent branches replay deterministically regardless of which
branch finishes first. When a run suspends, fails or finishes, any branch still
running is cancelled before the run returns.
"""
from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import time
import uuid
import weakref
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from .journal import Entry, Journal, MemoryJournal, find_secret_keys
from .policy import Policy, StepFailed, run_with_policy

Observer = Callable[[dict], None]

_current: contextvars.ContextVar[Optional[Context]] = contextvars.ContextVar("pylinxi_ctx", default=None)
_step_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("pylinxi_step", default=None)
_scope: contextvars.ContextVar[Optional[_Scope]] = contextvars.ContextVar("pylinxi_scope", default=None)

# kinds whose keys flow code reaches again on replay
_KEYED = ("step", "failed", "signal", "timer", "marker")


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


class ReplayedError(Exception):
    """Stands in for the original exception when a caught step failure is replayed."""


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
    __slots__ = ("prefix", "ctx", "counters", "children")

    def __init__(self, prefix: str, ctx: Context):
        self.prefix, self.ctx = prefix, ctx
        self.counters: Dict[str, int] = {}
        self.children = 0

    def child(self) -> _Scope:
        n = self.children
        self.children += 1
        return _Scope(f"{self.prefix}t{n}/", self.ctx)


_factory_loops: weakref.WeakSet = weakref.WeakSet()


def _install_task_factory(loop: asyncio.AbstractEventLoop) -> None:
    """Give every task created by *flow code* a child key scope, and track it so the
    run can cancel stragglers. Tasks created inside steps or outside flows are untouched."""
    try:
        if loop in _factory_loops:
            return
    except TypeError:  # pragma: no cover - loop not weak-referenceable
        pass
    prev = loop.get_task_factory()

    def factory(lp: asyncio.AbstractEventLoop, coro: Any, **kw: Any) -> asyncio.Future:
        parent = _scope.get()

        def make() -> asyncio.Future:
            if prev is not None:
                return prev(lp, coro, **kw)
            return asyncio.Task(coro, loop=lp, **kw)

        if parent is None or not parent.ctx._active or _step_key.get() is not None:
            return make()
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
        parent.ctx._tasks.add(task)
        return task

    loop.set_task_factory(factory)
    try:
        _factory_loops.add(loop)
    except TypeError:  # pragma: no cover
        pass


# ---------------------------------------------------------------- context
class Context:
    """Handed to every flow as its first argument."""

    def __init__(self, run_id: str, journal: Journal, state: dict, history: List[Entry],
                 emit: Callable[..., None], generation: int = 0):
        self.run_id, self.journal, self.state = run_id, journal, state
        self.generation = generation
        self.emit = emit
        self._root = _Scope("", self)
        self._active = True
        self._tasks: Set[asyncio.Future] = set()

        self._cache: Dict[str, Entry] = {}       # key -> successful outcome (persisted)
        self._failed: Dict[str, Entry] = {}      # key -> last failure with no later success
        self._history_keys: Set[str] = set()     # every keyed entry the flow will reach again
        self._consumed: Set[str] = set()
        self._pending_failures: Dict[str, str] = {}   # key -> scope prefix where it was raised
        self._inbox: Dict[str, List[Any]] = {}
        self._signals_used: Dict[str, int] = {}
        for e in history:
            if e.kind == "inbox":
                for name, vals in e.result.items():
                    self._inbox.setdefault(name, []).extend(vals)
                continue
            if e.kind not in _KEYED:
                continue
            self._history_keys.add(e.key)
            if e.kind == "failed":
                self._failed[e.key] = e
            else:
                self._failed.pop(e.key, None)
                if e.persisted:
                    self._cache[e.key] = e
            if e.kind == "signal" and not e.meta.get("timed_out"):
                n = e.meta.get("signal", "")
                self._signals_used[n] = self._signals_used.get(n, 0) + 1
        self._snap = self._snapshot()

    # -- internals
    def _scope_now(self) -> _Scope:
        s = _scope.get()
        return s if s is not None and s.ctx is self else self._root

    def _key(self, name: str) -> str:
        scope = self._scope_now()
        self._note_progress(scope.prefix)
        n = scope.counters.get(name, 0)
        scope.counters[name] = n + 1
        return f"{scope.prefix}{name}#{n}"

    def _note_progress(self, prefix: str) -> None:
        """Flow code in `prefix` kept going, so failures raised in it (or in branches
        below it) were caught: journal that, so replay re-raises instead of re-running."""
        for k, raised_in in list(self._pending_failures.items()):
            if raised_in.startswith(prefix):
                del self._pending_failures[k]
                self._record(f"caught:{k}", "marker", True, {})

    def _snapshot(self) -> Dict[str, str]:
        dumps = self.journal.codec.dumps
        return {k: dumps(v) for k, v in self.state.items()}

    def _diff(self) -> Tuple[dict, list]:
        new = self._snapshot()
        delta = {k: self.state[k] for k, v in new.items() if self._snap.get(k) != v}
        removed = [k for k in self._snap if k not in new]
        self._snap = new
        return delta, removed

    def _record(self, key: str, kind: str, result: Any, meta: dict, persisted: bool = True) -> None:
        delta, removed = self._diff()
        self.journal.append(Entry(self.run_id, key, kind, result if persisted else None,
                                  delta, removed, meta, persisted))

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
        key = self._key(name or str(getattr(fn, "__name__", "step")))
        return self._run(key, fn, args, kwargs, policy or Policy(), sensitive)

    async def _run(self, key: str, fn: Callable, args: tuple, kwargs: dict,
                   policy: Policy, sensitive: bool) -> Any:
        cached = self._replay(key)
        if cached is not None:
            self.emit("replayed", key)
            return cached.result
        failed = self._failed.get(key)
        if failed is not None and f"caught:{key}" in self._history_keys:
            self._consumed.add(f"caught:{key}")       # flow handled this failure last time
            self.emit("replayed", key, failed=True)
            raise StepFailed(key, int(failed.meta.get("attempts", 1)),
                             ReplayedError(str(failed.meta.get("error", "step failed"))))
        self.emit("started", key)
        token: Optional[contextvars.Token] = _step_key.set(f"{self.run_id}:{key}")
        try:
            result, meta = await run_with_policy(key, fn, args, kwargs, policy, self.emit)
        except StepFailed as e:
            if token is not None:
                _step_key.reset(token)
                token = None
            self._record(key, "failed", None, {"error": repr(e.error), "attempts": e.attempts})
            self._pending_failures[key] = self._scope_now().prefix
            self.emit("step_failed", key, error=repr(e.error), attempts=e.attempts)
            raise
        except BaseException as e:
            self.emit("step_crashed", key, error=repr(e))
            raise
        finally:
            if token is not None:
                _step_key.reset(token)
        self._record(key, "step", result, meta, persisted=not sensitive)
        self.emit("completed", key, **{k: v for k, v in meta.items() if k != "errors"})
        return result

    async def now(self) -> float:
        """Deterministic wall clock: recorded on first run, identical on replay."""
        return float(await self._invoke(time.time, (), {}, Policy(thread=False), "pylinxi.now", False))

    async def uuid(self) -> str:
        """Deterministic random id: recorded on first run, identical on replay."""
        return str(await self._invoke(lambda: uuid.uuid4().hex, (), {}, Policy(thread=False), "pylinxi.uuid", False))

    # -- time and signals
    def _timer(self, key: str, seconds: float) -> float:
        cached = self._replay(key)
        if cached is not None:
            return float(cached.result["wake_at"])
        wake_at = time.time() + seconds
        self._record(key, "timer", {"wake_at": wake_at}, {"seconds": seconds})
        return wake_at

    async def sleep(self, seconds: float, inline_max: float = 1.0) -> None:
        """Durable sleep. Short waits (<= inline_max s) sleep in-process; longer ones
        suspend the run and free the worker. A Worker (or journal.pending_timers())
        resumes it once due; resuming early simply suspends again."""
        wake_at = self._timer(self._key("sleep"), seconds)
        remaining = wake_at - time.time()
        if remaining <= 0:
            return
        if remaining <= inline_max:
            await asyncio.sleep(remaining)
            return
        raise Suspended(None, wake_at)

    async def wait_for(self, signal: str, timeout: Optional[float] = None, default: Any = None) -> Any:
        """Wait for `signal`, delivered via flow.run/resume(..., signals={signal: value}).

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
        used = self._signals_used.get(signal, 0)
        queue = self._inbox.get(signal, [])
        if used < len(queue):
            if tkey:
                self._replay(tkey)
            self._signals_used[signal] = used + 1
            value = queue[used]
            self._record(key, "signal", value, {"signal": signal})
            return value
        if tkey is None:
            raise Suspended(signal)
        wake_at = self._timer(tkey, timeout)  # type: ignore[arg-type]
        if time.time() >= wake_at:
            self._record(key, "signal", default, {"signal": signal, "timed_out": True})
            self.emit("signal_timeout", key)
            return default
        raise Suspended(signal, wake_at)

    def _unused_signals(self) -> Dict[str, List[Any]]:
        return {n: q[self._signals_used.get(n, 0):] for n, q in self._inbox.items()
                if len(q) > self._signals_used.get(n, 0)}

    # -- evolution
    def patched(self, change_id: str) -> bool:
        """Safely change flow code while runs are in flight.

            if ctx.patched("v2-extra-check"):
                await new_check()        # runs that reach this point on new code
            # runs already past this point keep the old behaviour on replay
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
        state. Use in long-lived loops (chat sessions, monitors) to keep replay short."""
        self._note_progress(self._scope_now().prefix)
        raise ContinueAsNew(args, kwargs, dict(self.state))

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
    run_id: str
    status: str                       # completed | suspended | failed
    output: Any = None
    state: dict = field(default_factory=dict)
    waiting_for: Optional[str] = None  # signal name, if waiting on one
    wake_at: Optional[float] = None    # unix time a timer is due, if any
    error: Optional[BaseException] = None
    events: List[dict] = field(default_factory=list)


@dataclass
class _Next:
    """Internal: hop to the next run in a continue-as-new chain."""
    run_id: str
    args: tuple
    kwargs: dict
    state: dict
    generation: int
    root: str
    signals: Optional[Dict[str, List[Any]]]     # name -> queued values


class Flow:
    def __init__(self, fn: Callable[..., Awaitable[Any]], journal: Optional[Journal] = None,
                 on_event: Union[None, Observer, List[Observer]] = None):
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("@flow functions must be async def")
        self.fn, self.journal = fn, journal or MemoryJournal()
        self.name: str = fn.__name__
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

    async def run(self, *args: Any, run_id: Optional[str] = None, state: Optional[dict] = None,
                  signals: Optional[Mapping[str, Any]] = None,
                  on_event: Optional[Observer] = None, **kwargs: Any) -> RunResult:
        """Start a run, or resume it if `run_id` already has history."""
        _install_task_factory(asyncio.get_running_loop())
        observers = self.observers + ([on_event] if on_event else [])
        events: List[dict] = []
        rid = run_id or uuid.uuid4().hex
        queued = {k: [v] for k, v in signals.items()} if signals else None
        nxt = _Next(rid, args, kwargs, dict(state or {}), 0, rid, queued)
        for _ in range(10_000):                       # continue-as-new hops
            def emit(event: str, key: str = "", _rid: str = nxt.run_id, **data: Any) -> None:
                ev = {"event": event, "key": key, "run_id": _rid, "flow": self.name, **data}
                events.append(ev)
                for obs in observers:
                    try:
                        obs(ev)
                    except Exception:  # noqa: BLE001 -- observability must never break a run
                        pass

            with self.journal.lock(nxt.run_id):       # RunLocked if another worker owns it
                out = await self._execute(nxt, events, emit)
            if isinstance(out, RunResult):
                return out
            nxt = out
        raise RuntimeError("continue-as-new chain too long")

    async def _execute(self, n: _Next, events: List[dict], emit: Callable[..., None]) -> Union[RunResult, _Next]:
        run_id = n.run_id
        history = self.journal.load(run_id)
        done = next((e for e in history if e.kind == "done"), None)
        if done is not None:
            nxt_id = done.meta.get("continued_as")
            if nxt_id:
                c = done.result
                return _Next(nxt_id, tuple(c["args"]), c["kwargs"], c["state"], c["generation"], c["root"],
                             n.signals)
            return RunResult(run_id, "completed", done.result, self.journal.state_at(run_id), events=events)

        start = next((e for e in history if e.kind == "start"), None)
        if start is None:
            self._reject_secrets("flow inputs", args=list(n.args), kwargs=n.kwargs, state=n.state)
            start = self.journal.append(Entry(
                run_id, "start", "start",
                {"args": list(n.args), "kwargs": n.kwargs, "state": n.state, "generation": n.generation,
                 "root": n.root, "flow": self.name}, dict(n.state)))
            history = [start]
            emit("run_started", generation=n.generation)
        else:
            emit("run_started", resumed=True, completed=sum(1 for e in history if e.kind == "step"))
        s = start.result
        args, kwargs = tuple(s["args"]), s["kwargs"]
        generation, root = int(s.get("generation", 0)), s.get("root", run_id)

        if n.signals:
            self._reject_secrets("signals", signals=n.signals)
            history.append(self.journal.append(Entry(run_id, "inbox", "inbox", dict(n.signals))))

        ctx = Context(run_id, self.journal, dict(s.get("state", {})), history, emit, generation)
        token, scope_token = _current.set(ctx), _scope.set(ctx._root)
        try:
            try:
                output = await self.fn(ctx, *args, **kwargs)
                ctx._note_progress("")                    # returning normally = failures were handled
            finally:
                await self._cancel_branches(ctx)
        except Suspended as sp:
            ctx._record("suspend", "suspend", {"signal": sp.signal, "wake_at": sp.wake_at}, {})
            emit("suspended", waiting_for=sp.signal, wake_at=sp.wake_at)
            return RunResult(run_id, "suspended", None, ctx.state, sp.signal, sp.wake_at, events=events)
        except ContinueAsNew as c:
            new_id = f"{root}.g{generation + 1}"
            if new_id == run_id:  # pragma: no cover - defensive
                raise RuntimeError("continue-as-new id collision") from None
            leftover = ctx._unused_signals()
            try:
                self._reject_secrets("continue_as_new inputs/state", args=list(c.args_), kwargs=c.kwargs,
                                     state=c.state)
                ctx._record("done", "done", {"args": list(c.args_), "kwargs": c.kwargs, "state": c.state,
                                             "generation": generation + 1, "root": root},
                            {"continued_as": new_id})
            except Exception as e:  # noqa: BLE001
                return self._fail(ctx, e, emit, events)
            emit("continued_as_new", next_run_id=new_id)
            return _Next(new_id, c.args_, c.kwargs, c.state, generation + 1, root, leftover or None)
        except Exception as e:  # noqa: BLE001
            return self._fail(ctx, e, emit, events)
        except BaseException as e:               # crash/cancel: close spans, then propagate
            emit("crashed", error=repr(e))
            raise
        finally:
            ctx._active = False
            _scope.reset(scope_token)
            _current.reset(token)

        stale = ctx._history_keys - ctx._consumed
        if stale:
            emit("divergence", stale=sorted(stale))   # flow code changed between runs
        try:
            ctx._record("done", "done", output, {})
        except Exception as e:  # noqa: BLE001 -- e.g. an output the codec can't journal
            return self._fail(ctx, e, emit, events)
        emit("finished")
        return RunResult(run_id, "completed", output, ctx.state, events=events)

    def _fail(self, ctx: Context, e: Exception, emit: Callable[..., None], events: List[dict]) -> RunResult:
        meta: Dict[str, Any] = {"error": repr(e)[:500]}
        if isinstance(e, StepFailed):
            meta["step"] = e.key
        try:
            ctx._record("flow", "failed", None, meta)
        except Exception:  # noqa: BLE001 -- state itself may be unserializable
            self.journal.append(Entry(ctx.run_id, "flow", "failed", None, {}, [], meta))
        emit("failed", error=repr(e))
        return RunResult(ctx.run_id, "failed", None, ctx.state, error=e, events=events)

    @staticmethod
    async def _cancel_branches(ctx: Context) -> None:
        """Cancel branches still running so they can't journal after the run returns."""
        ctx._active = False
        pending = [t for t in ctx._tasks if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for t in ctx._tasks:           # silence "exception was never retrieved" warnings
            if t.done() and not t.cancelled():
                t.exception()

    async def resume(self, run_id: str, signals: Optional[Mapping[str, Any]] = None,
                     on_event: Optional[Observer] = None) -> RunResult:
        return await self.run(run_id=run_id, signals=signals, on_event=on_event)

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
         on_event: Union[None, Observer, List[Observer]] = None) -> Any:
    """Decorate an `async def f(ctx, ...)` to make it a durable flow."""
    def deco(f: Callable) -> Flow:
        return Flow(f, journal, on_event)
    return deco(fn) if fn is not None else deco
