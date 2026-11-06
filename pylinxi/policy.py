"""Self-healing step policies: retry, timeout, validate, repair, fallback, circuit breaker."""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import inspect
import os
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple, Type


class ValidationError(Exception):
    """Raised when a step's output fails its validator. Always retryable."""


class CircuitOpen(Exception):
    """Raised when a step's circuit breaker is open and no fallback exists."""


class AttemptStillRunning(Exception):
    """A sync step timed out and its thread was still running when the next attempt
    was due, so the outcome of that attempt is unknown; pylinxi does not start
    another attempt alongside it (that could repeat a side effect)."""


class PriorAttemptRunning(BaseException):
    """An earlier execution of this run left an attempt of the step running (it was
    cancelled by a drain, a crash of the caller, a timeout) and it is still running.
    Its outcome is unknown, so the step must neither fail nor run again: the flow
    suspends until `retry_at` and then picks up that attempt's result. A
    BaseException so `except Exception` / `except StepFailed` in flow code can't
    turn it into a permanent, journaled failure."""

    def __init__(self, key: str, retry_at: float):
        super().__init__(f"an earlier attempt of step {key!r} is still running")
        self.key, self.retry_at = key, retry_at


PRIOR_ATTEMPT_WAIT = 5.0      # seconds a resume waits on a still-running earlier attempt


class StepFailed(Exception):
    def __init__(self, key: str, attempts: int, error: BaseException):
        super().__init__(f"step {key!r} failed after {attempts} attempt(s): {error!r}")
        self.key, self.attempts, self.error = key, attempts, error


@dataclass
class Retry:
    """attempts/backoff for transient errors. Errors that aren't instances of `on`
    are not retried, but still go to the fallback and the circuit breaker."""
    attempts: int = 3
    backoff: float = 0.25          # first delay in seconds
    factor: float = 2.0
    max_backoff: float = 10.0
    jitter: bool = True
    on: Tuple[Type[BaseException], ...] = (Exception,)

    def delay(self, attempt: int) -> float:
        d = min(self.backoff * (self.factor ** attempt), self.max_backoff)
        return d * random.uniform(0.5, 1.0) if self.jitter else d


class CircuitBreaker:
    """Shared across runs and concurrent calls: after `threshold` consecutive
    failures, short-circuit calls for `reset_after` seconds, then let exactly one
    trial call through (half-open). Failures reported while already open (from
    calls that started earlier) don't extend the outage. A trial that is cancelled
    gives its slot back, so the breaker can never get stuck half-open."""

    def __init__(self, threshold: int = 5, reset_after: float = 30.0):
        self.threshold, self.reset_after = threshold, reset_after
        self.failures = 0
        self.opened_at: Optional[float] = None
        self._trial = False
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return "closed"
        return "half_open" if time.monotonic() - self.opened_at >= self.reset_after else "open"

    def acquire(self) -> Optional[str]:
        """'closed' or 'trial' if a call may go ahead, None if the breaker is open."""
        with self._lock:
            st = self.state
            if st == "closed":
                return "closed"
            if st == "half_open" and not self._trial:
                self._trial = True
                return "trial"
            return None

    def allow(self) -> bool:
        return self.acquire() is not None

    def release_trial(self) -> None:
        """Give back a half-open trial slot without counting success or failure."""
        with self._lock:
            self._trial = False

    def success(self) -> None:
        with self._lock:
            self.failures, self.opened_at, self._trial = 0, None, False

    def failure(self) -> None:
        with self._lock:
            st = self.state
            if st == "open":
                return
            if st == "half_open":
                if self._trial:                       # the trial call failed: re-open
                    self.opened_at, self._trial = time.monotonic(), False
                return
            self.failures += 1
            if self.failures >= self.threshold:
                self.opened_at = time.monotonic()


@dataclass
class Policy:
    """How a step heals itself.

    retry     -- attempts/backoff for transient errors (see Retry.on)
    timeout   -- per-attempt timeout in seconds. Async steps are cancelled; a sync
                 step's thread can't be killed, so before the next attempt pylinxi
                 waits up to `timeout` more for it, uses its result if it finished,
                 and otherwise stops retrying (AttemptStillRunning) rather than run
                 the step twice at once.
    validate  -- fn(result) -> bool; False triggers ValidationError (retried)
    repair    -- fn(error, args, kwargs, attempt) -> (args, kwargs) | None;
                 lets you mutate inputs between attempts (e.g. feed the parse
                 error back into an LLM prompt). May be async.
    fallback  -- fn(*args, **kwargs) used when attempts are exhausted, an error is
                 not retryable, or the breaker is open. May be async.
    breaker   -- CircuitBreaker instance (share one per dependency)
    aggregate -- for generator (streaming) steps: turns the chunk list into the
                 journaled result. Default: "".join for str chunks, else the list.
    thread    -- run plain sync steps in a worker thread (default True)
    executor  -- thread pool for sync steps (default: a dedicated pylinxi pool, so
                 slow steps never starve asyncio's default executor)
    """
    retry: Retry = field(default_factory=lambda: Retry(attempts=1))
    timeout: Optional[float] = None
    validate: Optional[Callable[[Any], bool]] = None
    repair: Optional[Callable[..., Any]] = None
    fallback: Optional[Callable[..., Any]] = None
    breaker: Optional[CircuitBreaker] = None
    aggregate: Optional[Callable[[list], Any]] = None   # streaming steps: chunks -> result
    thread: bool = True                                 # run sync steps in a worker thread
    executor: Optional[concurrent.futures.Executor] = None


_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
_pool_guard = threading.Lock()


def step_executor() -> concurrent.futures.ThreadPoolExecutor:
    """The shared thread pool pylinxi runs sync steps in."""
    global _pool
    with _pool_guard:
        if _pool is None:
            _pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=max(32, (os.cpu_count() or 1) * 4), thread_name_prefix="pylinxi-step")
        return _pool


# Sync attempts whose thread is still running after a timeout or cancellation, keyed by
# "<run_id>:<step_key>". A later attempt of the same step -- in this call, a resume, or a
# Worker retry in this process -- waits for it instead of running alongside it.
_abandoned: dict = {}
_abandoned_guard = threading.Lock()


_ABANDONED_TTL, _ABANDONED_MAX = 3600.0, 1024


def _abandon(ident: Optional[str], fut: concurrent.futures.Future, reusable: bool) -> None:
    """Remember a sync attempt whose thread outlived its caller. A successful result
    is kept (bounded, for an hour) so the next attempt of that step uses it instead of
    running the side effect again."""
    if ident is None:
        return
    now = time.monotonic()
    with _abandoned_guard:
        for k in [k for k, v in _abandoned.items() if now - v[2] > _ABANDONED_TTL]:
            del _abandoned[k]
        while len(_abandoned) >= _ABANDONED_MAX:
            del _abandoned[next(iter(_abandoned))]
        _abandoned[ident] = (fut, reusable, now)

    def settled(f: Any) -> None:
        keep = reusable and not f.cancelled() and f.exception() is None
        if not keep:
            with _abandoned_guard:
                if _abandoned.get(ident, (None,))[0] is f:
                    del _abandoned[ident]
    fut.add_done_callback(settled)


def _claim(ident: Optional[str]) -> Optional[Tuple[Any, bool]]:
    if ident is None:
        return None
    with _abandoned_guard:
        v = _abandoned.pop(ident, None)
    return (v[0], v[1]) if v is not None else None


def _aggregate(chunks: list, agg: Optional[Callable[[list], Any]]) -> Any:
    if agg is not None:
        return agg(chunks)
    return "".join(chunks) if chunks and all(isinstance(c, str) for c in chunks) else chunks


class _ThreadTimeout(asyncio.TimeoutError):
    """An attempt on a worker thread timed out; `future` is that still-running thread
    (a concurrent.futures.Future, so it can be awaited from any later event loop)."""

    def __init__(self, future: concurrent.futures.Future, reusable: bool):
        super().__init__("step timed out")
        self.future, self.reusable = future, reusable


async def _call(fn: Callable, args: tuple, kwargs: dict, timeout: Optional[float],
                on_chunk: Optional[Callable[[Any], None]] = None,
                aggregate: Optional[Callable[[list], Any]] = None, thread: bool = True,
                executor: Optional[concurrent.futures.Executor] = None, ident: Optional[str] = None) -> Any:
    """Invoke any kind of step callable.

    async def            -> awaited (cancelled on timeout)
    async generator      -> streamed: each chunk goes to on_chunk, result is aggregate(chunks)
    sync generator       -> streamed from a worker thread; stops at the next chunk if
                            the attempt is abandoned (timeout/cancel)
    plain sync function  -> run in a worker thread (thread=True) so parallel branches
                            and timeouts work
    """
    loop = asyncio.get_running_loop()
    pool = executor or step_executor()

    async def bounded(aw: Any) -> Any:
        return await (asyncio.wait_for(aw, timeout) if timeout is not None else aw)

    if inspect.isasyncgenfunction(fn):
        async def stream() -> Any:
            chunks = []
            async for c in fn(*args, **kwargs):
                chunks.append(c)
                if on_chunk:
                    on_chunk(c)
            return _aggregate(chunks, aggregate)
        return await bounded(stream())

    if inspect.isgeneratorfunction(fn):
        abandoned = threading.Event()

        def collect() -> list:
            out = []
            gen = fn(*args, **kwargs)
            try:
                for c in gen:
                    if abandoned.is_set():
                        break
                    out.append(c)
                    if on_chunk:
                        loop.call_soon_threadsafe(on_chunk, c)
            finally:
                gen.close()
            return out
        cf = pool.submit(contextvars.copy_context().run, collect)
        fut = asyncio.wrap_future(cf)
        try:
            chunks = await (asyncio.wait_for(asyncio.shield(fut), timeout) if timeout is not None
                            else asyncio.shield(fut))
        except asyncio.TimeoutError:
            abandoned.set()
            _abandon(ident, cf, reusable=False)
            raise _ThreadTimeout(cf, reusable=False) from None    # partial output: never reuse
        except BaseException:
            abandoned.set()
            _abandon(ident, cf, reusable=False)
            raise
        return _aggregate(chunks, aggregate)

    if inspect.iscoroutinefunction(fn):
        return await bounded(fn(*args, **kwargs))

    if not thread:
        res = fn(*args, **kwargs)
        return await bounded(res) if inspect.isawaitable(res) else res

    # copy context vars into the thread (idempotency_key() etc.), as asyncio.to_thread does
    cf = pool.submit(functools.partial(contextvars.copy_context().run, fn, *args, **kwargs))
    fut = asyncio.wrap_future(cf)
    try:
        res = await asyncio.wait_for(asyncio.shield(fut), timeout) if timeout is not None else await asyncio.shield(fut)
    except asyncio.TimeoutError:
        _abandon(ident, cf, reusable=True)
        raise _ThreadTimeout(cf, reusable=True) from None
    except BaseException:                         # cancelled (e.g. a run draining): the thread runs on
        _abandon(ident, cf, reusable=True)
        raise
    return await bounded(res) if inspect.isawaitable(res) else res


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


async def run_with_policy(key: str, fn: Callable, args: tuple, kwargs: dict,
                          policy: Policy, emit: Callable[..., None], ident: Optional[str] = None) -> Tuple[Any, dict]:
    """Execute fn under policy. Returns (result, meta) where meta records healing.

    Every Exception from an attempt (retryable or not, from the step, its validator
    or its repair hook) ends in the fallback if there is one, otherwise StepFailed,
    and is reported to the breaker. BaseExceptions (cancellation, Suspended, ...)
    propagate untouched."""
    r, br = policy.retry, policy.breaker
    current = {"attempt": 0, "sent": False}

    def chunk_sink(attempt_no: int) -> Callable[[Any], None]:
        def on_chunk(c: Any) -> None:
            if current["attempt"] == attempt_no:       # drop late chunks from abandoned attempts
                current["sent"] = True
                emit("chunk", key, chunk=c)
        return on_chunk

    def reset_stream() -> None:
        if current["sent"]:
            emit("stream_reset", key)                  # consumers should discard partial output
            current["sent"] = False

    retryable = tuple(r.on) + (ValidationError, asyncio.TimeoutError)
    last: Optional[BaseException] = None
    errors: list = []
    ran = 0
    breaker_blocked = False
    abandoned: Optional[_ThreadTimeout] = None
    prior = _claim(ident)                         # an earlier execution's attempt may still be running
    if prior is not None:
        abandoned = _ThreadTimeout(prior[0], prior[1])

    async def checked(result: Any) -> Any:
        if policy.validate is not None and not await _maybe_await(policy.validate(result)):
            # never include the value itself: it may be sensitive
            raise ValidationError(f"output rejected by validator ({type(result).__name__})")
        return result

    def ok(result: Any, healed: Optional[str]) -> Tuple[Any, dict]:
        meta: dict = {"attempts": ran, "healed": healed, "errors": errors}
        if breaker_blocked:
            meta["breaker_open"] = True
        return result, meta

    for attempt in range(max(1, r.attempts)):
        if abandoned is not None:                  # a timed-out sync attempt may still be running
            prev, abandoned = abandoned, None
            from_prior = prior is not None and attempt == 0
            wait = min(policy.timeout or PRIOR_ATTEMPT_WAIT, PRIOR_ATTEMPT_WAIT) if from_prior else \
                (policy.timeout or 30.0)
            try:
                late = await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(prev.future)), wait)
            except asyncio.TimeoutError:
                _abandon(ident, prev.future, prev.reusable)    # still running: later attempts must see it
                if from_prior:                     # not this execution's attempt: wait for it, don't fail
                    raise PriorAttemptRunning(key, time.time() + PRIOR_ATTEMPT_WAIT) from None
                last = AttemptStillRunning(f"attempt {attempt} of step {key!r} is still running after "
                                           f"its timeout; not starting another one alongside it")
                errors.append(f"AttemptStillRunning: {last}"[:300])
                emit("attempt_failed", key, attempt=attempt, error=errors[-1])
                break
            except Exception:  # noqa: BLE001 -- that attempt failed after all: retry as planned
                pass
            else:
                if prev.reusable:                  # it finished: use its result, don't run again
                    try:
                        result = await checked(late)
                    except Exception as e:  # noqa: BLE001
                        last = e
                        errors.append(f"{type(e).__name__}: {e}"[:300])
                    else:
                        _claim(ident)              # used: never hand the same result out twice
                        if br is not None:
                            br.success()
                        emit("late_result", key, attempt=attempt)
                        return ok(result, "retry" if ran else None)
        perm = br.acquire() if br is not None else "closed"
        if perm is None:
            breaker_blocked = True
            errors.append(f"CircuitOpen: breaker open for {key!r}")
            emit("breaker_open", key)
            break
        current["attempt"] = attempt
        reset_stream()
        ran += 1
        try:
            result = await checked(await _call(fn, args, kwargs, policy.timeout, chunk_sink(attempt),
                                               policy.aggregate, policy.thread, policy.executor, ident))
        except Exception as e:  # noqa: BLE001 -- every failure is handled below
            perm = "closed"                        # outcome reported below: the trial slot is settled
            last = e
            if isinstance(e, _ThreadTimeout):
                abandoned = e
            errors.append(f"{type(e).__name__}: {e}"[:300])
            if br is not None:
                br.failure()
            emit("attempt_failed", key, attempt=attempt + 1, error=errors[-1])
            if not isinstance(e, retryable) or attempt + 1 >= r.attempts:
                break
            if policy.repair is not None:
                try:
                    fixed = await _maybe_await(policy.repair(e, args, kwargs, attempt))
                except Exception as re_:  # noqa: BLE001
                    last = re_
                    errors.append(f"repair {type(re_).__name__}: {re_}"[:300])
                    break
                if fixed is not None:
                    args, kwargs = fixed
                    emit("repaired", key, attempt=attempt + 1)
            await asyncio.sleep(r.delay(attempt))
        except BaseException:
            if br is not None and perm == "trial":
                br.release_trial()                 # cancelled mid-trial (not during backoff): free the slot
            raise
        else:
            if br is not None:
                br.success()
            return ok(result, "retry" if attempt else None)

    if last is None:
        last = CircuitOpen(key)
    if policy.fallback is not None:
        current["attempt"] = -1
        reset_stream()
        emit("fallback", key, error=repr(last))
        try:
            result = await _call(policy.fallback, args, kwargs, policy.timeout, chunk_sink(-1),
                                 policy.aggregate, policy.thread, policy.executor)
        except Exception as fe:  # noqa: BLE001
            errors.append(f"fallback {type(fe).__name__}: {fe}"[:300])
            raise StepFailed(key, ran, fe) from fe
        return ok(result, "fallback")
    raise StepFailed(key, ran, last)
