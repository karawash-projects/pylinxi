"""Self-healing step policies: retry, timeout, validate, repair, fallback, circuit breaker."""
from __future__ import annotations

import asyncio
import inspect
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple, Type


class ValidationError(Exception):
    """Raised when a step's output fails its validator. Always retryable."""


class CircuitOpen(Exception):
    """Raised when a step's circuit breaker is open and no fallback exists."""


class StepFailed(Exception):
    def __init__(self, key: str, attempts: int, error: BaseException):
        super().__init__(f"step {key!r} failed after {attempts} attempt(s): {error!r}")
        self.key, self.attempts, self.error = key, attempts, error


@dataclass
class Retry:
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
    calls that started earlier) don't extend the outage."""

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

    def allow(self) -> bool:
        with self._lock:
            st = self.state
            if st == "closed":
                return True
            if st == "half_open" and not self._trial:
                self._trial = True
                return True
            return False

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

    retry     -- attempts/backoff for transient errors
    timeout   -- per-attempt timeout in seconds
    validate  -- fn(result) -> bool; False triggers ValidationError (retried)
    repair    -- fn(error, args, kwargs, attempt) -> (args, kwargs) | None;
                 lets you mutate inputs between attempts (e.g. feed the parse
                 error back into an LLM prompt). May be async.
    fallback  -- fn(*args, **kwargs) used when attempts are exhausted or the
                 breaker is open. May be async.
    breaker   -- CircuitBreaker instance (share one per dependency)
    aggregate -- for generator (streaming) steps: turns the chunk list into the
                 journaled result. Default: "".join for str chunks, else the list.
    thread    -- run plain sync steps in a worker thread (default True)
    """
    retry: Retry = field(default_factory=lambda: Retry(attempts=1))
    timeout: Optional[float] = None
    validate: Optional[Callable[[Any], bool]] = None
    repair: Optional[Callable[..., Any]] = None
    fallback: Optional[Callable[..., Any]] = None
    breaker: Optional[CircuitBreaker] = None
    aggregate: Optional[Callable[[list], Any]] = None   # streaming steps: chunks -> result
    thread: bool = True                                 # run sync steps in a worker thread


def _aggregate(chunks: list, agg: Optional[Callable[[list], Any]]) -> Any:
    if agg is not None:
        return agg(chunks)
    return "".join(chunks) if chunks and all(isinstance(c, str) for c in chunks) else chunks


async def _call(fn: Callable, args: tuple, kwargs: dict, timeout: Optional[float],
                on_chunk: Optional[Callable[[Any], None]] = None,
                aggregate: Optional[Callable[[list], Any]] = None, thread: bool = True) -> Any:
    """Invoke any kind of step callable.

    async def            -> awaited
    async generator      -> streamed: each chunk goes to on_chunk, result is aggregate(chunks)
    sync generator       -> streamed from a worker thread; stops at the next chunk if
                            the attempt is abandoned (timeout/cancel)
    plain sync function  -> run in a worker thread (thread=True) so parallel branches
                            and timeouts work; note a timed-out thread cannot be killed
    """
    abandoned = threading.Event()

    async def invoke() -> Any:
        if inspect.isasyncgenfunction(fn):
            chunks = []
            async for c in fn(*args, **kwargs):
                chunks.append(c)
                if on_chunk:
                    on_chunk(c)
            return _aggregate(chunks, aggregate)
        if inspect.isgeneratorfunction(fn):
            loop = asyncio.get_running_loop()

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
            return _aggregate(await asyncio.to_thread(collect), aggregate)
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        res = await asyncio.to_thread(fn, *args, **kwargs) if thread else fn(*args, **kwargs)
        return await res if inspect.isawaitable(res) else res

    try:
        return await (asyncio.wait_for(invoke(), timeout) if timeout is not None else invoke())
    finally:
        abandoned.set()


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


async def run_with_policy(key: str, fn: Callable, args: tuple, kwargs: dict,
                          policy: Policy, emit: Callable[..., None]) -> Tuple[Any, dict]:
    """Execute fn under policy. Returns (result, meta) where meta records healing."""
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
    attempt = 0
    for attempt in range(max(1, r.attempts)):
        if br is not None and not br.allow():
            last = CircuitOpen(key)
            emit("breaker_open", key)
            break
        current["attempt"] = attempt
        reset_stream()
        try:
            result = await _call(fn, args, kwargs, policy.timeout, chunk_sink(attempt),
                                 policy.aggregate, policy.thread)
            if policy.validate is not None and not await _maybe_await(policy.validate(result)):
                # never include the value itself: it may be sensitive
                raise ValidationError(f"output rejected by validator ({type(result).__name__})")
            if br is not None:
                br.success()
            return result, {"attempts": attempt + 1, "healed": "retry" if attempt else None,
                            "errors": errors}
        except retryable as e:  # noqa: PERF203
            last = e
            errors.append(f"{type(e).__name__}: {e}"[:300])
            if br is not None:
                br.failure()
            emit("attempt_failed", key, attempt=attempt + 1, error=errors[-1])
            if attempt + 1 >= r.attempts:
                break
            if policy.repair is not None:
                fixed = await _maybe_await(policy.repair(e, args, kwargs, attempt))
                if fixed is not None:
                    args, kwargs = fixed
                    emit("repaired", key, attempt=attempt + 1)
            await asyncio.sleep(r.delay(attempt))

    if policy.fallback is not None:
        current["attempt"] = -1
        reset_stream()
        emit("fallback", key, error=repr(last))
        try:
            result = await _call(policy.fallback, args, kwargs, policy.timeout, chunk_sink(-1),
                                 policy.aggregate, policy.thread)
        except Exception as fe:  # noqa: BLE001
            errors.append(f"fallback {type(fe).__name__}: {fe}"[:300])
            raise StepFailed(key, attempt + 1, fe) from fe
        return result, {"attempts": attempt + 1, "healed": "fallback", "errors": errors}
    raise StepFailed(key, attempt + 1, last if last else RuntimeError("unknown"))
