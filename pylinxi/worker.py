"""Background worker: the fleet-level half of self-healing.

    worker = Worker(journal, [support, onboarding], retry_failed=True)
    await worker.run_forever(interval=5)

Each tick it resumes, with bounded concurrency:
  1. runs whose durable timers (ctx.sleep, wait_for timeouts) are due;
  2. runs that received a signal (journal.send_signal / flow.signal) while suspended;
  3. optionally (retry_failed) runs that ended in an unhandled failure, with
     exponential backoff, giving up after `max_failures` consecutive failures
     without progress;
  4. runs left `open` by a crashed process: their last entry is older than
     `resume_stale_after` seconds (default 60) and nobody holds their lock.
Run as many workers as you like against a PostgresJournal (or a file/SQLite journal
on one machine): per-run locks make them skip runs another worker already owns.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

from .flow import Flow, Observer
from .journal import Head, Journal, RunLocked

log = logging.getLogger("pylinxi")

_TERMINAL = ("done", "link")


@dataclass
class TickReport:
    resumed: List[str] = field(default_factory=list)       # run ids resumed this tick
    completed: List[str] = field(default_factory=list)
    gave_up: List[str] = field(default_factory=list)       # exceeded max_failures
    skipped: List[str] = field(default_factory=list)       # locked by another worker / unknown flow
    errors: Dict[str, str] = field(default_factory=dict)   # run id -> error raised while resuming


class Worker:
    def __init__(self, journal: Journal, flows: Union[Iterable[Flow], Mapping[str, Flow]],
                 retry_failed: bool = False, max_failures: int = 5,
                 backoff: float = 30.0, max_backoff: float = 3600.0,
                 on_event: Optional[Observer] = None, concurrency: int = 8,
                 resume_stale_after: Optional[float] = 60.0,
                 on_error: Optional[Callable[[BaseException], None]] = None,
                 on_tick: Optional[Callable[[TickReport], None]] = None):
        """concurrency        -- runs resumed in parallel per tick
        resume_stale_after -- seconds (default 60); resume `open` runs -- crashed
                              mid-execution -- whose last entry is older than this.
                              Runs still executing elsewhere hold their lock and are
                              skipped. None turns this off (do that for file/SQLite
                              journals shared across processes on Windows, which has
                              no cross-process file locks).
        on_error / on_tick -- callbacks for tick failures and per-tick reports
                              (errors are also logged to the "pylinxi" logger)."""
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.journal = journal
        self.flows: Dict[str, Flow] = dict(flows) if isinstance(flows, Mapping) else {f.name: f for f in flows}
        self.retry_failed, self.max_failures = retry_failed, max_failures
        self.backoff, self.max_backoff = backoff, max_backoff
        self.on_event, self.concurrency = on_event, concurrency
        self.resume_stale_after = resume_stale_after
        self.on_error, self.on_tick = on_error, on_tick
        self._gave_up: set = set()

    def _flow_for(self, run_id: str) -> Optional[Flow]:
        try:
            first = self.journal._raw(run_id)[:1]
        except Exception:  # noqa: BLE001 -- corrupt run: leave it for `pylinxi verify`
            return None
        for r in first:
            res = r.get("result") if r.get("kind") == "start" else None
            if isinstance(res, dict):
                return self.flows.get(str(res.get("flow", "")))
        return None

    def _failed_due(self, run_id: str, now: float) -> Optional[bool]:
        """True = retry now, False = wait, None = give up. Counts failures since the
        run last made progress, so a long run that keeps advancing isn't abandoned."""
        streak, last_ts, seen = 0, 0.0, set()
        for r in self.journal._raw(run_id):
            k = r.get("key")
            if r.get("kind") == "failed" and k == "flow":
                streak += 1
                last_ts = float(r.get("ts", 0.0))
            elif r.get("kind") in ("step", "signal", "timer") and k not in seen:
                streak = 0                         # first success of this key (re-runs don't count)
                seen.add(k)
        if streak == 0:
            return True
        if streak >= self.max_failures:
            return None
        wait = min(self.backoff * 2 ** (streak - 1), self.max_backoff)
        return now - last_ts >= wait

    async def _resume(self, run_id: str, report: TickReport) -> None:
        f = await asyncio.to_thread(self._flow_for, run_id)
        if f is None:
            report.skipped.append(run_id)
            return
        try:
            r = await f.resume(run_id, on_event=self.on_event)
        except RunLocked:
            report.skipped.append(run_id)
            return
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except BaseException as e:  # noqa: BLE001 -- one bad run must not starve the others
            report.errors[run_id] = repr(e)[:500]
            log.warning("pylinxi worker: resuming run %r failed: %r", run_id, e)
            return
        report.resumed.append(run_id)
        if r.status == "completed":
            report.completed.append(run_id)

    def _candidates(self, now: float, report: TickReport) -> List[str]:
        kinds = ["suspend", "inbox"]
        if self.retry_failed:
            kinds.append("failed")
        heads: List[Head] = self.journal.heads(None if self.resume_stale_after is not None else kinds)
        due: List[str] = []
        for h in heads:
            if h.kind == "suspend":
                res = h.raw().get("result")
                wake = res.get("wake_at") if isinstance(res, dict) else None
                if isinstance(wake, (int, float)) and wake <= now:
                    due.append(h.run_id)
            elif h.kind == "inbox":
                due.append(h.run_id)                    # a signal arrived while it was suspended
            elif h.kind == "failed" and h.key == "flow" and self.retry_failed:
                if h.run_id in self._gave_up:
                    continue
                try:
                    verdict = self._failed_due(h.run_id, now)
                except Exception as e:  # noqa: BLE001 -- corrupt run: report, keep going
                    report.errors[h.run_id] = repr(e)[:500]
                    continue
                if verdict is None:
                    self._gave_up.add(h.run_id)
                    report.gave_up.append(h.run_id)
                elif verdict:
                    due.append(h.run_id)
            elif (self.resume_stale_after is not None and h.kind not in _TERMINAL + ("suspend",)
                  and not (h.kind == "failed" and h.key == "flow")
                  and now - h.ts >= self.resume_stale_after):
                due.append(h.run_id)                    # open run whose executor is gone
        return due

    async def tick(self) -> TickReport:
        report, now = TickReport(), time.time()
        due = await asyncio.to_thread(self._candidates, now, report)
        sem = asyncio.Semaphore(self.concurrency)

        async def one(rid: str) -> None:
            async with sem:
                await self._resume(rid, report)
        await asyncio.gather(*(one(rid) for rid in dict.fromkeys(due)))
        return report

    async def run_forever(self, interval: float = 5.0, stop: Optional[asyncio.Event] = None) -> None:
        stop = stop or asyncio.Event()
        failures = 0
        while not stop.is_set():
            delay = interval
            try:
                report = await self.tick()
                failures = 0
                if self.on_tick is not None:
                    self.on_tick(report)
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:  # noqa: BLE001 -- a bad tick must not kill the worker
                failures += 1
                delay = min(interval * 2 ** failures, 60.0)
                log.exception("pylinxi worker: tick failed (%d in a row); retrying in %.0fs", failures, delay)
                if self.on_error is not None:
                    try:
                        self.on_error(e)
                    except Exception:  # noqa: BLE001
                        log.exception("pylinxi worker: on_error callback failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    def __repr__(self) -> str:  # pragma: no cover
        return f"Worker(flows={list(self.flows)}, retry_failed={self.retry_failed})"


__all__: List[Any] = ["Worker", "TickReport"]
