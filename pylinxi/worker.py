"""Background worker: the fleet-level half of self-healing.

    worker = Worker(journal, [support, onboarding], retry_failed=True)
    await worker.run_forever(interval=5)

Each tick it
  1. resumes runs whose durable timers (ctx.sleep, wait_for timeouts) are due;
  2. optionally re-resumes failed runs with exponential backoff, giving up after
     `max_failures` -- useful when failures are transient (an outage that outlasted
     the step's own retry policy).
Run as many workers as you like against a PostgresJournal: per-run locks make
them skip runs another worker already owns.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

from .flow import Flow, Observer
from .journal import Journal, RunLocked


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
                 on_event: Optional[Observer] = None):
        self.journal = journal
        self.flows: Dict[str, Flow] = dict(flows) if isinstance(flows, Mapping) else {f.name: f for f in flows}
        self.retry_failed, self.max_failures = retry_failed, max_failures
        self.backoff, self.max_backoff = backoff, max_backoff
        self.on_event = on_event
        self._gave_up: set = set()

    def _flow_for(self, run_id: str) -> Optional[Flow]:
        try:
            entries = self.journal.load(run_id)
        except Exception:  # noqa: BLE001 -- corrupt run: leave it for `pylinxi verify`
            return None
        start = next((e for e in entries if e.kind == "start"), None)
        return self.flows.get(start.result.get("flow", "")) if start else None

    def _failed_due(self, run_id: str, now: float) -> Optional[bool]:
        """True = retry now, False = wait, None = give up."""
        fails = [e for e in self.journal.load(run_id) if e.kind == "failed" and e.key == "flow"]
        if len(fails) >= self.max_failures:
            return None
        wait = min(self.backoff * 2 ** (len(fails) - 1), self.max_backoff)
        return now - fails[-1].ts >= wait

    async def _resume(self, run_id: str, report: TickReport) -> None:
        f = self._flow_for(run_id)
        if f is None:
            report.skipped.append(run_id)
            return
        try:
            r = await f.resume(run_id, on_event=self.on_event)
        except RunLocked:
            report.skipped.append(run_id)
            return
        except Exception as e:  # noqa: BLE001 -- one bad run must not starve the others
            report.errors[run_id] = repr(e)
            return
        report.resumed.append(run_id)
        if r.status == "completed":
            report.completed.append(run_id)

    async def tick(self) -> TickReport:
        report, now = TickReport(), time.time()
        for run_id, _ in self.journal.pending_timers(due_before=now):
            await self._resume(run_id, report)
        if self.retry_failed:
            # only runs that *ended* in an unhandled failure; failures the flow caught don't count
            for run_id in self.journal.runs_where_last("failed", key="flow"):
                if run_id in self._gave_up:
                    continue
                due = self._failed_due(run_id, now)
                if due is None:
                    self._gave_up.add(run_id)
                    report.gave_up.append(run_id)
                elif due:
                    await self._resume(run_id, report)
        return report

    async def run_forever(self, interval: float = 5.0, stop: Optional[asyncio.Event] = None) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 -- a bad tick must not kill the worker
                pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    def __repr__(self) -> str:  # pragma: no cover
        return f"Worker(flows={list(self.flows)}, retry_failed={self.retry_failed})"


__all__: List[Any] = ["Worker", "TickReport"]
