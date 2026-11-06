"""OpenTelemetry tracing for pylinxi flows.  pip install "pylinxi[otel]"

    from pylinxi.otel import OTelExporter
    @flow(journal=..., on_event=OTelExporter())

Trace shape per execution (each run/resume is its own trace segment, linked by
the `pylinxi.run_id` attribute):

    pylinxi.run <flow>                      status, waiting_for, replayed-step events
      ├─ pylinxi.step classify#0            attempts, healed; events: attempt_failed, repaired
      ├─ pylinxi.step lookup_charges#0      events: breaker_open, fallback
      └─ pylinxi.step send#0                ERROR status if the step finally failed

Uses only the OpenTelemetry *API*; configure the SDK and any exporter
in your application as usual. Without an SDK configured, spans
are no-ops.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional, Tuple

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
except ImportError as _e:  # pragma: no cover
    raise ImportError('OTelExporter needs opentelemetry-api: pip install opentelemetry-api') from _e

_HEAL_EVENTS = {"attempt_failed", "repaired", "fallback", "breaker_open"}
_SAFE_ATTRS = ("attempt", "attempts", "healed", "error", "waiting_for", "completed", "resumed")


def _attrs(ev: dict) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in _SAFE_ATTRS:
        v = ev.get(k)
        if v is not None:
            out[f"pylinxi.{k}"] = v if isinstance(v, (bool, int, float, str)) else str(v)
    return out


class OTelExporter:
    def __init__(self, tracer: Optional[trace.Tracer] = None, record_errors: bool = True):
        self.tracer = tracer or trace.get_tracer("pylinxi")
        self.record_errors = record_errors   # set False if error strings may contain PII
        self._runs: Dict[str, Any] = {}
        self._steps: Dict[Tuple[str, str], Any] = {}
        self._lock = threading.Lock()

    def __call__(self, ev: dict) -> None:
        if not self.record_errors:
            ev = {k: v for k, v in ev.items() if k != "error"}
        name, rid, key = ev["event"], ev["run_id"], ev.get("key", "")
        with self._lock:
            run = self._runs.get(rid)

            if name == "run_started":
                span = self.tracer.start_span(f"pylinxi.run {ev.get('flow', '')}".strip(),
                                              attributes={"pylinxi.run_id": rid, "pylinxi.flow": ev.get("flow", ""),
                                                          **_attrs(ev)})
                self._runs[rid] = span
            elif run is None:
                return                               # event for a run we never saw start

            elif name == "started":
                ctx = trace.set_span_in_context(run)
                self._steps[(rid, key)] = self.tracer.start_span(
                    f"pylinxi.step {key.rsplit('#', 1)[0]}", context=ctx,
                    attributes={"pylinxi.run_id": rid, "pylinxi.step.key": key})
            elif name in _HEAL_EVENTS:
                (self._steps.get((rid, key)) or run).add_event(f"pylinxi.{name}", _attrs(ev))
            elif name == "completed":
                span = self._steps.pop((rid, key), None)
                if span is not None:
                    span.set_attributes(_attrs(ev))
                    span.end()
            elif name in ("step_failed", "step_crashed"):
                span = self._steps.pop((rid, key), None)
                if span is not None:
                    span.set_attributes(_attrs(ev))
                    span.set_status(Status(StatusCode.ERROR, str(ev.get("error", name))[:200]))
                    span.end()
            elif name in ("replayed", "divergence"):
                run.add_event(f"pylinxi.{name}", {"pylinxi.step.key": key} if key else
                              {"pylinxi.stale": ",".join(ev.get("stale", []))})

            elif name in ("finished", "suspended", "failed", "crashed", "continued_as_new"):
                run.set_attribute("pylinxi.status", {"finished": "completed", "continued_as_new": "continued"}
                                  .get(name, name))
                if ev.get("next_run_id"):
                    run.set_attribute("pylinxi.next_run_id", str(ev["next_run_id"]))
                run.set_attributes(_attrs(ev))
                if name in ("failed", "crashed"):
                    run.set_status(Status(StatusCode.ERROR, str(ev.get("error", name))[:200]))
                for k in [k for k in self._steps if k[0] == rid]:   # close any orphaned step spans
                    self._steps.pop(k).end()
                self._runs.pop(rid).end()
