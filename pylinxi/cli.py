"""pylinxi command line: inspect, verify and manage run journals.

    pylinxi --journal sqlite:///runs.db runs
    pylinxi runs --status failed            # journal from $PYLINXI_JOURNAL
    pylinxi show ticket-1001
    pylinxi state ticket-1001 --seq 3       # time travel
    pylinxi verify --all --require-signed   # exit code 1 on any failure (CI / audits)
    pylinxi timers --due
    pylinxi delete ticket-1001 --yes

Signing keys come from --key kid=secret (repeatable) or $PYLINXI_KEYS="k1=...,k2=...".
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

from .journal import Entry, Journal, JournalCorrupt, open_journal


def _parse_keys(pairs: Sequence[str]) -> Dict[str, bytes]:
    keys: Dict[str, bytes] = {}
    for p in pairs:
        if "=" not in p:
            raise SystemExit(f"bad key {p!r}: expected kid=secret")
        kid, secret = p.split("=", 1)
        keys[kid.strip()] = secret.encode()
    return keys


def run_status(entries: List[Entry]) -> str:
    """completed | continued | failed | sleeping (timer) | waiting (signal) | open (running or crashed)"""
    if not entries:
        return "missing"
    last = entries[-1]
    if last.kind == "done":
        return "continued" if last.meta.get("continued_as") else "completed"
    if last.kind == "failed" and last.key == "flow":
        return "failed"
    if last.kind == "suspend":
        return "sleeping" if (last.result or {}).get("wake_at") is not None else "waiting"
    return "open"


def _fmt_ts(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _preview(v: Any, width: int = 48) -> str:
    s = json.dumps(v, default=str) if not isinstance(v, str) else v
    s = s.replace("\n", " ")
    return s if len(s) <= width else s[: width - 1] + "…"


def _table(rows: List[List[str]], headers: List[str]) -> str:
    widths = [max(len(str(x)) for x in col) for col in zip(headers, *rows)]
    line = lambda r: "  ".join(str(c).ljust(w) for c, w in zip(r, widths)).rstrip()  # noqa: E731
    return "\n".join([line(headers), line(["-" * w for w in widths])] + [line(r) for r in rows])


def _out(args: argparse.Namespace, data: Any, text: str) -> None:
    print(json.dumps(data, indent=2, default=str) if args.json else text)


def cmd_runs(j: Journal, args: argparse.Namespace) -> int:
    rows: List[Dict[str, Any]] = []
    for rid in sorted(j.runs()):
        try:
            es = j.load(rid)
            st = run_status(es)
        except JournalCorrupt:
            es, st = [], "corrupt"
        if args.status and st != args.status:
            continue
        rows.append({"run_id": rid, "status": st, "entries": len(es),
                     "updated": es[-1].ts if es else None})
    rows.sort(key=lambda r: r["updated"] or 0, reverse=True)
    rows = rows[: args.limit]
    _out(args, rows, _table([[r["run_id"], r["status"], str(r["entries"]),
                              _fmt_ts(r["updated"]) if r["updated"] else ""] for r in rows],
                            ["RUN", "STATUS", "ENTRIES", "UPDATED"]) if rows else "no runs")
    return 0


def cmd_show(j: Journal, args: argparse.Namespace) -> int:
    try:
        es = j.load(args.run_id)
    except JournalCorrupt as e:
        print(f"run {args.run_id!r} is corrupt: {e}", file=sys.stderr)
        return 1
    if not es:
        print(f"run {args.run_id!r} not found", file=sys.stderr)
        return 1
    data: List[Dict[str, Any]] = [{"seq": e.seq, "kind": e.kind, "key": e.key, "ts": e.ts, "kid": e.kid,
             "attempts": e.meta.get("attempts"), "healed": e.meta.get("healed"),
             "error": e.meta.get("error"), "result": e.result if e.persisted else "[sensitive]",
             "changed": sorted(e.delta) + [f"-{k}" for k in e.removed]} for e in es]
    rows = [[str(d["seq"]), d["kind"], d["key"], _fmt_ts(d["ts"]),
             "" if d["attempts"] in (None, 1) else str(d["attempts"]), d["healed"] or "",
             ",".join(d["changed"]), _preview(d["error"] or d["result"])] for d in data]
    head = f"run {args.run_id}  status={run_status(es)}  entries={len(es)}\n\n"
    _out(args, data, head + _table(rows, ["SEQ", "KIND", "KEY", "TIME", "TRIES", "HEALED", "STATE", "RESULT"]))
    return 0


def cmd_state(j: Journal, args: argparse.Namespace) -> int:
    if not j.load(args.run_id):
        print(f"run {args.run_id!r} not found", file=sys.stderr)
        return 1
    print(json.dumps(j.state_at(args.run_id, args.seq), indent=2, default=str))
    return 0


def cmd_verify(j: Journal, args: argparse.Namespace) -> int:
    ids = sorted(j.runs()) if args.all else args.run_ids
    if not ids:
        print("nothing to verify (give run ids or --all)", file=sys.stderr)
        return 2
    results = {rid: j.verify(rid, require_signed=args.require_signed) for rid in ids}
    bad = [r for r, ok in results.items() if not ok]
    _out(args, results, "\n".join(f"{'OK  ' if ok else 'FAIL'}  {rid}" for rid, ok in results.items())
         + f"\n\n{len(ids) - len(bad)}/{len(ids)} verified")
    return 1 if bad else 0


def cmd_timers(j: Journal, args: argparse.Namespace) -> int:
    now = time.time()
    timers = j.pending_timers(due_before=now if args.due else None)
    data: List[Dict[str, Any]] = [{"run_id": r, "wake_at": w, "due": w <= now} for r, w in timers]
    _out(args, data, _table([[d["run_id"], _fmt_ts(d["wake_at"]), "yes" if d["due"] else ""] for d in data],
                            ["RUN", "WAKE AT", "DUE"]) if data else "no pending timers")
    return 0


def cmd_delete(j: Journal, args: argparse.Namespace) -> int:
    if not args.yes:
        print("refusing to delete without --yes (this is permanent)", file=sys.stderr)
        return 2
    for rid in args.run_ids:
        j.delete(rid)
        print(f"deleted {rid}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pylinxi", description="Inspect and manage pylinxi run journals.")
    p.add_argument("--journal", default=os.environ.get("PYLINXI_JOURNAL"),
                   help="journal URL (memory://, file://DIR, sqlite:///FILE, postgresql://...); "
                        "default $PYLINXI_JOURNAL")
    p.add_argument("--key", action="append", default=[], metavar="KID=SECRET",
                   help="signing key for verify (repeatable); default $PYLINXI_KEYS")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("runs", help="list runs")
    s.add_argument("--status", choices=["completed", "continued", "failed", "sleeping", "waiting", "open"])
    s.add_argument("--limit", type=int, default=100)
    s.set_defaults(fn=cmd_runs)

    s = sub.add_parser("show", help="show a run's journal")
    s.add_argument("run_id")
    s.set_defaults(fn=cmd_show)

    s = sub.add_parser("state", help="rebuild state, optionally as of an entry")
    s.add_argument("run_id")
    s.add_argument("--seq", type=int)
    s.set_defaults(fn=cmd_state)

    s = sub.add_parser("verify", help="verify hash chain and signatures")
    s.add_argument("run_ids", nargs="*")
    s.add_argument("--all", action="store_true")
    s.add_argument("--require-signed", action="store_true")
    s.set_defaults(fn=cmd_verify)

    s = sub.add_parser("timers", help="list runs waiting on timers")
    s.add_argument("--due", action="store_true", help="only timers that are due now")
    s.set_defaults(fn=cmd_timers)

    s = sub.add_parser("delete", help="permanently delete runs")
    s.add_argument("run_ids", nargs="+")
    s.add_argument("--yes", action="store_true")
    s.set_defaults(fn=cmd_delete)
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.journal:
        print("no journal: pass --journal URL or set PYLINXI_JOURNAL", file=sys.stderr)
        return 2
    keys = _parse_keys(args.key or [k for k in os.environ.get("PYLINXI_KEYS", "").split(",") if k])
    with open_journal(args.journal, secret=keys or None) as j:
        return int(args.fn(j, args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
