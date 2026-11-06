"""Order checkout as a saga: reserve stock, charge the card, ship. If shipping fails
for good, the flow compensates (refund, release stock) in plain `except` code. The
worker crashes right after charging the card; the resume does NOT charge again.
Run:  python examples/checkout_saga.py
"""
import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pylinxi import Policy, Retry, SQLiteJournal, StepFailed, flow, idempotency_key, step  # noqa: E402

journal = SQLiteJournal(str(Path(tempfile.mkdtemp()) / "orders.db"), secret=b"rotate-me")
ledger = []                     # what really happened in the outside world
crash = {"after_charge": True}
couriers_down = {"ORD-2"}


class WorkerKilled(BaseException):
    """Not an Exception, so nothing catches it -- like the process being killed."""


class CourierRejected(Exception):
    """Permanent: retrying won't help."""


@step
def reserve(order_id: str, sku: str, qty: int) -> str:
    ledger.append(f"reserve {qty}x{sku} for {order_id}")
    return f"rsv-{order_id}"


@step(policy=Policy(retry=Retry(attempts=3, backoff=0.01)))
def charge(order_id: str, cents: int) -> str:
    # The payment provider dedupes on this key, so even a retry after a timeout
    # can't charge twice; pylinxi makes sure we don't even send it twice.
    ledger.append(f"charge {cents}c ({idempotency_key()})")
    return f"ch-{order_id}"


@step(policy=Policy(retry=Retry(attempts=3, backoff=0.01, on=(TimeoutError,))))
def ship(order_id: str) -> str:
    if order_id in couriers_down:
        raise CourierRejected("no courier serves this address")
    ledger.append(f"ship {order_id}")
    return f"trk-{order_id}"


@step
def refund(charge_id: str) -> str:
    ledger.append(f"refund {charge_id}")
    return f"rf-{charge_id}"


@step
def release(reservation: str) -> str:
    ledger.append(f"release {reservation}")
    return reservation


@flow(journal=journal)
async def checkout(ctx, order_id: str, sku: str, qty: int, cents: int):
    reservation = await reserve(order_id, sku, qty)
    charge_id = await charge(order_id, cents)
    if crash["after_charge"]:
        crash["after_charge"] = False
        raise WorkerKilled("power cut right after the charge")
    try:
        tracking = await ship(order_id)
    except StepFailed:
        await refund(charge_id)                 # compensations are just steps too
        await release(reservation)
        ctx.state["status"] = "cancelled"
        return {"order": order_id, "status": "cancelled"}
    ctx.state["status"] = "shipped"
    return {"order": order_id, "status": "shipped", "tracking": tracking}


async def main() -> None:
    print("ORD-1: the worker dies right after charging the card")
    try:
        await checkout.run("ORD-1", "mug", 2, 2400, run_id="ORD-1")
    except WorkerKilled as e:
        print("  crashed:", e)
    r = await checkout.resume("ORD-1")           # e.g. a Worker on another machine
    print("  resumed ->", r.output)
    assert sum(x.startswith("charge") for x in ledger) == 1, "charged twice!"

    print("ORD-2: shipping is rejected -> refund and release, exactly once each")
    r = await checkout.run("ORD-2", "lamp", 1, 5900, run_id="ORD-2")
    print("  ->", r.output)
    assert r.output["status"] == "cancelled"

    print("\nWhat happened in the outside world:")
    for line in ledger:
        print("  ", line)
    assert journal.verify("ORD-1") and journal.verify("ORD-2")
    print("journals verify (signed, hash-chained): OK")


if __name__ == "__main__":
    asyncio.run(main())
