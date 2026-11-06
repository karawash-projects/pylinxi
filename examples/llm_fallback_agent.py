"""An LLM classifier that heals itself: retries with backoff, validates the output,
feeds the parse error back into the prompt, and falls back to a cheaper model when
the primary one keeps failing. A shared circuit breaker stops hammering a provider
that is down. Run:  python examples/llm_fallback_agent.py
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pylinxi import CircuitBreaker, MemoryJournal, Policy, Retry, flow, step  # noqa: E402

calls = {"primary": 0, "fallback": 0}


class ProviderDown(Exception):
    """A transient provider error (rate limit, 503, ...)."""


# --- pretend models ------------------------------------------------------------
def primary_model(prompt: str) -> str:
    calls["primary"] += 1
    if "outage" in prompt:
        raise ProviderDown("503 from primary")
    if "Previous answer was not valid JSON" not in prompt:
        return "Sure! {category: billing"           # chatty, broken JSON on the first try
    return json.dumps({"category": "billing" if "invoice" in prompt else "other"})


def cheap_model(prompt: str) -> str:
    calls["fallback"] += 1
    return json.dumps({"category": "other", "model": "cheap"})


# --- healing hooks -------------------------------------------------------------
def is_json_object(text: str) -> bool:
    try:
        return isinstance(json.loads(text), dict)
    except ValueError:
        return False


def tell_the_model(error, args, kwargs, attempt):
    """repair: rewrite the prompt for the next attempt using the error."""
    (prompt,) = args
    return (f"{prompt}\nPrevious answer was not valid JSON ({type(error).__name__}). Reply with JSON only.",), kwargs


primary_breaker = CircuitBreaker(threshold=3, reset_after=30.0)   # one per dependency


@step(policy=Policy(
    retry=Retry(attempts=3, backoff=0.01, on=(ProviderDown,)),
    validate=is_json_object,
    repair=tell_the_model,
    fallback=cheap_model,
    breaker=primary_breaker,
    timeout=5.0,
))
def classify(prompt: str) -> str:
    return primary_model(prompt)


@flow(journal=MemoryJournal())
async def triage(ctx, ticket: str):
    label = json.loads(await classify(f"Classify this ticket: {ticket}"))
    ctx.state["label"] = label
    return label


def show(event: dict) -> None:
    if event["event"] in ("attempt_failed", "repaired", "fallback", "breaker_open", "completed"):
        detail = event.get("error") or event.get("healed") or ""
        print(f"  {event['event']:15} {event.get('key', '')} {detail}"[:110])


async def main() -> None:
    print("1) malformed output -> validator rejects -> repair rewrites the prompt")
    r = await triage.run("My invoice is wrong", run_id="t1", on_event=show)
    print("  ->", r.output)
    assert r.output == {"category": "billing"}

    print("2) provider outage -> retries exhausted -> fallback model answers")
    r = await triage.run("outage: cannot log in", run_id="t2", on_event=show)
    print("  ->", r.output)
    assert r.output["model"] == "cheap"

    print("3) replay: a finished run returns the journaled answer without calling any model")
    before = dict(calls)
    r = await triage.resume("t1")
    assert r.output == {"category": "billing"} and calls == before
    print("  -> no model calls on replay:", calls)


if __name__ == "__main__":
    asyncio.run(main())
