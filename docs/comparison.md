# pylinxi and LangGraph

LangGraph is mature and also offers a functional API (`@entrypoint`/`@task`), retry policies, caching and many checkpointers, plus LangSmith and a hosted platform. Choose pylinxi when you want:

- a small surface (two decorators, no graph or reducer schema), zero core dependencies, and code you can read in an afternoon;
- per-step validate → repair → retry → fallback → breaker as one policy;
- delta journals with signing, rotation, redaction and a verify CLI built in;
- durable timers, `patched()` and `continue_as_new()` for long-lived agents.

Choose LangGraph when you need its integrations ecosystem, LangSmith's tracing UI, or a managed runtime.

On certifications: SOC 2 / ISO 27001 apply to organizations and hosted services, not libraries. pylinxi makes audits easier — signed journals, redaction, no network calls, no core dependencies to vet.
