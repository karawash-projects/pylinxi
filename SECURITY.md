# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 1.1.x | ✅ |
| 1.0.x | Security fixes only, until 1.2.0 is released |
| < 1.0 | ❌ |

## Reporting a vulnerability

Please **don't open a public issue**. Report it privately through GitHub:
[Security → Report a vulnerability](https://github.com/karawash-projects/pylinxi/security/advisories/new).

Include what you found, how to reproduce it, and the impact you expect. You'll get
an acknowledgement within 3 business days and a plan within 10. Once a fix is
released, the advisory is published with credit to you unless you prefer
otherwise.

## In scope

Anything that breaks pylinxi's security guarantees, for example:

- forging, editing, reordering or copying journal entries without `verify()` or
  `load()` detecting it;
- secrets reaching the journal despite redaction or the secret-key checks;
- a worker that lost a run's lock still writing to it (fencing);
- path traversal or file clobbering through run ids in `FileJournal`;
- SQL injection through run ids, table names or signals.

## Out of scope

- Steps leaking data you chose to return from a non-sensitive step (step results
  are journaled by design; use `@step(sensitive=True)`).
- Attacks that require the journal's signing keys or write access to the
  process running your flows.
