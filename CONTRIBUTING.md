# Contributing to pylinxi

Thanks for helping! Bug reports with a reproduction script are the most valuable
contribution of all — see the issue templates.

## Setup

```bash
git clone https://github.com/karawash-projects/pylinxi && cd pylinxi
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev,docs]"
```

## Checks

```bash
python -m unittest discover -s tests -t .      # Postgres tests skip without a DSN
PYLINXI_PG_DSN=postgresql://user@localhost/db python -m unittest discover -s tests -t .
ruff check pylinxi tests examples && mypy pylinxi
for f in examples/*.py; do python "$f" || break; done
mkdocs serve                                     # docs at http://127.0.0.1:8000
```

CI runs all of this on Python 3.9–3.13 with a real Postgres, plus a job that
checks journals written by the previous release still load.

## What makes a change easy to accept

- **A test that fails without it.** Replay bugs especially: show the side effect
  that ran twice, or the result that was lost, then fix it. Tests that exercise
  journals should run on every backend (see `_journals()` in
  `tests/test_review_1_1.py`).
- **Small and focused.** One fix or feature per pull request.
- **No new core dependencies.** The core stays dependency-free; integrations go
  behind an optional extra.
- **Compatibility.** Journals written by earlier 1.x releases must keep loading,
  verifying and resuming. Behaviour changes go in `CHANGELOG.md` (and "Upgrading"
  in the docs if users must act).
- **Docs.** Update `README.md` / `docs/` when you change behaviour users see.

## Design principles

1. A finished step never runs twice. A change that risks this needs a very good
   reason and tests on every journal.
2. Flows are plain Python. No graph DSL, no required base classes.
3. Fail loudly. Divergence, corruption and unjournalable results raise instead of
   silently doing something else.

## Releasing (maintainers)

See [Development → Releasing](https://karawash-projects.github.io/pylinxi/contributing/#releasing).
