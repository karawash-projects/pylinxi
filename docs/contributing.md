# Development

```bash
git clone https://github.com/karawash-projects/pylinxi && cd pylinxi
pip install -e ".[dev]"
python -m unittest discover -s tests -t .                            # Postgres tests skip without a DSN
PYLINXI_PG_DSN=postgresql://user@localhost/db python -m unittest discover -s tests -t .
ruff check pylinxi tests examples && mypy pylinxi
python examples/support_agent.py
```

CI runs lint, type checks, and the full suite with a Postgres service on Python 3.9–3.13.

## Releasing

Bump `version` in `pyproject.toml` and `__version__` in `pylinxi/__init__.py`, add a `## X.Y.Z` section to `CHANGELOG.md`, then:

```bash
git tag vX.Y.Z && git push origin vX.Y.Z
```

(or on GitHub: **Releases → Draft a new release → Choose a tag → type `vX.Y.Z` → Create new tag → Publish release**.)

The `release` workflow checks the tag matches the version, runs the tests, builds, publishes to PyPI via trusted publishing (no API token), and creates a GitHub Release with the changelog notes and built files.
