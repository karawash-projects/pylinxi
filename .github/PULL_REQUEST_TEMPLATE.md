## What and why

<!-- What does this change, and what problem does it solve? Link the issue if there is one. -->

## Checklist

- [ ] Tests cover the change (a bug fix includes a test that fails without it)
- [ ] `python -m unittest discover -s tests -t .` passes
- [ ] `ruff check pylinxi tests examples && mypy pylinxi` passes
- [ ] Docs / README / CHANGELOG updated if behaviour changed
- [ ] Journals written by the previous release still load (if the journal format or replay changed)
