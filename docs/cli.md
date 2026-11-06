# Command line

```bash
export PYLINXI_JOURNAL=postgresql://app@db/agents PYLINXI_KEYS_FILE=/run/secrets/pylinxi-keys

pylinxi runs --status failed            # completed|continued|failed|sleeping|waiting|open
pylinxi show t-1001                     # every entry: kind, key, tries, healed, changed state, result
pylinxi state t-1001 --seq 3            # time travel
pylinxi verify --all                    # exit 1 on any failure: use in CI or audits
pylinxi timers --due
pylinxi delete t-1001 --yes             # retention / right-to-erasure (all generations)
pylinxi --json runs                     # machine-readable
```

Keys come from `--key-file` / `$PYLINXI_KEYS_FILE` (`kid=secret` lines or a JSON object), `$PYLINXI_KEYS`, or `--key` (which warns: it shows up in process listings). Write binary keys, or keys containing commas, as `hex:…` or `base64:…`. When keys are given, `verify` requires every entry to be signed (`--allow-unsigned` while migrating). The CLI never creates a journal, so a mistyped path is an error rather than an empty "no runs".
