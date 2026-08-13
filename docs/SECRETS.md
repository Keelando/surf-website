# Secrets

This repo is public. Nothing secret goes in a tracked file, ever.

## Where credentials live

`config/.env` — gitignored, never committed:

```bash
WINDY_API_KEY=<key>
SURREY_API_USERNAME=<username>
SURREY_API_PASSWORD=<password>
```

Read them with `lib/env.py`, which checks `os.environ` first and falls back
to the file, so cron jobs work without the crontab exporting anything:

```python
from lib.env import get_env, require_env

PASSWORD = require_env("SURREY_API_PASSWORD")  # raises if missing
WINDY_API_KEY = get_env("WINDY_API_KEY")       # None if missing
```

Other credential stores on this host, outside the repo:
`~/.config/sr3/credentials.conf` (Sarracenia).

## What leaked, and how

The Windy API key reached the public repo in early 2026. Nobody typed it into
a tracked file: a whole-repo digest tool (`._codebase_digest.txt`) inlined
`config/.env` into its output, and the 07:17 auto-backup cron committed and
pushed that file unattended. It was removed from `HEAD` but remains in
history, which is why it must never be reused. The key has since expired —
Windy's API returns HTTP 410 — and it is **not** being rotated out of history:
rewriting a public repo's history is disruptive and buys nothing for a dead
credential.

Two lessons shaped the defences below:

1. **The dangerous files are generated, not written.** Digests, dumps, logs
   and backups copy secrets into new paths. Ignoring one filename does not
   help; the next tool picks a different name.
2. **Unattended commits remove the human check.** The nightly backup commits
   whatever is staged, so the guard has to be automatic.

## Defences

| Layer | What it does |
|-------|--------------|
| `.gitignore` | `*.env`, `._codebase_digest.txt`, `config/crontab.txt.bak-*` |
| `scripts/hooks/check_secrets.py` | Scans staged content: any value from `config/.env`, plus JWTs, credential-shaped assignments, AWS keys, private-key blocks |
| `scripts/hooks/pre-commit` | Runs the scan first, before ruff/pytest/eslint |
| `tests/test_secrets.py` | Same scan over every tracked file — catches a `--no-verify` commit or an uninstalled hook |
| `scripts/backup_crontab.sh` | Refuses to dump a live crontab that assigns a credential-shaped variable |

Audit the whole tracked tree at any time:

```bash
.venv/bin/python scripts/hooks/check_secrets.py --all
```

Docs may show the *shape* of a credential line — `<password>`,
`your_key_here` and similar placeholders pass the scan deliberately.

If the scanner blocks something that is genuinely not a secret, add the path
to `SKIP_PATHS` in `check_secrets.py` rather than reaching for
`ALLOW_SECRETS=1`, so the exemption is reviewable.

## Rotating a key

1. Put the new value in `config/.env`. Confirm it is not tracked:
   `git check-ignore -v config/.env`
2. `.venv/bin/python scripts/hooks/check_secrets.py --all`
3. For Windy specifically, flip `WINDY_PUSH_ENABLED` back to `True` in
   `scripts/fetch/fetch_surrey_wave_v2.py` and re-check `WINDY_STATIONS`
   against the station ids the new account issues.
