# Secrets

This repo is public. Nothing secret goes in a tracked file, ever, and
neither do host or network details (see "Not only credentials" below).

## Two public surfaces

Easy to think of "public" as meaning git. It doesn't — there are two surfaces,
and they fail differently:

| Surface | What's public | Guarded by |
|---------|---------------|------------|
| **The git repo** | Every *tracked* file, and every commit's message and author lines | `.gitignore`, the pre-commit and commit-msg scans, `tests/test_secrets.py`, and the publication check before the nightly push to GitHub |
| **`site/`** | Everything Caddy serves at halibutbank.ca, tracked or not | `check_secrets.py --served`, nightly |

The nightly `scripts/git_backup.sh` runs the full publication check before
it pushes `main` to GitHub. A manual `git push origin main` does not (yet,
see TODO), so run `--range origin/main..` first when pushing by hand. `dev`
goes only to the private Forgejo remote.

The dangerous overlap is `site/data/`: it is **gitignored** — so every
git-based defence above is blind to it — while every file in it is fetchable
by name from the live site. Directory listing is off, but that is not a
boundary: the frontend JS names these files, so they are all discoverable in
the page source.

The rule that follows: **never write an upstream API response into
`site/data/` wholesale.** Copy an explicit allowlist of fields. `lib/windy.py`
is the worked example — the Windy read endpoint echoes station passwords, so
it returns a fixed tuple of safe fields and the health check keeps Windy out
of the published report entirely.

## Where credentials live

`config/.env` — gitignored, never committed:

```bash
SURREY_API_USERNAME=<username>
SURREY_API_PASSWORD=<password>

# Windy Stations API v2 — one identifier/password pair per station,
# for CRPILE, CRCHAN and COLEB.
WINDY_<STATION>_ID=<windy station identifier>
WINDY_<STATION>_PASSWORD=<station password>
```

`WINDY_API_KEY` is the retired account-wide upload key: dead since Windy's
January 2026 API change, kept in `config/.env` only so the scanner keeps
blocking it from re-entering a tracked file.

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
`config/.env` into its output, and the 07:17 auto-backup cron (then a bare
`git add -A`) committed and pushed that file unattended. It was removed from `HEAD` but remains in
history, which is why it must never be reused. The key has since expired —
Windy's API returns HTTP 410 — and it is **not** being rotated out of history:
rewriting a public repo's history is disruptive and buys nothing for a dead
credential.

Separately, credentials sat in `config/crontab.txt` — tracked and public — as
plain `VAR=value` lines, which is how cron exported them to the fetch scripts.
Commit `1442565` (2026-02-01) removed the file and gitignored it; `lib/env.py`
replaced the ad-hoc parsing it fed (one `.env` reader and three copies of
`_require_env`). Three values were exposed, and all three are still the
current ones, but neither needs rotating:

- `WINDY_API_KEY` — the same v1 key described above. Expired; Windy returns
  HTTP 410. Kept in `config/.env` only so the scanner keeps blocking it from
  re-entering a tracked file.
- `SURREY_API_USERNAME` / `SURREY_API_PASSWORD` — published on Surrey's own
  website. Moving them out of the repo was hygiene, not an incident.

Two lessons shaped the defences below:

1. **The dangerous files are generated, not written.** Digests, dumps, logs
   and backups copy secrets into new paths. Ignoring one filename does not
   help; the next tool picks a different name.
2. **Unattended commits remove the human check.** The nightly backup used to
   commit whatever was in the tree, so the guard has to be automatic. It now
   commits only `config/crontab.txt`, and scans before it pushes.

## Not only credentials

Hostnames, private addresses, tunnel details and the names of other
services on the host are not secrets, but publishing them tells an attacker
which doors exist. The scanner catches the generic shapes (private IPv4
ranges, `.local`/`.lan`/`.internal` hostnames) and every term in
`config/publish_denylist.txt`: one term per line, case-insensitive, `re:`
for a regex. That file is gitignored, since a tracked list would publish
exactly what it protects, and findings name a term by its line number rather
than reprinting it.

A sweep of the full history on 2026-09-29 found such details in old commits
and messages (a LAN address, hostnames, another service's name), all since
removed from the tree. They stay in history: rewriting a public repo's
history is disruptive and would not unpublish anything already cloned.

## Defences

| Layer | What it does |
|-------|--------------|
| `.gitignore` | `*.env`, `config/publish_denylist.txt`, `._codebase_digest.txt`, `config/crontab.txt.bak-*` |
| `scripts/hooks/check_secrets.py` | Scans staged content: any value from `config/.env`; JWTs, credential-shaped assignments, AWS keys, private-key blocks; private addresses and hostnames; denylisted terms |
| `scripts/hooks/pre-commit` | Runs the scan first, before ruff/pytest/eslint |
| `scripts/hooks/commit-msg` | The same scan over the commit message |
| `tests/test_secrets.py` | Same scan over every tracked file — catches a `--no-verify` commit or an uninstalled hook |
| `scripts/backup_crontab.sh` | Refuses to dump a live crontab that assigns a credential-shaped variable, runs a job from outside the repo, or fails the scan |
| `scripts/git_backup.sh` | Before the nightly push to GitHub: `--range` over every commit being pushed (messages, authors, each commit's added lines), `--all`, `--served`, all `--strict` |
| `check_secrets.py --served` | Scans everything Caddy serves from `site/`, including gitignored `site/data/`; nightly |

Audit any surface at any time:

```bash
S=scripts/hooks/check_secrets.py
.venv/bin/python $S --strict --all                  # tracked tree
.venv/bin/python $S --strict --served               # what the web sees
.venv/bin/python $S --strict --range origin/main..  # unpushed commits
.venv/bin/python $S --strict --range HEAD           # all of history
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
3. For Windy specifically, add both halves of each station's pair
   (`WINDY_<STATION>_ID`, `WINDY_<STATION>_PASSWORD`), confirm each station's
   name, position and elevation under My Stations on windy.com — the v2
   update endpoint sends measurements only and cannot set them — then flip
   `WINDY_PUSH_ENABLED` to `True` in `lib/windy.py` (both the pusher and the
   health check read it from there).
