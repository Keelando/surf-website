#!/usr/bin/env bash
# Nightly git backup (07:17 UTC, after backup_crontab.sh at 07:15).
#
# 1. Commit the crontab dump on `main` — and nothing else. This used to be
#    `git add -A`, which is how a generated digest carrying a credential was
#    published unattended. Anything else left uncommitted is reported, not
#    committed: commit your own work.
# 2. Push `main` to GitHub only after the publication check passes on what
#    the push would add: the commits' messages, authors and diffs, and the
#    tracked tree. GitHub is the public surface.
# 3. Scan everything Caddy serves. `site/data/` changes every few minutes and
#    no commit ever contains it, so this is its nightly check.
# 4. Back up `main`, `dev` and tags to Forgejo (private). `dev` never goes to
#    GitHub.
#
# Exits non-zero if any step failed, so the log says which.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT" || exit 1
PY="$REPO_ROOT/.venv/bin/python"
SCAN="$REPO_ROOT/scripts/hooks/check_secrets.py"
status=0

echo "=== git backup $(date -u +%FT%TZ) ==="

if [[ "$(git branch --show-current)" != "main" ]]; then
  echo "ERROR: $REPO_ROOT is not on main; nothing done." >&2
  exit 1
fi

# ── 1. Commit the crontab dump ───────────────────────────────
git add config/crontab.txt
if ! git diff --cached --quiet; then
  if git commit -q -m "Auto-backup $(date +%Y-%m-%d)"; then
    echo "committed config/crontab.txt"
  else
    echo "ERROR: auto-backup commit failed (pre-commit hook); left staged." >&2
    status=1
  fi
fi

stray="$(git status --porcelain)"
if [[ -n "$stray" ]]; then
  echo "WARNING: production checkout has changes that were not committed:" >&2
  echo "$stray" | sed 's/^/  /' >&2
fi

# ── 2. Publish main to GitHub, gated ─────────────────────────
if [[ -n "$(git rev-list origin/main..main)" ]]; then
  if "$PY" "$SCAN" --strict --range origin/main..main && "$PY" "$SCAN" --strict --all; then
    git push -q origin main || status=1
  else
    echo "ERROR: publication check failed; main NOT pushed to GitHub." >&2
    status=1
  fi
fi

# ── 3. What the website serves ───────────────────────────────
if ! "$PY" "$SCAN" --strict --served; then
  echo "ERROR: publication check failed on files the site serves (above)." >&2
  status=1
fi

# ── 4. Private backup ────────────────────────────────────────
# Separate pushes: a down Forgejo must not stop the others, and `dev` is
# rebased onto main from time to time, so it needs a lease rather than a
# fast-forward.
git push -q forgejo main --tags || status=1
if git show-ref --verify --quiet refs/heads/dev; then
  git push -q --force-with-lease forgejo dev || status=1
fi

echo "=== done (status $status) ==="
exit "$status"
