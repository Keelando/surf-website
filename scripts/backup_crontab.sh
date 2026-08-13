#!/usr/bin/env bash
# Dump the live crontab to config/crontab.txt — but only if it's valid.
#
# Validates that every script referenced in the live crontab exists on disk.
# If any are missing, writes the dump to config/crontab.txt.broken-<ts> and
# leaves the canonical config/crontab.txt untouched, so the auto-backup git
# commit won't pin a broken state into the repo.
#
# Designed to run from cron and be silent on success.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CANONICAL="$REPO_ROOT/config/crontab.txt"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

crontab -l > "$TMP" 2>/dev/null || {
  echo "ERROR: crontab -l failed; not touching $CANONICAL" >&2
  exit 1
}

# Refuse to write credentials into the canonical file. Someone editing via
# `crontab -e` can reintroduce a `KEY=value` line, and this script would
# otherwise dump it straight into a tracked, public file — which is how the
# Windy key leaked. Credentials belong in config/.env (see lib/env.py).
if grep -qE '^\s*[A-Z0-9_]*(API_KEY|APIKEY|PASSWORD|PASSWD|SECRET|TOKEN|CREDENTIAL)[A-Z0-9_]*\s*=\s*\S' "$TMP"; then
  echo "ERROR: live crontab assigns a credential-shaped variable." >&2
  grep -nE '^\s*[A-Z0-9_]*(API_KEY|APIKEY|PASSWORD|PASSWD|SECRET|TOKEN|CREDENTIAL)[A-Z0-9_]*\s*=' "$TMP" \
    | sed -E 's/=.*/=<redacted>/' >&2
  echo "Move it to config/.env (gitignored); $CANONICAL left untouched." >&2
  exit 1
fi

# Validate: every absolute /path/to/script.{py,sh} referenced must exist.
missing=()
while IFS= read -r path; do
  [[ -e "$path" ]] || missing+=("$path")
done < <(grep -vE '^\s*#|^\s*$' "$TMP" | grep -oE '/\S+\.(py|sh)' | sort -u)

if (( ${#missing[@]} > 0 )); then
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  broken="$CANONICAL.broken-$ts"
  cp "$TMP" "$broken"
  echo "ERROR: live crontab references ${#missing[@]} missing script(s):" >&2
  printf '  %s\n' "${missing[@]}" >&2
  echo "Dumped to $broken; $CANONICAL left untouched." >&2
  exit 1
fi

# Valid — replace canonical only if content differs (avoid touching mtime for no reason).
if ! diff -q "$TMP" "$CANONICAL" > /dev/null 2>&1; then
  cp "$TMP" "$CANONICAL"
fi
