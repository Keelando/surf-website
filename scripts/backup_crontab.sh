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

# Refuse to write jobs that belong to other projects on this host.
#
# `crontab -l` returns the WHOLE crontab for this user, and this script dumps it
# into a tracked file in a public repo. That is how an unrelated project's jobs
# — and ~56 lines of homelab notes describing them — ended up published on
# halibutbank.ca's GitHub (removed 2026-08-24). One user gets one crontab, so
# the fix is not to filter here but to keep foreign jobs OUT of the user
# crontab entirely: put them in /etc/cron.d/<name> with a `keelando` user field,
# which `crontab -l` cannot see. See system-issues/deployed/ for that pattern.
#
# Anything referencing a path outside the repo must be listed below, with a
# reason. If this fires on a job you just added, move it to /etc/cron.d instead
# of widening the allowlist.
#
# This scans /home paths of ANY extension, on ALL lines including comments.
# Both widenings are deliberate, and both were found the hard way on
# 2026-08-29: a minecraft bot section had been sitting in the crontab where
# only one of its three jobs (a .sh) tripped the old `.py|.sh` check -- a
# `/usr/bin/node .../farm-digest.js` job and a bare `cd /home/keelando/...`
# were both invisible to it. Commented-out jobs from the same project had
# already been published, because the old check skipped comment lines
# entirely. A comment naming a foreign path leaks it just as effectively as
# the job does.
#
# Only /home is scanned, not every absolute path: system binaries
# (/usr/bin/flock, /bin/bash) are outside the repo but are not anyone's
# project, and flagging them would make this unusable.
FOREIGN_ALLOWED=(
  /home/keelando/backup_surf.sh          # restic; logs into envcan_wave/logs/
  /home/keelando/psi_sample.sh           # host telemetry, tracked in ~/system-issues
  /home/keelando/sys_stats_mqtt.py       # host telemetry, tracked in ~/system-issues
  /home/keelando/.sys-venv/bin/python    # interpreter for sys_stats_mqtt.py above
  /home/keelando/.config/GeoIP.conf      # geoipupdate config; feeds site analytics
)

foreign=()
while IFS= read -r path; do
  # Bare REPO_ROOT (e.g. a `cd` into it) as well as anything beneath it.
  [[ "$path" == "$REPO_ROOT" || "$path" == "$REPO_ROOT"/* ]] && continue
  allowed=0
  for ok in "${FOREIGN_ALLOWED[@]}"; do
    [[ "$path" == "$ok" ]] && { allowed=1; break; }
  done
  (( allowed )) || foreign+=("$path")
done < <(grep -oE '/home/[^ "'"'"';|)&>]+' "$TMP" | sed 's/[.,;:]$//' | sort -u)

if (( ${#foreign[@]} > 0 )); then
  echo "ERROR: live crontab runs ${#foreign[@]} script(s) from outside this repo:" >&2
  printf '  %s\n' "${foreign[@]}" >&2
  echo "This file is tracked and the repo is public. Move the job to" >&2
  echo "/etc/cron.d/<name> (user field: keelando), or add it to FOREIGN_ALLOWED" >&2
  echo "in $0 with a reason. $CANONICAL left untouched." >&2
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
