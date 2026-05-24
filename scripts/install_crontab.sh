#!/usr/bin/env bash
# Install config/crontab.txt as the live crontab, with safety checks.
#
# - Snapshots the current live crontab to ~/crontab_backups/ first
# - Validates that every script referenced in config/crontab.txt exists
# - Refuses to install if validation fails (use --force to override)
#
# Usage: scripts/install_crontab.sh [--force] [--diff-only]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CRONTAB_FILE="$REPO_ROOT/config/crontab.txt"
BACKUP_DIR="$HOME/crontab_backups"

FORCE=0
DIFF_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    --diff-only) DIFF_ONLY=1 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

if [[ ! -f "$CRONTAB_FILE" ]]; then
  echo "ERROR: $CRONTAB_FILE not found" >&2
  exit 1
fi

# Validate: every absolute /path/to/script.{py,sh} must exist.
missing=()
while IFS= read -r path; do
  [[ -e "$path" ]] || missing+=("$path")
done < <(grep -vE '^\s*#|^\s*$' "$CRONTAB_FILE" | grep -oE '/\S+\.(py|sh)' | sort -u)

if (( ${#missing[@]} > 0 )); then
  echo "ERROR: $CRONTAB_FILE references ${#missing[@]} missing script(s):" >&2
  printf '  %s\n' "${missing[@]}" >&2
  if (( FORCE == 0 )); then
    echo "Re-run with --force to install anyway." >&2
    exit 1
  fi
  echo "WARNING: --force given, installing despite missing scripts." >&2
fi

# Diff against current live crontab.
echo "=== Diff (live → $CRONTAB_FILE) ==="
if diff -u <(crontab -l 2>/dev/null || true) "$CRONTAB_FILE"; then
  echo "No changes — live crontab already matches $CRONTAB_FILE"
  exit 0
fi

if (( DIFF_ONLY == 1 )); then
  exit 0
fi

# Snapshot.
mkdir -p "$BACKUP_DIR"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
backup="$BACKUP_DIR/crontab.${ts}.bak"
crontab -l > "$backup" 2>/dev/null || true
echo "Backed up live crontab → $backup"

# Install.
crontab "$CRONTAB_FILE"
echo "Installed $CRONTAB_FILE as live crontab."
