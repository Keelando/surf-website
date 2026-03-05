#!/bin/bash
#
# Generate website analytics using GoAccess
# Runs every 6 hours via cron to create analytics dashboard
#
# Authentication: The analytics.html page is protected by Cloudflare Access
# at the edge. No local authentication is required - Cloudflare handles
# access control before requests reach this server.
#

set -euo pipefail

# Configuration
CADDY_LOG="/var/log/caddy/halibutbank-access.log"
CONVERTER="/home/keelando/envcan_wave/site/scripts/caddy_to_clf.py"
REPORT_OUTPUT="/home/keelando/envcan_wave/site/analytics.html"
TEMP_LOG="/tmp/caddy-clf-$$.log"
DAYS=30

# Filter to last N days and convert to Combined Log Format.
# Reading JSON and checking 'ts' (Unix timestamp) is fast even on large files.
CUTOFF_TS=$(date -d "$DAYS days ago" +%s)

{
    # Logrotate compressed archives (.log.2.gz, .log.3.gz, ... oldest first)
    for f in $(ls "${CADDY_LOG}".*.gz 2>/dev/null | sort -V); do
        zcat "$f"
    done
    # Old Caddy-format archives (before logrotate was set up), oldest first
    for f in $(ls /var/log/caddy/halibutbank-access-*.log.gz 2>/dev/null | sort); do
        zcat "$f"
    done
    # Logrotate .log.1 (yesterday, uncompressed due to delaycompress)
    [ -f "${CADDY_LOG}.1" ] && cat "${CADDY_LOG}.1"
    # Current log
    cat "$CADDY_LOG"
} | python3 -c "
import json, sys
cutoff = $CUTOFF_TS
for line in sys.stdin:
    try:
        d = json.loads(line)
        if d.get('ts', 0) < cutoff:
            continue
        ua = d.get('request', {}).get('headers', {}).get('User-Agent', [''])[0]
        if 'Uptime-Kuma' in ua:
            continue
        uri = d.get('request', {}).get('uri', '')
        if uri.startswith('/.git') or uri.startswith('/.env') or uri in ('/wp-login.php', '/xmlrpc.php'):
            continue
        sys.stdout.write(line)
    except Exception:
        pass
" | python3 "$CONVERTER" > "$TEMP_LOG"

LINE_COUNT=$(wc -l < "$TEMP_LOG")
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Filtered to $LINE_COUNT entries (last ${DAYS} days)"

# Generate GoAccess HTML report
goaccess "$TEMP_LOG" \
    --log-format=COMBINED \
    --output="$REPORT_OUTPUT" \
    --html-report-title="Halibut Bank Analytics (Last ${DAYS} Days)" \
    --no-query-string \
    --no-term-resolver \
    --ignore-crawlers \
    2>/dev/null

# Clean up
rm -f "$TEMP_LOG"

chmod 644 "$REPORT_OUTPUT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Analytics report generated: $REPORT_OUTPUT"
