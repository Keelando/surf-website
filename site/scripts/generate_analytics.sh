#!/bin/bash
#
# Generate website analytics using GoAccess
# Runs hourly via cron to create analytics dashboard
#
# Authentication: The analytics.html page is protected by Cloudflare Access
# at the edge. No local authentication is required - Cloudflare handles
# access control before requests reach this server.
#

set -euo pipefail

# Configuration
CADDY_LOG="/var/log/caddy/halibutbank-access.log"
CONVERTER="/home/keelando/site/scripts/caddy_to_clf.py"
REPORT_OUTPUT="/home/keelando/site/analytics.html"
TEMP_LOG="/tmp/caddy-clf-$$.log"

# Convert Caddy JSON logs to Combined Log Format
sudo cat "$CADDY_LOG" | python3 "$CONVERTER" > "$TEMP_LOG"

# Generate GoAccess HTML report (static, updates hourly via cron)
goaccess "$TEMP_LOG" \
    --log-format=COMBINED \
    --output="$REPORT_OUTPUT" \
    --html-report-title="Halibut Bank Analytics" \
    --no-query-string \
    --no-term-resolver \
    --anonymize-ip \
    --ignore-crawlers \
    2>/dev/null

# Clean up
rm -f "$TEMP_LOG"

# Set permissions
chmod 644 "$REPORT_OUTPUT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Analytics report generated: $REPORT_OUTPUT"
