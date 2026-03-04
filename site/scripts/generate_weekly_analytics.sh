#!/bin/bash
#
# Generate weekly website analytics using GoAccess
# Usage: ./generate_weekly_analytics.sh [YYYY-MM-DD] [days]
#   YYYY-MM-DD: Start date (default: 7 days ago)
#   days: Number of days to include (default: 7)
#
# Examples:
#   ./generate_weekly_analytics.sh                    # Last 7 days
#   ./generate_weekly_analytics.sh 2025-12-01         # Week starting Dec 1
#   ./generate_weekly_analytics.sh 2025-12-01 14     # 2 weeks starting Dec 1
#

set -euo pipefail

# Configuration
CADDY_LOG="/var/log/caddy/halibutbank-access.log"
CONVERTER="/home/keelando/site/scripts/caddy_to_clf.py"
REPORT_OUTPUT="/home/keelando/site/analytics-weekly.html"
TEMP_LOG="/tmp/caddy-clf-weekly-$$.log"
TEMP_FILTERED="/tmp/caddy-filtered-$$.log"

# Parse arguments
START_DATE="${1:-$(date -d '7 days ago' '+%Y-%m-%d')}"
DAYS="${2:-7}"

# Calculate end date
END_DATE=$(date -d "$START_DATE + $DAYS days" '+%Y-%m-%d')

echo "Generating analytics for: $START_DATE to $END_DATE ($DAYS days)"

# Convert start/end dates to timestamps for filtering
START_TS=$(date -d "$START_DATE" '+%s')
END_TS=$(date -d "$END_DATE 23:59:59" '+%s')

# Filter Caddy JSON logs by timestamp range
# The 'ts' field in Caddy logs is a Unix timestamp
sudo cat "$CADDY_LOG" | python3 -c "
import json
import sys

start_ts = $START_TS
end_ts = $END_TS

for line in sys.stdin:
    try:
        data = json.loads(line.strip())
        ts = data.get('ts', 0)
        if start_ts <= ts <= end_ts:
            print(line.strip())
    except:
        pass
" > "$TEMP_FILTERED"

# Convert filtered logs to Combined Log Format
cat "$TEMP_FILTERED" | python3 "$CONVERTER" > "$TEMP_LOG"

# Count lines processed
LINE_COUNT=$(wc -l < "$TEMP_LOG")

if [ "$LINE_COUNT" -eq 0 ]; then
    echo "WARNING: No log entries found for this date range!"
    rm -f "$TEMP_LOG" "$TEMP_FILTERED"
    exit 1
fi

echo "Processing $LINE_COUNT log entries..."

# Generate GoAccess HTML report
goaccess "$TEMP_LOG" \
    --log-format=COMBINED \
    --output="$REPORT_OUTPUT" \
    --html-report-title="Weekly Analytics: $START_DATE to $END_DATE" \
    --no-query-string \
    --no-term-resolver \
    --anonymize-ip \
    --ignore-crawlers \
    2>/dev/null

# Clean up
rm -f "$TEMP_LOG" "$TEMP_FILTERED"

# Set permissions
chmod 644 "$REPORT_OUTPUT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Weekly analytics report generated: $REPORT_OUTPUT"
echo "View at: https://halibutbank.ca/analytics-weekly.html"
