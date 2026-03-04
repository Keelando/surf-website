#!/bin/bash
#
# Generate website analytics for a specific date range using GoAccess
#
# Usage:
#   ./generate_analytics_range.sh [days]
#
# Examples:
#   ./generate_analytics_range.sh          # Default: last 7 days
#   ./generate_analytics_range.sh 1        # Last 24 hours
#   ./generate_analytics_range.sh 30       # Last 30 days
#   ./generate_analytics_range.sh 90       # Last 90 days
#

set -euo pipefail

# Configuration
CADDY_LOG="/var/log/caddy/halibutbank-access.log"
CONVERTER="/home/keelando/site/scripts/caddy_to_clf.py"
REPORT_OUTPUT="/home/keelando/site/analytics.html"
TEMP_LOG="/tmp/caddy-clf-$$.log"
TEMP_FILTERED="/tmp/caddy-filtered-$$.log"

# Default to last 7 days if no argument provided
DAYS=${1:-7}

echo "Generating analytics for last $DAYS days..."

# Calculate the date threshold (YYYY-MM-DD format)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    DATE_THRESHOLD=$(date -v-${DAYS}d '+%Y-%m-%d')
else
    # Linux
    DATE_THRESHOLD=$(date -d "$DAYS days ago" '+%Y-%m-%d')
fi

echo "Including data from $DATE_THRESHOLD onwards..."

# Convert Caddy JSON logs to Combined Log Format
sudo cat "$CADDY_LOG" | python3 "$CONVERTER" > "$TEMP_LOG"

# Filter by date range
# Combined Log Format date pattern: [DD/MMM/YYYY:HH:MM:SS ...]
# We need to convert DATE_THRESHOLD to this format and filter

# Convert threshold to timestamp for easier comparison
THRESHOLD_TS=$(date -d "$DATE_THRESHOLD" +%s)

# Filter log entries by date
awk -v threshold="$THRESHOLD_TS" '
{
    # Extract date from Combined Log Format: [20/Nov/2025:14:30:00 +0000]
    if (match($0, /\[([0-9]{2})\/([A-Za-z]{3})\/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2})/, arr)) {
        day = arr[1]
        month = arr[2]
        year = arr[3]
        hour = arr[4]
        min = arr[5]
        sec = arr[6]

        # Convert month name to number
        months = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec"
        month_num = (index(months, month) + 3) / 4

        # Build date string for date command
        date_str = sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month_num, day, hour, min, sec)

        # Convert to timestamp (using system date command)
        cmd = "date -d \"" date_str "\" +%s 2>/dev/null"
        cmd | getline log_ts
        close(cmd)

        # Compare and print if within range
        if (log_ts >= threshold) {
            print $0
        }
    }
}' "$TEMP_LOG" > "$TEMP_FILTERED"

# Count how many lines we have
LINE_COUNT=$(wc -l < "$TEMP_FILTERED")
echo "Found $LINE_COUNT log entries in the specified date range"

if [ "$LINE_COUNT" -eq 0 ]; then
    echo "Warning: No log entries found for the specified date range!"
    echo "The analytics page will be empty or show 'No valid data' message."
fi

# Generate GoAccess HTML report
goaccess "$TEMP_FILTERED" \
    --log-format=COMBINED \
    --output="$REPORT_OUTPUT" \
    --html-report-title="Halibut Bank Analytics (Last $DAYS Days)" \
    --no-query-string \
    --no-term-resolver \
    --anonymize-ip \
    --ignore-crawlers \
    2>/dev/null

# Clean up
rm -f "$TEMP_LOG" "$TEMP_FILTERED"

# Set permissions
chmod 644 "$REPORT_OUTPUT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Analytics report generated: $REPORT_OUTPUT"
echo "Date range: $DATE_THRESHOLD to $(date '+%Y-%m-%d')"
echo "Total entries processed: $LINE_COUNT"
