#!/bin/bash
#
# Analytics Helper - Easy weekly/monthly analytics generation
# Usage: ./analytics-helper.sh [option]
#
# Options:
#   last-week       - Last 7 days (default)
#   this-week       - Current week (Mon-Sun)
#   last-month      - Last 30 days
#   this-month      - Current calendar month
#   custom          - Prompt for custom date range
#   list-weeks      - Show available weeks in logs
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEEKLY_SCRIPT="$SCRIPT_DIR/generate_weekly_analytics.sh"

# Function to generate report
generate() {
    local start_date=$1
    local days=$2
    local description=$3

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Generating: $description"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    "$WEEKLY_SCRIPT" "$start_date" "$days"
}

# Parse option
OPTION="${1:-last-week}"

case "$OPTION" in
    last-week)
        START=$(date -d '7 days ago' '+%Y-%m-%d')
        generate "$START" 7 "Last 7 Days"
        ;;

    this-week)
        # Get Monday of current week
        START=$(date -d 'last monday' '+%Y-%m-%d')
        DAYS=$(( ($(date +%s) - $(date -d "$START" +%s)) / 86400 + 1 ))
        generate "$START" "$DAYS" "This Week (Mon-Today)"
        ;;

    last-month)
        START=$(date -d '30 days ago' '+%Y-%m-%d')
        generate "$START" 30 "Last 30 Days"
        ;;

    this-month)
        # Get first day of current month
        START=$(date -d "$(date +%Y-%m-01)" '+%Y-%m-%d')
        DAYS=$(date +%d)
        generate "$START" "$DAYS" "This Month ($(date +%B))"
        ;;

    list-weeks)
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Available Weeks in Logs"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "To generate a specific week, use:"
        echo "  $WEEKLY_SCRIPT YYYY-MM-DD [days]"
        echo ""
        echo "Recent weeks:"

        # Show last 8 weeks
        for i in {0..7}; do
            WEEK_START=$(date -d "$(( i * 7 )) days ago last monday" '+%Y-%m-%d')
            WEEK_END=$(date -d "$WEEK_START + 6 days" '+%Y-%m-%d')

            # Count log entries for this week
            START_TS=$(date -d "$WEEK_START" '+%s')
            END_TS=$(date -d "$WEEK_END 23:59:59" '+%s')

            COUNT=$(sudo cat /var/log/caddy/halibutbank-access.log | python3 -c "
import json, sys
count = 0
for line in sys.stdin:
    try:
        data = json.loads(line.strip())
        if $START_TS <= data.get('ts', 0) <= $END_TS:
            count += 1
    except: pass
print(count)
" 2>/dev/null || echo "0")

            if [ "$COUNT" -gt 0 ]; then
                if [ "$i" -eq 0 ]; then
                    echo "  $WEEK_START  (This week) - $COUNT requests"
                else
                    echo "  $WEEK_START  ($i weeks ago) - $COUNT requests"
                fi
            fi
        done
        ;;

    custom)
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Custom Date Range"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        read -p "Start date (YYYY-MM-DD): " START_DATE
        read -p "Number of days: " DAYS

        generate "$START_DATE" "$DAYS" "Custom: $START_DATE + $DAYS days"
        ;;

    *)
        echo "Analytics Helper - Generate weekly/monthly analytics"
        echo ""
        echo "Usage: $0 [option]"
        echo ""
        echo "Options:"
        echo "  last-week       - Last 7 days"
        echo "  this-week       - Current week (Mon-Today)"
        echo "  last-month      - Last 30 days"
        echo "  this-month      - Current calendar month"
        echo "  custom          - Custom date range (interactive)"
        echo "  list-weeks      - Show available weeks"
        echo ""
        echo "Examples:"
        echo "  $0 last-week"
        echo "  $0 this-month"
        echo "  $0 custom"
        exit 1
        ;;
esac
