# Health Monitoring System Plan

## Problem
Recent schema changes (wind_direction → wind_direction_deg) revealed gaps:
- No automated detection of stale data (Cherry Point down 4+ hours)
- Log file location mismatches went unnoticed
- Field name mismatches between database and code not caught
- Cron job failures silent (jobs run but produce no output)

## Proposed Health Check Script

**Location**: `scripts/monitoring/health_check.py`  
**Run frequency**: Every hour via cron  
**Output**: `/home/keelando/site/data/system_health.json`

### Checks to Implement

1. **Data Freshness**
   - Flag stations with data >2hrs old (warning)
   - Flag stations with data >4hrs old (error)
   - Check all databases: wind_data.sqlite, buoy_data.sqlite, tide_data.sqlite

2. **Database Schema Validation**
   - Verify expected columns exist in each table
   - Check for field name mismatches (e.g., wind_direction vs wind_direction_deg)
   - Validate data types match expectations

3. **Cron Job Monitoring**
   - Parse syslog for recent cron executions
   - Verify critical jobs ran in expected timeframe
   - Flag jobs that ran but produced no log output

4. **Log File Health**
   - Check log files exist at expected locations
   - Verify log files are being written to (recent mtime)
   - Flag size 0 logs that should have content
   - Compare cron redirect paths vs actual log paths

5. **Export File Freshness**
   - Verify JSON exports updated recently
   - Check export file sizes are reasonable (not empty, not huge)
   - Validate JSON is parseable

6. **Database Integrity**
   - Check database file sizes
   - Verify WAL mode is enabled
   - Count recent inserts (last hour)

### Output Format

```json
{
  "generated_utc": "2025-12-19T03:30:00Z",
  "overall_status": "warning",  // ok, warning, error
  "checks": {
    "data_freshness": {
      "status": "warning",
      "stale_stations": [
        {"id": "CPMW1", "name": "Cherry Point", "age_hours": 4.5}
      ],
      "total_stations": 17,
      "stale_count": 1
    },
    "schema_validation": {
      "status": "ok",
      "issues": []
    },
    "cron_jobs": {
      "status": "ok",
      "last_check": "2025-12-19T03:00:00Z",
      "jobs_checked": 15,
      "failed_jobs": []
    },
    "log_files": {
      "status": "warning",
      "mismatches": [
        {
          "script": "fetch_noaa_land.py",
          "cron_redirect": "/logs/noaa_land.log",
          "actual_log": "/lib/logs/noaa_land.log"
        }
      ]
    }
  }
}
```

### Optional: Alert Integration

- **Email**: Send alerts on errors (via sendmail/SMTP)
- **MQTT**: Publish health status to Home Assistant
- **Webhook**: POST to monitoring service
- **Desktop notification**: Use notify-send for critical issues

### Benefits

1. **Catch issues early** - Stale data detected within 1 hour
2. **Schema validation** - Field mismatches caught immediately after migration
3. **Cron monitoring** - Silent failures become visible
4. **Dashboard ready** - JSON output can power a status page
5. **Refactor confidence** - Know immediately if changes break data pipeline

## Implementation Priority

1. **Phase 1** (Next session): Data freshness + basic cron monitoring
2. **Phase 2**: Schema validation + log file checks
3. **Phase 3**: Alert integration + status dashboard

## Notes

- Keep check script lightweight (<5s runtime)
- Cache results to avoid repeated expensive checks
- Add `--verbose` flag for debugging
- Include self-check (verify health script ran recently)
