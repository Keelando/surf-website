# Stability Audit (Planned)

A periodic review to catch slow-burn issues before they cause outages.

## Checklist

- [ ] Verify all cron jobs are running and producing fresh output (check log mtimes)
- [ ] Check disk usage on `/` and `/mnt/storage` (webcam archives)
- [ ] SQLite integrity check on all databases (`PRAGMA integrity_check`)
- [ ] Confirm sr3 subscriptions are active and receiving data
- [ ] Review log sizes and logrotate effectiveness across all logs
- [ ] Check for stale JSON exports (data freshness vs. cron schedule)
- [ ] Verify Uptime-Kuma alerts are firing correctly (test a deliberate outage)
- [ ] Review Caddy error logs for recurring 5xx or unexpected patterns
- [ ] Confirm webcam archives are rotating properly and not filling `/mnt/storage`
