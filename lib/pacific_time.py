"""Pacific time for the backend: the zone, and the label the website uses.

zoneinfo reads the system's tz data, which knows BC keeps UTC-7 all year from
2026-03-08 (tzdata 2026e). pytz did not: it ships its own copy of the
database (2025.2 here), which put Vancouver back on PST after 2026-11-01 —
never use it (tests/test_no_pytz.py).
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Vancouver")

# 02:00 PST, 2026-03-08: BC's last clock change, into year-round UTC-7.
BC_FIXED_UTC7_FROM = datetime(2026, 3, 8, 10, tzinfo=timezone.utc)


def pacific_label(local):
    """The zone name for a Pacific time: "PCT" from 2026-03-08, before that
    "PDT" at UTC-7 and "PST" at UTC-8.

    PCT ("Pacific time") is the BC government's name for the year-round zone;
    it replaces PST/PDT. Not %Z: up-to-date tz data names it "MST", which
    reads as Mountain time. Matches the website (shared/format-time.js).
    """
    if local >= BC_FIXED_UTC7_FROM:
        return "PCT"
    return "PDT" if local.utcoffset() == timedelta(hours=-7) else "PST"
