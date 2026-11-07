"""Pacific time for the backend: the zone, and the label the website uses.

zoneinfo reads the system's tz data, which knows BC keeps UTC-7 all year from
2026-03-08 (tzdata 2026e). pytz did not: it ships its own copy of the
database (2025.2 here), which put Vancouver back on PST after 2026-11-01 —
never use it (tests/test_no_pytz.py).
"""

from datetime import timedelta
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Vancouver")


def pacific_label(local):
    """The zone name for a Pacific time: "PDT" at UTC-7, "PST" at UTC-8.

    Not %Z: up-to-date tz data names BC's permanent UTC-7 "MST", which reads
    as Mountain time. "PDT" matches the website (shared/format-time.js).
    """
    return "PDT" if local.utcoffset() == timedelta(hours=-7) else "PST"
