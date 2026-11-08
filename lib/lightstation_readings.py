"""What counts as a lightstation *reading*, for every query that asks.

A row can exist without saying anything. Until 2026-09-23 the SXCN parser stored
"NA" / "UNAVAILABLE" entries as rows with every value NULL, and those rows made
Chrome, Entrance and Merry Island look as if they reported on a regular cycle:
the page showed "12h ago" for a report that contained nothing, the schedule
inference counted the empty slots, and the health check counted them as fresh.

The parser no longer writes such rows, but a new spelling of "no reading" would
bring them straight back, so the consumers do not trust row existence either:
the latest-report export, the schedule inference and the health check all
filter on this one predicate.

An FPCN61 line like "PINE ISLAND. VISIBILITY ZERO." is a real report that
states no wind or sea. It fails this test too, which is the right answer for
all three consumers: there is nothing in it to display or chart, and the
visibility it does state is not stored.
"""

# SQL fragment, for a WHERE clause on lightstation_observation.
HAS_READING_SQL = (
    "(wind_speed_kt IS NOT NULL OR wind_calm = 1 OR wind_direction IS NOT NULL"
    " OR sea_height_ft IS NOT NULL OR sea_condition IS NOT NULL"
    " OR swell_intensity IS NOT NULL OR swell_direction IS NOT NULL)"
)


def has_reading(row):
    """Python twin of HAS_READING_SQL, for a parsed row (dict) before it is
    stored. Keep the two in step; tests/test_lightstation_readings.py checks
    them against each other."""
    return (
        row.get("wind_speed_kt") is not None
        or row.get("wind_calm") == 1
        or row.get("wind_direction") is not None
        or row.get("sea_height_ft") is not None
        or row.get("sea_condition") is not None
        or row.get("swell_intensity") is not None
        or row.get("swell_direction") is not None
    )
