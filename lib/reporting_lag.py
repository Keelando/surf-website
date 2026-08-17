#!/usr/bin/env python3
"""
Reporting-lag tracking: how long an observation takes to get from the
instrument to the website.

WHY A SEPARATE DATABASE
    Lag is a cross-source question ("is 46131 slower than usual, and is it
    slower than the wind stations?"), so it cannot live inside any one
    source's database. Keeping it separate also means writers keep opening
    their own observation databases exactly as they do today.

THE TWO COMPONENTS
    "Lag" is not one number. An observation passes through three clocks:

        observation_time  instrument sampled the water
              |  source_lag   (upstream + fetch/parse: not ours to fix)
        ingested_at       row landed in our SQLite
              |  publish_lag  (waiting for the next export run: ours)
        published_at      value appeared in site/data/*.json

    Splitting them is the whole point. A buoy that reads 40 minutes stale on
    the page could be a slow EC feed (source_lag) or an export that has not
    run (publish_lag), and the fix is completely different in each case.
    `total_lag_seconds` is what a visitor experiences; the two components say
    who owns it.

ONE ROW PER OBSERVATION, NOT PER CHECK
    A heartbeat table (every station, every export run) would grow with the
    export cadence and mostly record that nothing changed. Instead each
    observation gets one row, and `last_seen_at` is bumped on every export
    where it is still the newest thing we hold. That keeps stalls visible —
    a station stuck for six hours shows one row with `stale_seconds` climbing
    — without a row per five-minute tick.

    A row whose `published_at` is NULL while newer rows are published is data
    we collected and never displayed: the observation arrived and was
    superseded before any export ran.

BASELINES ARE PER STATION
    AMQP-pushed EC feeds and cron-polled sources have different source_lag
    floors, so a global "late" threshold is noise. Compare a station against
    its own history. The delivery mechanism is deliberately NOT a column
    here — it is a property of the station, and `config/stations.json` plus
    `docs/DATA_FEEDS.md` are the source of truth for it.

CAVEAT: `recorded_at` IS NOT ALWAYS FIRST-INGEST TIME
    Ingest time comes from the observation table's `recorded_at`, which is a
    column default — so it is only "when this row first arrived" if the
    writer never *replaces* the row. Safe patterns, both in use here:

      - `INSERT OR IGNORE` (EC/NOAA buoys, DFO tides): first write wins, so
        the stamp survives. Correct where upstream never revises a reading.
      - `INSERT ... ON CONFLICT DO UPDATE`, or check-then-UPDATE (Surrey
        tides and waves): the value is updatable but `recorded_at` is left
        alone. Needed because Surrey publishes the newest reading
        provisionally and corrects it within the hour.

    The pattern to avoid is `INSERT OR REPLACE`: REPLACE deletes and
    re-inserts, so every row in a re-fetched window gets a fresh
    `recorded_at` on every fetch. Surrey tides did this until 2026-08-17,
    which made a 24-hour window re-fetched every 20 minutes read as ~22
    hours of fake lag, and left no recoverable history to backfill from.

    This table captures ingest time at *first sighting* and never revises
    it, so a long export outage inflates `source_lag` on the rows written
    when it comes back. Do not "fix" that by widening the export interval.

THIS IS TELEMETRY, NOT THE PRODUCT
    Every entry point swallows its own errors. A lag-table problem must never
    fail an export that is otherwise ready to serve the site.
"""

import sqlite3
import time

from lib.config import REPORTING_LAG_DATABASE, REPORTING_LAG_RETENTION_DAYS
from lib.logging_config import setup_logging

logger = setup_logging("reporting_lag")

# All timestamps here are Unix epoch seconds (the project convention), so the
# generated columns are plain subtraction. Observation tables store
# `recorded_at` as a UTC text stamp instead, hence the strftime() on the way in.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reporting_lag (
    -- Which pipeline the observation came from: 'buoy', 'wind',
    -- 'lightstation', 'tide', 'weather'. Part of the key because station
    -- ids are only unique within a source.
    source TEXT NOT NULL,
    station_id TEXT NOT NULL,

    -- The three clocks. observation_time and ingested_at are known at write
    -- time; published_at is filled in by the first export that carries this
    -- observation, and stays NULL if none ever did.
    observation_time INTEGER NOT NULL,
    ingested_at INTEGER NOT NULL,
    published_at INTEGER,

    -- Most recent export run where this was still the freshest observation
    -- we held for the station. Set equal to published_at on first publish,
    -- then bumped; the gap between them is how long the station went
    -- without new data.
    last_seen_at INTEGER,

    -- Instrument -> our database. Upstream latency plus our fetch/parse.
    source_lag_seconds INTEGER
        GENERATED ALWAYS AS (ingested_at - observation_time) VIRTUAL,
    -- Our database -> the website. Export cadence, and export failures.
    publish_lag_seconds INTEGER
        GENERATED ALWAYS AS (published_at - ingested_at) VIRTUAL,
    -- What a visitor experiences: how old the number was when first shown.
    total_lag_seconds INTEGER
        GENERATED ALWAYS AS (published_at - observation_time) VIRTUAL,
    -- Peak staleness: how old this observation got before it was replaced.
    -- The metric the 2-hour freshness window only answers yes/no about.
    stale_seconds INTEGER
        GENERATED ALWAYS AS (last_seen_at - observation_time) VIRTUAL,

    -- 1 on the first row ever recorded for a station. Its published_at is
    -- when tracking began, not when the value first reached the site, so its
    -- publish_lag is overstated by up to one export cycle. Exclude these
    -- from publish/total lag statistics: `WHERE seeded = 0`. source_lag is
    -- unaffected and valid on seeded rows.
    seeded INTEGER NOT NULL DEFAULT 0,

    created_at INTEGER DEFAULT (strftime('%s', 'now')),

    PRIMARY KEY (source, station_id, observation_time)
);
"""

CREATE_INDEXES_SQL = [
    # Retention pruning and cross-station windows ("all sources, last 7
    # days"). Per-station queries are served by the primary key prefix.
    ("CREATE INDEX IF NOT EXISTS idx_reporting_lag_obs_time ON reporting_lag(observation_time);"),
    # "What is still unpublished?" — partial index so it stays tiny; in
    # steady state only the newest observation per station qualifies.
    (
        "CREATE INDEX IF NOT EXISTS idx_reporting_lag_unpublished "
        "ON reporting_lag(source, station_id, observation_time) "
        "WHERE published_at IS NULL;"
    ),
]

# First publish wins: published_at is the moment a value first reached the
# site, so a later export re-publishing the same (still newest) observation
# must only push last_seen_at forward. The CASE guards the stall path, where
# the incoming row carries no publish stamp at all.
UPSERT_SQL = """
INSERT INTO reporting_lag
    (source, station_id, observation_time, ingested_at, published_at, last_seen_at, seeded)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source, station_id, observation_time) DO UPDATE SET
    published_at = COALESCE(reporting_lag.published_at, excluded.published_at),
    last_seen_at = CASE
        WHEN excluded.last_seen_at IS NULL THEN reporting_lag.last_seen_at
        ELSE MAX(COALESCE(reporting_lag.last_seen_at, 0), excluded.last_seen_at)
    END
"""


# The stall path: same observation still on the page. Only the clocks that
# describe "still the newest thing we hold" move. published_at is left alone
# (first publish wins), and it is set here only if an earlier export somehow
# recorded the observation without publishing it.
BUMP_SQL = """
UPDATE reporting_lag
   SET last_seen_at = MAX(COALESCE(last_seen_at, 0), ?),
       published_at = COALESCE(published_at, ?)
 WHERE source = ? AND station_id = ? AND observation_time = ?
"""


def ensure_schema(conn):
    """Create the table and indexes if absent. Safe to call on every write."""
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    for stmt in CREATE_INDEXES_SQL:
        cur.execute(stmt)
    conn.commit()


def _watermarks(conn, source):
    """Newest observation_time already recorded, per station."""
    rows = conn.execute(
        "SELECT station_id, MAX(observation_time) FROM reporting_lag WHERE source = ? GROUP BY station_id",
        (source,),
    )
    return dict(rows)


def _purge(conn):
    cutoff = int(time.time()) - REPORTING_LAG_RETENTION_DAYS * 86400
    deleted = conn.execute("DELETE FROM reporting_lag WHERE observation_time < ?", (cutoff,)).rowcount
    if deleted:
        logger.info(f"Purged {deleted} lag rows older than {REPORTING_LAG_RETENTION_DAYS} days")


def record_publication(source, source_conn, table, station_column, published, published_at=None):
    """
    Record the lag of everything published by an export that just ran.

    `published` maps station_id -> observation_time of the observation the
    export actually put on the page. Observations that arrived since the last
    call but were superseded before this export get rows too, with a NULL
    published_at — that gap is the "collected but never displayed" signal.

    `source_conn` is the caller's already-open connection to the observation
    database; it is only read from. Rows without a `recorded_at` stamp are
    skipped rather than guessed at: without an ingest time there is no way to
    split the two components, and a fabricated one would read as zero
    source_lag.

    Never raises: telemetry must not take down an export.
    """
    if not published:
        return

    now = int(published_at or time.time())

    try:
        with sqlite3.connect(REPORTING_LAG_DATABASE, timeout=5) as lag_conn:
            lag_conn.execute("PRAGMA journal_mode=WAL;")
            ensure_schema(lag_conn)
            marks = _watermarks(lag_conn, source)

            rows = []
            bumps = []
            for station_id, latest_time in published.items():
                # First sighting of a station: seed with just the observation
                # being published. Older history has no publish record to
                # recover, so backfilling it here would only add NULL rows.
                first_sighting = station_id not in marks
                since = marks.get(station_id, latest_time - 1)
                if since >= latest_time:
                    # Nothing new — the station is stalled on an observation
                    # we already recorded. Bump last_seen_at so the stall
                    # shows up as growing stale_seconds on that one row. An
                    # UPDATE, not an upsert: if the row is somehow missing we
                    # have no real ingest time, and inventing one would post a
                    # source_lag of "however long the station has been stuck".
                    bumps.append((now, now, source, station_id, latest_time))
                    continue

                for obs_time, ingested_at in source_conn.execute(
                    f"""
                    SELECT observation_time, CAST(strftime('%s', recorded_at) AS INTEGER)
                    FROM {table}
                    WHERE {station_column} = ?
                      AND observation_time > ?
                      AND observation_time <= ?
                      AND recorded_at IS NOT NULL
                    """,
                    (station_id, since, latest_time),
                ):
                    if ingested_at is None:
                        continue
                    is_published = obs_time == latest_time
                    rows.append(
                        (
                            source,
                            station_id,
                            obs_time,
                            ingested_at,
                            now if is_published else None,
                            now if is_published else None,
                            1 if (is_published and first_sighting) else 0,
                        )
                    )

            lag_conn.executemany(UPSERT_SQL, rows)
            lag_conn.executemany(BUMP_SQL, bumps)
            _purge(lag_conn)
            logger.debug(f"Recorded {len(rows)} new and {len(bumps)} stalled {source} lag rows")
    except Exception as e:
        logger.warning(f"Could not record {source} reporting lag: {e}")
