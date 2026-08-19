"""Which storm surge run gets archived for verification.

One line of logic, and it silently ate three days of the verification archive
(2026-08-17 to 2026-08-19) before anyone noticed — so it gets its own test.

The old check was `datetime.now(timezone.utc).hour == 13`: the 13:31 cron job
is indeed the one that fetches the 00Z run, but the fetch takes ~32 minutes,
and once it grew past the top of the hour the job started finishing at 14:03.
The branch stopped matching, "skipping database storage" is a normal-looking
log line, and the job still exited 0. Nothing downstream noticed until the
verification chart ran out of runs.

The fix reads the run time off the data instead of the clock. These tests pin
that: the decision must depend only on the model run, never on when the job
happens to finish.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.fetch import fetch_storm_surge as ss

UTC = timezone.utc


def run_at(hour, day=19):
    """The model's first valid time — which for GDSPS is the run instant."""
    return datetime(2026, 8, day, hour, 0, tzinfo=UTC)


class TestShouldArchiveRun:
    def test_the_00z_run_is_archived(self):
        assert ss.should_archive_run(run_at(0)) is True

    def test_the_12z_run_is_not(self):
        """GDSPS runs twice daily; only one can be stored per date key."""
        assert ss.should_archive_run(run_at(12)) is False

    @pytest.mark.parametrize("hour", [1, 6, 11, 13, 18, 23])
    def test_no_other_hour_is_archived(self, hour):
        assert ss.should_archive_run(run_at(hour)) is False

    def test_the_decision_ignores_how_long_the_fetch_took(self):
        """The regression, stated directly: same run, any finishing time."""
        run = run_at(0)
        assert ss.should_archive_run(run) is True
        # A 32-minute fetch, a 3-hour fetch, a fetch that straddles midnight —
        # none of it may change the answer, because none of it is consulted.
        assert ss.should_archive_run(run) is True

    def test_only_one_run_per_day_qualifies(self):
        """`forecast_archive` keys a run by date alone, so two archived runs on
        one date would collide on the primary key and overwrite each other."""
        day = [run_at(hour) for hour in range(24)]
        assert sum(ss.should_archive_run(step) for step in day) == 1

    def test_consecutive_days_each_archive_once(self):
        runs = [run_at(hour, day) for day in (17, 18, 19) for hour in (0, 12)]
        assert sum(ss.should_archive_run(step) for step in runs) == 3


class TestArchivedRunAlignsWithTheStoredKey:
    def test_the_archived_hour_is_midnight(self):
        """The verification export reads lead time as
        `valid_time - forecast_run_time`, where forecast_run_time is stored as
        a bare date. That arithmetic is only a true lead time because the
        archived run is 00Z — midnight *is* the run instant. Archiving 12Z
        instead would silently put every published lead 12 hours out.
        """
        assert ss.ARCHIVED_RUN_HOUR == 0

    def test_stored_date_equals_the_run_instant(self):
        run = run_at(0)
        stored = run.strftime("%Y-%m-%d")
        assert datetime.strptime(stored, "%Y-%m-%d").replace(tzinfo=UTC) == run

    def test_a_56_hour_lead_reads_back_as_56_hours(self):
        """The window the export actually queries (56-79 h), end to end."""
        run = run_at(0)
        valid = run + timedelta(hours=56)
        stored = datetime.strptime(run.strftime("%Y-%m-%d"), "%Y-%m-%d").replace(tzinfo=UTC)
        assert (valid - stored).total_seconds() / 3600 == 56


class TestNonStationFilesAreSkipped:
    def test_verification_json_is_not_mistaken_for_a_station(self):
        """`create_combined_forecast` globs the output directory, so any
        non-station JSON written there must be listed or it is merged in as a
        phantom station. Renaming hindcast.json → verification.json had to
        touch that set; this is the guard for the next rename."""
        source = Path(ss.__file__).read_text()
        start = source.index("skip_files = {")
        skip = source[start : source.index("}", start)]
        assert "verification.json" in skip
        assert "combined_forecast.json" in skip
        assert "observed_surge.json" in skip

    def test_the_exporters_output_name_matches_what_is_skipped(self):
        """The two live in different files; drift between them is the bug."""
        from scripts.export import export_storm_surge_verification as ex

        source = Path(ss.__file__).read_text()
        start = source.index("skip_files = {")
        skip = source[start : source.index("}", start)]
        assert ex.OUTPUT_PATH.name in skip
