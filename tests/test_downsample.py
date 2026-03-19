"""Tests for downsample and freshness logic from export scripts."""

# Import the downsample function directly from the export script
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.export.export_tide_json import downsample_to_15min

# ── downsample_to_15min ──────────────────────────────────────


def _make_row(year, month, day, hour, minute, second=0, value=1.5):
    """Helper: create a (timestamp, value) row tuple."""
    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return (int(dt.timestamp()), value)


class TestDownsampleTo15Min:
    def test_keeps_quarter_hour_marks(self):
        rows = [
            _make_row(2026, 3, 19, 10, 0),
            _make_row(2026, 3, 19, 10, 5),
            _make_row(2026, 3, 19, 10, 10),
            _make_row(2026, 3, 19, 10, 15),
            _make_row(2026, 3, 19, 10, 20),
            _make_row(2026, 3, 19, 10, 25),
            _make_row(2026, 3, 19, 10, 30),
            _make_row(2026, 3, 19, 10, 35),
            _make_row(2026, 3, 19, 10, 40),
            _make_row(2026, 3, 19, 10, 45),
            _make_row(2026, 3, 19, 10, 50),
            _make_row(2026, 3, 19, 10, 55),
        ]
        result = downsample_to_15min(rows)
        assert len(result) == 4  # :00, :15, :30, :45

    def test_preserves_values(self):
        rows = [_make_row(2026, 3, 19, 10, 0, value=2.5)]
        result = downsample_to_15min(rows)
        assert len(result) == 1
        assert result[0][1] == 2.5

    def test_rejects_non_zero_seconds(self):
        """Timestamps with non-zero seconds should be excluded even at :00/:15/:30/:45."""
        rows = [
            _make_row(2026, 3, 19, 10, 0, second=30),
            _make_row(2026, 3, 19, 10, 15, second=1),
        ]
        result = downsample_to_15min(rows)
        assert len(result) == 0

    def test_empty_input(self):
        assert downsample_to_15min([]) == []

    def test_full_hour_all_kept(self):
        """All four quarter-hour marks in an hour should be kept."""
        rows = [_make_row(2026, 3, 19, 14, m) for m in (0, 15, 30, 45)]
        result = downsample_to_15min(rows)
        assert len(result) == 4

    def test_reduction_ratio(self):
        """5-min data for 1 hour (12 rows) should reduce to 4 rows."""
        rows = [_make_row(2026, 3, 19, 8, m) for m in range(0, 60, 5)]
        result = downsample_to_15min(rows)
        assert len(result) == 4
        # ~67% reduction
        assert len(result) / len(rows) == pytest.approx(1 / 3, abs=0.01)


# ── Freshness / staleness logic ──────────────────────────────


class TestStalenessCalculation:
    """Test the staleness logic used across export scripts.

    The pattern is: age_minutes = (now_ts - latest_time) / 60
    stale = age_minutes > 180 (3 hours)
    """

    @pytest.mark.parametrize(
        "age_seconds, expected_stale",
        [
            (0, False),          # just now
            (60, False),         # 1 minute
            (3600, False),       # 1 hour
            (7200, False),       # 2 hours
            (10800, False),      # exactly 3 hours (not > 180 min)
            (10801, True),       # 3 hours + 1 second
            (14400, True),       # 4 hours
            (86400, True),       # 24 hours
        ],
    )
    def test_staleness_threshold(self, age_seconds, expected_stale):
        age_minutes = age_seconds / 60
        stale = age_minutes > 180
        assert stale == expected_stale

    def test_age_calculation(self):
        now = 1710849600  # arbitrary epoch
        latest = now - 7200  # 2 hours ago
        age_minutes = (now - latest) / 60
        assert age_minutes == 120.0
        age_hours = age_minutes / 60
        assert age_hours == 2.0
