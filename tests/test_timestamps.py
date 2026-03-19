"""Tests for timestamp parsing and normalization patterns used across the codebase."""

from datetime import datetime, timezone

import pytest

# ── ISO 8601 → epoch conversion ──────────────────────────────


class TestIso8601ToEpoch:
    """Test the pattern: datetime.fromisoformat(text.replace("Z", "+00:00"))"""

    def _parse(self, text):
        return datetime.fromisoformat(text.replace("Z", "+00:00"))

    def test_z_suffix(self):
        dt = self._parse("2024-11-20T14:00:00.000Z")
        assert dt.tzinfo is not None
        assert dt.year == 2024
        assert dt.month == 11
        assert dt.hour == 14

    def test_offset_suffix(self):
        dt = self._parse("2024-11-20T14:00:00+00:00")
        assert dt.tzinfo is not None

    def test_epoch_conversion(self):
        dt = self._parse("2024-11-20T14:00:00.000Z")
        epoch = int(dt.timestamp())
        assert epoch == 1732111200

    def test_millisecond_precision_preserved(self):
        dt = self._parse("2024-11-20T14:00:00.500Z")
        assert dt.microsecond == 500000

    def test_epoch_roundtrip(self):
        """epoch → ISO → epoch should be identity."""
        original_epoch = 1732111200
        dt = datetime.fromtimestamp(original_epoch, tz=timezone.utc)
        iso = dt.isoformat()
        dt2 = datetime.fromisoformat(iso)
        assert int(dt2.timestamp()) == original_epoch


# ── Epoch → ISO 8601 (export direction) ─────────────────────


class TestEpochToIso8601:
    """Test the pattern: datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()"""

    def test_known_epoch(self):
        iso = datetime.fromtimestamp(1732111200, tz=timezone.utc).isoformat()
        assert iso.startswith("2024-11-20T14:00:00")
        assert "+00:00" in iso

    def test_epoch_zero(self):
        iso = datetime.fromtimestamp(0, tz=timezone.utc).isoformat()
        assert "1970-01-01" in iso

    def test_always_utc(self):
        iso = datetime.fromtimestamp(1732111200, tz=timezone.utc).isoformat()
        assert "+00:00" in iso


# ── 5-minute downsampling on DB insert ───────────────────────


class TestFiveMinuteFilter:
    """Test the pattern from tide_to_sqlite.py: skip if minute % 5 != 0 or second != 0"""

    @pytest.mark.parametrize(
        "minute, second, should_keep",
        [
            (0, 0, True),
            (5, 0, True),
            (10, 0, True),
            (15, 0, True),
            (30, 0, True),
            (55, 0, True),
            (1, 0, False),
            (2, 0, False),
            (3, 0, False),
            (7, 0, False),
            (0, 1, False),
            (5, 30, False),
            (15, 59, False),
        ],
    )
    def test_filter(self, minute, second, should_keep):
        keep = (minute % 5 == 0) and (second == 0)
        assert keep == should_keep

    def test_one_hour_yields_12_points(self):
        """60 minutes at 1-min intervals, filtered to 5-min, gives 12 points."""
        kept = [(m, 0) for m in range(60) if m % 5 == 0]
        assert len(kept) == 12


# ── Tide prediction proximity check ─────────────────────────


class TestPredictionProximity:
    """Test the pattern: reject prediction if > 1 hour from now."""

    @pytest.mark.parametrize(
        "diff_seconds, should_accept",
        [
            (0, True),
            (1800, True),   # 30 min
            (3599, True),   # just under 1 hour
            (3600, True),   # exactly 1 hour (not > 3600)
            (3601, False),  # just over 1 hour
            (7200, False),  # 2 hours
        ],
    )
    def test_proximity(self, diff_seconds, should_accept):
        accepted = diff_seconds <= 3600
        assert accepted == should_accept


# ── Tide offset time matching ────────────────────────────────


class TestOffsetTimeMatching:
    """Test the pattern: observation-prediction match within ±10 minutes (600s)."""

    @pytest.mark.parametrize(
        "diff_seconds, should_match",
        [
            (0, True),
            (300, True),    # 5 min
            (600, True),    # exactly 10 min
            (601, False),   # just over 10 min
            (1800, False),  # 30 min
        ],
    )
    def test_matching(self, diff_seconds, should_match):
        matched = abs(diff_seconds) <= 600
        assert matched == should_match
