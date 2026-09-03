"""The lightstation timeseries window is stated in two places; keep them equal.

`HOURS_BACK` in the export decides how much data `site/data/` actually holds.
`WINDOW_HOURS` in `site/assets/js/lightstation-charts.js` decides what the page
tells the reader it is showing ("72-Hour Reports", "No reports in the past 72
hours"). Nothing at runtime connects them — the JSON is a bare
station-keyed object and adding a metadata key to it would change a live
public API response shape — so the link is asserted here instead.
"""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPORT_SCRIPT = PROJECT_ROOT / "scripts" / "export" / "export_lightstation_timeseries.py"
CHARTS_JS = PROJECT_ROOT / "site" / "assets" / "js" / "lightstation-charts.js"


def _single_int(path: Path, pattern: str) -> int:
    matches = re.findall(pattern, path.read_text(), re.MULTILINE)
    assert len(matches) == 1, f"expected exactly one {pattern!r} in {path.name}, found {matches}"
    return int(matches[0])


def test_export_and_frontend_windows_agree():
    hours_back = _single_int(EXPORT_SCRIPT, r"^HOURS_BACK = (\d+)$")
    window_hours = _single_int(CHARTS_JS, r"^const WINDOW_HOURS = (\d+);$")
    assert hours_back == window_hours, (
        f"export writes a {hours_back}h window but the page says {window_hours}h; "
        "update WINDOW_HOURS in site/assets/js/lightstation-charts.js"
    )


def test_window_covers_the_slowest_reporting_station():
    """Chrome Island and Entrance Island run 33h at the 90th percentile.

    A window at or below that holds zero points for them most of the time
    while they are reporting perfectly normally, which is the bug this
    threshold exists to prevent. 48h is the bare minimum; the export ships 72h
    for margin.
    """
    hours_back = _single_int(EXPORT_SCRIPT, r"^HOURS_BACK = (\d+)$")
    assert hours_back >= 48, (
        f"{hours_back}h is below the 33h p90 gap of the slowest lightstations "
        "(Chrome Island, Entrance Island) plus any useful margin"
    )
