"""pytz must not come back: it bundles its own tz data, which predates BC's permanent UTC-7.

pytz 2025.2 put Vancouver back on PST after 2026-11-01, so every webcam stamp,
sunlight "today" and storm-surge day window read an hour early (aa8e408).
zoneinfo reads the system's tz data instead (lib/pacific_time.py).
"""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IMPORT = re.compile(r"^\s*(import\s+pytz\b|from\s+pytz\b)", re.MULTILINE)


def test_no_tracked_python_imports_pytz():
    files = subprocess.run(
        ["git", "ls-files", "*.py"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    offenders = [f for f in files if IMPORT.search((ROOT / f).read_text(encoding="utf-8", errors="replace"))]
    assert offenders == [], f"pytz imported in {offenders}; use lib.pacific_time (zoneinfo)"


def test_pytz_is_not_a_declared_dependency():
    for name in ("pyproject.toml", "requirements-lock.txt"):
        text = (ROOT / name).read_text().lower()
        assert not re.search(r'^\s*"?pytz\b', text, re.MULTILINE), f"pytz listed in {name}"
