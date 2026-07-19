"""Asset cache-bust versions must match current file contents.

Guards the automated ?v= scheme (scripts/update_asset_versions.py): every
local .js/.css reference in tracked site HTML carries the content hash of the
file it points at, and points at a file that exists. Fails when someone edits
an asset without running the updater (the pre-commit hook runs it for you).
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "update_asset_versions.py"


def test_asset_versions_current():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "Stale or broken asset ?v= references — run "
        f"scripts/update_asset_versions.py\n{result.stdout}{result.stderr}"
    )
