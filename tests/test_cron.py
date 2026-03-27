"""Tests for crontab health — verify all referenced scripts exist on disk."""

import re
import subprocess

import pytest


def get_crontab_lines():
    """Get non-comment, non-empty lines from the live crontab."""
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip("No crontab installed for current user")
    return [
        line
        for line in result.stdout.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def extract_script_paths(cron_lines):
    """Extract absolute file paths to scripts from crontab lines."""
    paths = []
    for line in cron_lines:
        # Match python script paths: /absolute/path/to/script.py
        for match in re.finditer(r"(/\S+\.(?:py|sh))", line):
            paths.append(match.group(1))
    return paths


def extract_interpreter_paths(cron_lines):
    """Extract interpreter/binary paths from crontab lines."""
    paths = []
    for line in cron_lines:
        # Match explicit interpreter paths (python, bash, etc.)
        for match in re.finditer(r"(/\S+/(?:python3?|bash|sh|node))\s", line):
            paths.append(match.group(1))
    return paths


class TestCrontab:
    """Verify the live crontab references valid files."""

    def test_all_scripts_exist(self):
        """Every script referenced in crontab should exist on disk."""
        lines = get_crontab_lines()
        scripts = extract_script_paths(lines)
        assert scripts, "No scripts found in crontab — is it empty?"

        missing = [p for p in scripts if not __import__("pathlib").Path(p).exists()]
        assert missing == [], "Crontab references missing scripts:\n" + "\n".join(
            f"  {p}" for p in missing
        )

    def test_all_interpreters_exist(self):
        """Every interpreter (python3, bash, etc.) in crontab should exist."""
        lines = get_crontab_lines()
        interpreters = extract_interpreter_paths(lines)
        assert interpreters, "No interpreters found in crontab"

        missing = [
            p for p in interpreters if not __import__("pathlib").Path(p).exists()
        ]
        assert missing == [], (
            "Crontab references missing interpreters:\n"
            + "\n".join(f"  {p}" for p in missing)
        )

    def test_log_dirs_exist(self):
        """Every log output directory in crontab should exist."""
        lines = get_crontab_lines()
        log_dirs = set()
        for line in lines:
            for match in re.finditer(r">>\s*(/\S+\.log)", line):
                log_dirs.add(__import__("pathlib").Path(match.group(1)).parent)

        missing = [str(d) for d in log_dirs if not d.exists()]
        assert missing == [], "Crontab references missing log dirs:\n" + "\n".join(
            f"  {d}" for d in missing
        )
