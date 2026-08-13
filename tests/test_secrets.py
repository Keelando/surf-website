"""
Guard against credentials reaching this public repo.

The Windy API key leaked once, inside a generated whole-repo digest that
inlined `config/.env`. The pre-commit hook is the primary defence; these
tests keep the scanner honest and assert the tracked tree stays clean, so a
commit made with `--no-verify` (or a hook that was never installed) still
gets caught by `pytest`.
"""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCANNER = REPO_ROOT / "scripts" / "hooks" / "check_secrets.py"

sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

from check_secrets import scan_text  # noqa: E402


class TestTrackedTree:
    def test_no_secrets_in_tracked_files(self):
        """Every tracked file is free of credentials."""
        result = subprocess.run(
            [sys.executable, str(SCANNER), "--all"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        assert result.returncode == 0, f"Secret scan found issues:\n{result.stdout}"

    def test_env_file_is_not_tracked(self):
        """config/.env must never be committed."""
        tracked = subprocess.run(
            ["git", "ls-files"], capture_output=True, text=True, cwd=REPO_ROOT, check=True
        ).stdout.split()
        offenders = [f for f in tracked if Path(f).name == ".env" or f.endswith("/.env")]
        assert not offenders, f"Credential files are tracked: {offenders}"

    def test_crontab_assigns_no_credentials(self):
        """
        config/crontab.txt is tracked and public. Credentials used to live
        there; lib/env.py + config/.env replaced that.
        """
        text = (REPO_ROOT / "config" / "crontab.txt").read_text()
        findings = scan_text("config/crontab.txt", text, set())
        assert not findings, f"Credentials in the tracked crontab: {findings}"


class TestScanner:
    """The scanner has to catch the shapes that actually leaked."""

    @pytest.mark.parametrize(
        "text,why",
        [
            (
                "WINDY_API_KEY=eyJhbGciOiJIUzI1NiJ9.eyJjaSI6MTEyNzkyMDd9.vs_q3qKnJe8cbOseKJ2h",
                "JWT — the exact shape of the key that leaked",
            ),
            ("SURREY_API_PASSWORD=hunter2xyz", "credential env var with a literal"),
            ('  api_key = "aB3xY9kLmN2pQ7rS4tU8vW1z"', "generic assignment"),
            ("AKIAIOSFODNN7EXAMPLE", "AWS access key id"),
            ("-----BEGIN PRIVATE KEY-----", "private key block"),
        ],
    )
    def test_catches(self, text, why):
        assert scan_text("f.txt", text, set()), f"missed: {why}"

    @pytest.mark.parametrize(
        "text,why",
        [
            ('WINDY_API_KEY = os.environ.get("WINDY_API_KEY")', "reads from env"),
            ('PASSWORD = require_env("SURREY_API_PASSWORD")', "reads via lib.env"),
            ("SURREY_API_PASSWORD=<password>", "documented placeholder"),
            ("SURREY_API_PASSWORD=your_password_here", "documented placeholder"),
            ("# WINDY_API_KEY loaded from config/.env", "a comment"),
            ("stationID=WZO", "short non-credential assignment"),
        ],
    )
    def test_allows(self, text, why):
        assert not scan_text("f.txt", text, set()), f"false positive: {why}"

    def test_matches_known_env_values(self):
        """A value taken from .env is caught even in an unrecognised shape."""
        secret = "s0me-Very-Secret-Value"
        assert scan_text("notes.md", f"pasted this: {secret}", {secret})
        assert not scan_text("notes.md", "nothing to see", {secret})
