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


class TestPrivateDetails:
    """Not credentials, but still not for publication (docs/SECRETS.md)."""

    @pytest.fixture
    def private(self):
        from check_secrets import PRIVATE_PATTERNS

        return list(PRIVATE_PATTERNS)

    @pytest.mark.parametrize(
        "text",
        [
            "ssh to 192.168.1.20 first",
            "the NAS at 10.0.0.5",
            "docker bridge 172.17.0.1",
            "tailnet peer 100.101.102.103",
            "the git remote is on backup-box.local",
            "see nas.lan:3000",
        ],
    )
    def test_catches(self, text, private):
        assert scan_text("f.md", text, set(), private), f"missed: {text}"

    @pytest.mark.parametrize(
        "text",
        [
            "curl http://127.0.0.1:4173/",
            "version 10.2.3",  # three parts, not an address
            "lon -123.1210.5",  # digits run on: not a standalone address
            "ECharts 5.10.0.1 build",
            "copy .claude/settings.local.json",  # a filename, not a host
            "i.local=1",  # minified JS property access
            "a doc at docs.local_notes",
        ],
    )
    def test_allows(self, text, private):
        assert not scan_text("f.md", text, set(), private), f"false positive: {text}"

    def test_denylist_terms_match_case_insensitively(self, tmp_path, monkeypatch):
        import check_secrets

        (tmp_path / "config").mkdir()
        (tmp_path / check_secrets.DENYLIST_FILE).write_text(
            "# comment\n\nSecretHost\nre:game-?server\n"
        )
        monkeypatch.setattr(check_secrets, "REPO_ROOT", tmp_path)
        patterns = check_secrets.denylist_patterns()
        assert len(patterns) == 2
        findings = scan_text("f.md", "ssh secrethost; the GameServer", set(), patterns)
        assert len(findings) == 2
        # The finding points at the term's line; it never reprints the term.
        assert all("secrethost" not in f.lower() for f in findings)
        assert "line 3" in findings[0]

    def test_denylist_is_not_tracked(self):
        import check_secrets

        tracked = subprocess.run(
            ["git", "ls-files", check_secrets.DENYLIST_FILE],
            capture_output=True, text=True, cwd=REPO_ROOT, check=True,
        ).stdout
        assert not tracked, "the denylist is tracked: it would publish what it protects"


class TestCommitRange:
    """--range reads what file scans never see: messages and history."""

    def git(self, repo, *args):
        return subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        ).stdout.strip()

    def test_reads_messages_and_intermediate_diffs(self, tmp_path, monkeypatch):
        import check_secrets

        repo = tmp_path
        self.git(repo, "init", "-q")
        self.git(repo, "config", "user.email", "t@example.com")
        self.git(repo, "config", "user.name", "t")
        (repo / "a.txt").write_text("hello\n")
        self.git(repo, "add", "a.txt")
        self.git(repo, "commit", "-qm", "base")
        base = self.git(repo, "rev-parse", "HEAD")
        # Added then removed: still in the pushed history.
        (repo / "a.txt").write_text("hello\nhost 192.168.4.4\n")
        self.git(repo, "commit", "-qam", "add")
        (repo / "a.txt").write_text("hello\n")
        self.git(repo, "commit", "-qam", "moved it to nas.lan")

        monkeypatch.setattr(check_secrets, "REPO_ROOT", repo)
        texts = check_secrets.commit_range_texts(f"{base}..HEAD")
        findings = [
            f
            for label, text in texts
            for f in scan_text(label, text, set(), list(check_secrets.PRIVATE_PATTERNS))
        ]
        assert any("a.txt" in f and "IPv4" in f for f in findings), findings
        assert any("message" in f and "hostname" in f for f in findings), findings
