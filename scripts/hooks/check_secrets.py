#!/usr/bin/env python3
"""
Block secrets from being committed.

Why this exists: the Windy API key reached the public repo inside
`._codebase_digest.txt`, a generated whole-repo dump that inlined
`config/.env`. Nobody typed the key into a tracked file — a tool copied it
there, and the 07:17 auto-backup cron committed and pushed it unattended.
Ignoring that one filename does not close the hole; the next dump tool,
backup or pasted log gets a different name.

Two checks, cheapest and most precise first:

1. **Known values.** Every value in the repo's own `.env` files is treated as
   a secret. If one appears verbatim in staged content, the commit is blocked.
   No guessing, no false positives.
2. **Shapes.** Generic patterns (JWTs, `api_key = <long value>`) catch
   credentials this repo does not hold yet — a new key pasted before it ever
   reaches `.env`.

There are two public surfaces, and git is only one of them. `site/` is served
by Caddy at halibutbank.ca, and `site/data/` is *gitignored* — so the staged
and tracked scans never look at it, while every file in it is fetchable by
name. `--served` covers that blind spot.

Usage:
    check_secrets.py              # scan staged content (pre-commit)
    check_secrets.py --all        # scan every tracked file (audit)
    check_secrets.py --served     # scan everything Caddy serves from site/
    check_secrets.py FILE [FILE…] # scan specific files

Escape hatch for a genuine false positive:
    ALLOW_SECRETS=1 git commit …
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Values shorter than this are too collision-prone to match on.
MIN_SECRET_LEN = 12

# Where this project keeps real credentials. Gitignored, never staged.
ENV_FILES = ("config/.env", ".env")

# Placeholder values that live in example files and docs — not real secrets.
# Docs must be able to show the *shape* of a credential line without tripping
# this scan, so anything angle-bracketed or obviously fake is allowed through.
PLACEHOLDER_RE = re.compile(
    r"^(your[_-]?|xxx+|todo|changeme|placeholder|example|dummy|test|redacted|"
    r"none|null|\.\.\.|<)",
    re.IGNORECASE,
)

SHAPE_PATTERNS = (
    # JWT — three base64url segments. This is exactly what leaked.
    ("JWT", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")),
    # KEY=value / "key": "value" with a long opaque value.
    (
        "credential assignment",
        re.compile(
            r"(?i)\b(?:api[_-]?key|apikey|auth[_-]?token|access[_-]?token|secret[_-]?key"
            r"|client[_-]?secret|password|passwd|credential)\b\s*[:=]\s*['\"]?"
            r"([A-Za-z0-9_\-./+]{16,})"
        ),
    ),
    # A credential-shaped env var assigned a literal at the start of a line —
    # the `SURREY_API_PASSWORD=…` shape that sat in config/crontab.txt. Values
    # can be short, so length is not a usable signal here; instead this only
    # fires when the name itself says "credential" and the value is a literal
    # rather than a lookup (os.environ…, process.env…, $VAR, empty).
    (
        "credential assigned a literal value",
        re.compile(
            r"(?m)^\s*(?:export\s+)?[A-Z0-9_]*"
            r"(?:API_KEY|APIKEY|PASSWORD|PASSWD|SECRET|TOKEN|CREDENTIAL)[A-Z0-9_]*"
            r"\s*=\s*(?!os\.|process\.|require_env|get_env|\$|\"\"|''|#|$)"
            r"['\"]?([^\s#'\"]{3,})"
        ),
    ),
    ("AWS access key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("private key block", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----")),
)

# Files that legitimately describe credential *shapes* rather than hold them.
SKIP_PATHS = {
    "scripts/hooks/check_secrets.py",
    "tests/test_secrets.py",
    "config/webcams.example.json",
}

SKIP_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".zip", ".gz", ".pdf")


def mask(value: str) -> str:
    """Show enough to identify a secret without reprinting it."""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}…{value[-4:]} ({len(value)} chars)"


def known_secret_values() -> set[str]:
    """Every value assigned in the repo's .env files."""
    values: set[str] = set()
    for rel in ENV_FILES:
        path = REPO_ROOT / rel
        if not path.is_file():
            continue
        for line in path.read_text(errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            value = line.split("=", 1)[1].strip().strip("'\"")
            if len(value) >= MIN_SECRET_LEN and not PLACEHOLDER_RE.match(value):
                values.add(value)
    return values


def staged_files() -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=True,
    )
    return [f for f in out.stdout.splitlines() if f]


def staged_content(path: str) -> str | None:
    """Content as it would be committed, not as it sits on disk."""
    out = subprocess.run(
        ["git", "show", f":{path}"], capture_output=True, cwd=REPO_ROOT, check=False
    )
    if out.returncode != 0:
        return None
    try:
        return out.stdout.decode("utf-8")
    except UnicodeDecodeError:
        return None  # binary


def served_files() -> list[str]:
    """Every file Caddy serves from `site/`, whatever git thinks of it.

    Deliberately walks the filesystem rather than asking git: the point is to
    catch what git cannot see. `site/data/` is gitignored but public, so a
    credential written there by an export or a monitor would pass every other
    check in this repo and land straight on halibutbank.ca.
    """
    root = REPO_ROOT / "site"
    if not root.is_dir():
        return []
    return [str(p.relative_to(REPO_ROOT)) for p in sorted(root.rglob("*")) if p.is_file()]


def tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, cwd=REPO_ROOT, check=True
    )
    return [f for f in out.stdout.splitlines() if f]


def scan_text(path: str, text: str, secrets: set[str]) -> list[str]:
    findings = []
    for value in secrets:
        if value in text:
            findings.append(f"{path}: contains a value from your .env — {mask(value)}")
    for label, pattern in SHAPE_PATTERNS:
        for match in pattern.finditer(text):
            hit = match.group(match.lastindex or 0)
            if PLACEHOLDER_RE.match(hit):
                continue
            findings.append(f"{path}: looks like a {label} — {mask(hit)}")
    return findings


def should_skip(path: str) -> bool:
    return path in SKIP_PATHS or path.endswith(SKIP_SUFFIXES)


def main(argv: list[str]) -> int:
    if os.environ.get("ALLOW_SECRETS"):
        print("⚠️  secret scan skipped (ALLOW_SECRETS set)")
        return 0

    secrets = known_secret_values()

    if "--all" in argv:
        paths = tracked_files()
        read = lambda p: (  # noqa: E731
            (REPO_ROOT / p).read_text(errors="replace") if (REPO_ROOT / p).is_file() else None
        )
    elif "--served" in argv:
        paths = served_files()
        read = lambda p: (  # noqa: E731
            (REPO_ROOT / p).read_text(errors="replace") if (REPO_ROOT / p).is_file() else None
        )
    elif explicit := [a for a in argv if not a.startswith("-")]:
        paths = explicit
        read = lambda p: (  # noqa: E731
            Path(p).read_text(errors="replace") if Path(p).is_file() else None
        )
    else:
        paths = staged_files()
        read = staged_content

    findings: list[str] = []
    for path in paths:
        if should_skip(path):
            continue
        try:
            text = read(path)
        except (OSError, UnicodeDecodeError):
            continue
        if text:
            findings.extend(scan_text(path, text, secrets))

    if findings:
        print("\n🔐 Secret scan FAILED — refusing to commit:\n")
        for f in findings:
            print(f"   {f}")
        print(
            "\nKeep credentials in config/.env (gitignored) and read them via\n"
            "os.environ. If this is genuinely a false positive, add the path to\n"
            "SKIP_PATHS in scripts/hooks/check_secrets.py, or commit once with\n"
            "ALLOW_SECRETS=1.\n"
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
