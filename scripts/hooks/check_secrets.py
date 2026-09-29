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

3. **Private details.** Not every sensitive thing is a credential: hostnames,
   private addresses and the names of other services on the host tell an
   attacker which doors exist. Generic private-address patterns live here;
   the specific terms live in `config/publish_denylist.txt` (gitignored —
   a tracked list would publish exactly what it protects).

There are two public surfaces, and git is only one of them. `site/` is served
by Caddy at halibutbank.ca, and `site/data/` is *gitignored* — so the staged
and tracked scans never look at it, while every file in it is fetchable by
name. `--served` covers that blind spot. And a commit publishes more than its
files: `--range` also reads the messages, author lines and every intermediate
diff of the commits about to be pushed, which no file scan sees.

Usage:
    check_secrets.py              # scan staged content (pre-commit)
    check_secrets.py --all        # scan every tracked file (audit)
    check_secrets.py --served     # scan everything Caddy serves from site/
    check_secrets.py --range A..B # scan the commits in A..B: messages,
                                  # authors, and every line they add
    check_secrets.py --message F  # scan a commit message file (commit-msg)
    check_secrets.py FILE [FILE…] # scan specific files

    --strict  fail if config/.env or the denylist is missing, instead of
              silently scanning with less (the deploy gate uses this)

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

# Terms that are not credentials but must not be published. Gitignored.
DENYLIST_FILE = "config/publish_denylist.txt"

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

# Private network details. Specific names go in DENYLIST_FILE; these catch the
# generic shapes (a LAN address, a `.local` hostname) before anyone thinks to
# list them. A hostname followed by `.ext` is a filename (`settings.local.json`).
PRIVATE_PATTERNS = (
    (
        "private IPv4 address",
        re.compile(
            r"(?<![\d.])(?:10\.\d{1,3}|192\.168|172\.(?:1[6-9]|2\d|3[01])"
            r"|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7]))\.\d{1,3}\.\d{1,3}(?![\d.])"
        ),
    ),
    (
        "private hostname",
        re.compile(r"(?<![\w.$])[A-Za-z][\w-]{2,}\.(?:local|lan|home\.arpa|internal)\b(?![\w(]|\.\w)"),
    ),
)

# Files that legitimately describe credential *shapes* rather than hold them.
SKIP_PATHS = {
    "scripts/hooks/check_secrets.py",
    "tests/test_secrets.py",
    "config/webcams.example.json",
}

# Third-party minified bundles: dense enough to match the generic patterns by
# accident (`i.local`), and never where our own details end up.
SKIP_PREFIXES = ("site/assets/vendor/",)

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


def denylist_patterns() -> list[tuple[str, re.Pattern]]:
    """Terms from DENYLIST_FILE, matched case-insensitively.

    One term per line; `#` starts a comment; a line starting `re:` is a
    regular expression. Findings name the term by its line in the file, so
    a log or a pasted error never reprints the term it protects.
    """
    path = REPO_ROOT / DENYLIST_FILE
    if not path.is_file():
        return []
    patterns = []
    for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        term = line.strip()
        if not term or term.startswith("#"):
            continue
        regex = term[3:].strip() if term.startswith("re:") else re.escape(term)
        label = f"denylisted term (line {number} of {DENYLIST_FILE})"
        patterns.append((label, re.compile(regex, re.IGNORECASE)))
    return patterns


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
    # os.walk, not rglob: in the dev worktree `site/data` is a symlink, and
    # rglob (before 3.13) does not descend into symlinked directories.
    found = []
    for dirpath, _dirs, files in os.walk(root, followlinks=True):
        for name in files:
            found.append(str((Path(dirpath) / name).relative_to(REPO_ROOT)))
    return sorted(found)


def tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, cwd=REPO_ROOT, check=True
    )
    return [f for f in out.stdout.splitlines() if f]


def line_of(text: str, match: re.Match) -> int:
    return text.count("\n", 0, match.start()) + 1


def scan_text(
    path: str,
    text: str,
    secrets: set[str],
    private: list[tuple[str, re.Pattern]] | None = None,
) -> list[str]:
    """Findings in `text`. `private` adds denylist and private-address checks."""
    findings = []
    for value in secrets:
        if value in text:
            findings.append(f"{path}: contains a value from your .env — {mask(value)}")
    for label, pattern in SHAPE_PATTERNS:
        for match in pattern.finditer(text):
            hit = match.group(match.lastindex or 0)
            if PLACEHOLDER_RE.match(hit):
                continue
            findings.append(f"{path}:{line_of(text, match)}: looks like a {label} — {mask(hit)}")
    for label, pattern in private or ():
        for match in pattern.finditer(text):
            # A denylist label already says which term; don't reprint it.
            shown = "" if label.startswith("denylisted") else f" — {mask(match.group(0))}"
            findings.append(f"{path}:{line_of(text, match)}: {label}{shown}")
    return findings


def commit_range_texts(rev_range: str) -> list[tuple[str, str]]:
    """(label, text) pairs for everything the commits in `rev_range` publish.

    The message and author/committer lines of each commit, plus the lines
    each commit *adds* — per commit, not the net diff, because a value added
    in one commit and removed in the next is still in the pushed history.
    """
    revs = subprocess.run(
        ["git", "rev-list", "--reverse", rev_range],
        capture_output=True, text=True, cwd=REPO_ROOT, check=True,
    ).stdout.split()
    texts: list[tuple[str, str]] = []
    for rev in revs:
        short = rev[:10]
        meta = subprocess.run(
            ["git", "show", "-s", "--format=%an <%ae>%n%cn <%ce>%n%B", rev],
            capture_output=True, text=True, cwd=REPO_ROOT, check=True,
        ).stdout
        texts.append((f"commit {short} message/author", meta))
        patch = subprocess.run(
            ["git", "show", "--format=", "--unified=0", "--no-color", "--no-ext-diff", rev],
            capture_output=True, cwd=REPO_ROOT, check=True,
        ).stdout.decode("utf-8", errors="replace")
        added: dict[str, list[str]] = {}
        current = None
        for line in patch.splitlines():
            if line.startswith("+++ "):
                current = line[6:] if line.startswith("+++ b/") else None
            elif current and line.startswith("+") and not should_skip(current):
                added.setdefault(current, []).append(line[1:])
        for path, lines in added.items():
            texts.append((f"commit {short} {path}", "\n".join(lines)))
    return texts


def should_skip(path: str) -> bool:
    return path in SKIP_PATHS or path.startswith(SKIP_PREFIXES) or path.endswith(SKIP_SUFFIXES)


def option_value(argv: list[str], flag: str) -> str | None:
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
        raise SystemExit(f"{flag} needs a value")
    return None


def read_worktree(path: str) -> str | None:
    full = REPO_ROOT / path
    return full.read_text(errors="replace") if full.is_file() else None


def main(argv: list[str]) -> int:
    if os.environ.get("ALLOW_SECRETS"):
        print("⚠️  secret scan skipped (ALLOW_SECRETS set)")
        return 0

    secrets = known_secret_values()
    denylist = denylist_patterns()
    private = [*PRIVATE_PATTERNS, *denylist]

    if "--strict" in argv:
        missing = [
            name
            for name, ok in (("config/.env", bool(secrets)), (DENYLIST_FILE, bool(denylist)))
            if not ok
        ]
        if missing:
            print(f"🔐 --strict: nothing loaded from {', '.join(missing)}; refusing to scan blind.")
            return 1

    texts: list[tuple[str, str]] = []
    if rev_range := option_value(argv, "--range"):
        texts = commit_range_texts(rev_range)
    elif message_file := option_value(argv, "--message"):
        # Drop git's own `#` comment lines (the commit template), keep the rest.
        lines = Path(message_file).read_text(errors="replace").splitlines()
        texts = [("commit message", "\n".join(ln for ln in lines if not ln.startswith("#")))]
    else:
        if "--all" in argv:
            paths, read = tracked_files(), read_worktree
        elif "--served" in argv:
            paths, read = served_files(), read_worktree
        elif explicit := [a for a in argv if not a.startswith("-")]:
            paths = explicit
            read = lambda p: (  # noqa: E731
                Path(p).read_text(errors="replace") if Path(p).is_file() else None
            )
        else:
            paths, read = staged_files(), staged_content
        for path in paths:
            if should_skip(path):
                continue
            try:
                text = read(path)
            except (OSError, UnicodeDecodeError):
                continue
            if text:
                texts.append((path, text))

    findings: list[str] = []
    for label, text in texts:
        findings.extend(scan_text(label, text, secrets, private))

    if findings:
        print("\n🔐 Publication scan FAILED — this would make it public:\n")
        for f in findings:
            print(f"   {f}")
        print(
            "\nKeep credentials in config/.env (gitignored) and read them via\n"
            "lib/env.py; keep host and network details out of tracked files and\n"
            "commit messages. If this is genuinely a false positive, add the path\n"
            "to SKIP_PATHS in scripts/hooks/check_secrets.py, or commit once with\n"
            "ALLOW_SECRETS=1.\n"
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
