#!/usr/bin/env python3
"""Rewrite ?v= cache-bust params on local <script>/<link> assets to content hashes.

Scans tracked site/*.html files for src/href attributes referencing local
.js/.css files and sets ?v=<first 10 hex of sha1(file contents)>. Idempotent:
unchanged assets keep the same hash, so diffs only appear when an asset's
content actually changed. References to missing asset files are an error.

Usage:
    scripts/update_asset_versions.py            # rewrite in place
    scripts/update_asset_versions.py --check    # exit 1 if anything is stale

Run automatically by the pre-commit hook; manual bumping is obsolete
(see site/docs/CACHE_BUSTING.md).
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = REPO_ROOT / "site"
HASH_LEN = 10

# src/href pointing at a .js/.css file, with or without an existing query.
ASSET_REF = re.compile(
    r'(?P<attr>\b(?:src|href))="(?P<path>[^"?]+\.(?:js|css))(?:\?[^"]*)?"'
)


def tracked_html_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "site/*.html"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [REPO_ROOT / line for line in out.splitlines() if line]


def asset_hash(asset: Path, cache: dict[Path, str]) -> str:
    if asset not in cache:
        cache[asset] = hashlib.sha1(asset.read_bytes()).hexdigest()[:HASH_LEN]
    return cache[asset]


def resolve_asset(ref: str, html_file: Path) -> Path | None:
    """Map an src/href value to a file on disk; None for external URLs."""
    if ref.startswith(("http://", "https://", "//")):
        return None
    if ref.startswith("/"):
        return SITE_DIR / ref.lstrip("/")
    return html_file.parent / ref


def process_file(
    html_file: Path, cache: dict[Path, str], errors: list[str]
) -> tuple[str, str]:
    original = html_file.read_text()

    def replace(m: re.Match) -> str:
        asset = resolve_asset(m.group("path"), html_file)
        if asset is None:
            return m.group(0)
        if not asset.is_file():
            errors.append(
                f"{html_file.relative_to(REPO_ROOT)}: "
                f'{m.group("attr")}="{m.group("path")}" does not exist'
            )
            return m.group(0)
        return f'{m.group("attr")}="{m.group("path")}?v={asset_hash(asset, cache)}"'

    return original, ASSET_REF.sub(replace, original)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="report stale versions and exit 1 without writing",
    )
    args = parser.parse_args()

    cache: dict[Path, str] = {}
    errors: list[str] = []
    stale: list[Path] = []

    for html_file in tracked_html_files():
        original, updated = process_file(html_file, cache, errors)
        if updated != original:
            stale.append(html_file)
            if not args.check:
                html_file.write_text(updated)

    for err in errors:
        print(f"ERROR: {err}", file=sys.stderr)

    if stale:
        verb = "stale asset versions in" if args.check else "updated"
        for f in stale:
            print(f"{verb} {f.relative_to(REPO_ROOT)}")

    if errors or (args.check and stale):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
