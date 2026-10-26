#!/usr/bin/env python3
"""Git merge driver for site HTML: ?v= cache-bust hashes never conflict.

Every main -> dev rebase used to stop on HTML lines that differed only in
their ?v= hashes (update_asset_versions.py rewrites them on every commit that
touches an asset), seven times in one afternoon. This driver merges the pages
with the hashes taken out, so only real edits can conflict, then puts a hash
back on each reference:

- only one side changed an asset's hash: that side's (the asset itself
  merged cleanly from that side, so the hash is the right one);
- both sides changed it: ours, stale on purpose. The asset was merged from
  both, so neither hash matches it; the post-rewrite hook runs
  update_asset_versions.py after the rebase, which recomputes it.

Without the config below git falls back to its normal text merge, so a fresh
clone merges exactly as before. Install (once per clone; worktrees share it):

    git config merge.asset-versions.name "site HTML, ?v= hashes ignored"
    git config merge.asset-versions.driver \
        "python3 scripts/hooks/merge_asset_versions.py %O %A %B %L %P"

.gitattributes routes site/**/*.html here. Git calls it with the ancestor,
ours (also the output file) and theirs, the conflict-marker size and the
path; exit 0 for a clean merge, 1 when conflicts are left in the file.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

# The same references update_asset_versions.py rewrites: a local .js/.css
# path with its ?v= hash.
VERSIONED = re.compile(r'(?P<path>[^"?\s]+\.(?:js|css))\?v=(?P<hash>[0-9A-Za-z]*)')


def hashes(text: str) -> dict[str, str]:
    return {m.group("path"): m.group("hash") for m in VERSIONED.finditer(text)}


def neutral(text: str) -> str:
    return VERSIONED.sub(lambda m: f"{m.group('path')}?v=", text)


def main() -> int:
    base_file, ours_file, theirs_file, marker_size, path = sys.argv[1:6]
    base, ours, theirs = (
        Path(f).read_text(encoding="utf-8") for f in (base_file, ours_file, theirs_file)
    )

    with tempfile.TemporaryDirectory() as tmp:
        files = []
        for name, text in (("ours", ours), ("base", base), ("theirs", theirs)):
            f = Path(tmp) / name
            f.write_text(neutral(text), encoding="utf-8")
            files.append(str(f))
        result = subprocess.run(
            [
                "git", "merge-file", "-p", f"--marker-size={marker_size}",
                "-L", f"{path} (ours)", "-L", f"{path} (base)", "-L", f"{path} (theirs)",
                *files,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    # merge-file exits with the number of conflicts, negative on error.
    if result.returncode < 0:
        sys.stderr.write(result.stderr)
        return 2

    base_h, ours_h, theirs_h = hashes(base), hashes(ours), hashes(theirs)

    def restore(m: re.Match) -> str:
        p = m.group("path")
        took_theirs = ours_h.get(p) == base_h.get(p) and p in theirs_h
        h = theirs_h[p] if took_theirs else ours_h.get(p, theirs_h.get(p, ""))
        return f"{p}?v={h}"

    Path(ours_file).write_text(VERSIONED.sub(restore, result.stdout), encoding="utf-8")
    return 1 if result.returncode > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
