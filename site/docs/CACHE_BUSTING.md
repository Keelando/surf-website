# Cache Busting

**Automated since 2026-07-19.** Manual `?v=` bumping is obsolete.

## How it works

`scripts/update_asset_versions.py` rewrites the `?v=` query param on every
local `.js`/`.css` reference (`src`/`href`) in tracked `site/**/*.html` files
to the first 10 hex chars of the asset's SHA-1 content hash:

```html
<script src="/assets/js/theme-manager.js?v=f9b331e56b"></script>
```

Content-hash versions are idempotent — the URL only changes when the file's
bytes change, which is exactly when caches need busting.

## Workflow

Edit JS/CSS freely and commit. Nothing else to do:

- **Pre-commit hook** runs the updater, stages any refreshed HTML, and
  proceeds. The nightly auto-backup commit gets the same treatment.
- **`tests/test_asset_versions.py`** fails the suite if a reference is stale
  or points at a missing file (dangling refs are hard errors in the script).
- Manual run: `.venv/bin/python scripts/update_asset_versions.py`
  (`--check` to report without writing).

## Gotcha: the hook stages whole HTML files

The pre-commit hook refreshes versions from the **working tree**, then
`git add`s each HTML file it touched — the whole file, not a hunk. So any
HTML you staged *partially* gets replaced by its full working-tree content
on commit, silently pulling in changes you meant to leave for a later commit.

This only bites when splitting one working tree into several commits, but it
bites hard: it looks like the split worked (the pre-commit output does name
the files it re-staged) and the extra changes only show up in the commit
afterwards.

`git add -p` on an HTML file is therefore not enough. Instead, make the
working tree *be* the intermediate state you want to commit:

```bash
git stash -u                                   # park the finished work
git checkout HEAD -- site/thepage.html         # or rebuild the intermediate
# ...apply only the first commit's edits, commit...
git stash pop                                  # restore, commit the rest
```

Reconstructing the intermediate by hand is usually quicker than fighting the
index, and it has the better property: each commit is a state that actually
ran, not a slice of one. Worth checking afterwards that every commit in the
range is self-consistent:

```bash
for c in $(git rev-list HEAD~4..HEAD); do
  git checkout -q "$c" && .venv/bin/python scripts/update_asset_versions.py --check
done; git checkout -q main
```

Hit while splitting the 2026-08-26 forecasts-page work (CSS extraction,
`setSafeHTML` consolidation and a bug fix) into four commits.

## Scope and limits

- External URLs (`http(s)://`, `//`) are left untouched.
- Only `<script src>` / `<link href>` in HTML are versioned. Static ES-module
  `import`s inside JS resolve without query params; they rely on the origin's
  `Cache-Control: no-store` for non-image assets (Caddyfile), same as before.
- The `-v4` *filename* suffixes (`style-v4.css`, …) are historical naming, no
  longer a cache-busting mechanism. Removing them is on `TODO.md` (naming
  drift), safe to do any time since versions now live in the query string.
