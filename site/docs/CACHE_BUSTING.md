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

## Scope and limits

- External URLs (`http(s)://`, `//`) are left untouched.
- Only `<script src>` / `<link href>` in HTML are versioned. Static ES-module
  `import`s inside JS resolve without query params; they rely on the origin's
  `Cache-Control: no-store` for non-image assets (Caddyfile), same as before.
- The `-v4` *filename* suffixes (`style-v4.css`, …) are historical naming, no
  longer a cache-busting mechanism. Removing them is on `TODO.md` (naming
  drift), safe to do any time since versions now live in the query string.
