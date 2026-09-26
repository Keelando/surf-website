# Dev preview (`dev` branch → dev.halibutbank.ca)

How to change shared parts of the frontend without breaking the live site.
Set up 2026-09-26 for the UI overhaul.

## When to use which

| Change | Where it previews |
|--------|-------------------|
| A **new page** that nothing links to yet | An unlisted page on `main`: `noindex` meta, no nav link, not in `sitemap.xml`, not in the Playwright/screenshot page lists. Promote it by adding all four. |
| A change to **shared or existing surfaces**: the nav, `style-v4.css`, `site/assets/js/shared/`, the hero, an in-place page rework | The `dev` branch, served at dev.halibutbank.ca. |
| **Backend**: fetch, parse, export, cron, sr3, `/api/v1` | Always `main`. There is no dev backend. |

The unlisted-page trick cannot protect shared files, since the live pages
load the same `style-v4.css` and shared modules. A `/beta/` path prefix
does not work either, because every page uses root-absolute URLs
(`/assets/…`, `/data/…`, `/winds.html`), so a page under `/beta/` would load
production's assets and link out of the preview. A separate hostname keeps
every path identical.

## Layout

`dev` is checked out as a **git worktree**, i.e. a second working directory
sharing this repo's `.git`:

```
~/envcan_wave        main  ← production serves this working tree, live
~/envcan_wave-dev    dev   ← the preview serves this one
```

Three things in the dev worktree are **symlinks into the main checkout**,
not copies:

| Path | Why |
|------|-----|
| `site/data` | The preview shows live exports. It is gitignored, so a fresh worktree has none. |
| `.venv` | The pre-commit hook calls `.venv/bin/…` relative to the repo root. |
| `node_modules` | For ESLint, Biome and Playwright. |

These are listed in `.git/info/exclude` (local, untracked, shared by both
worktrees). `.gitignore` cannot cover them: its patterns (`site/data/`,
`.venv*/`, `node_modules/`) end in `/`, which matches directories only,
never symlinks.

The preview's web-server block shares its security headers (CSP included)
with production's, so a CSP problem shows up on dev before it ships. It
adds `X-Robots-Tag: noindex`, its own `Disallow: /` robots.txt, and
`Cache-Control: no-cache` on everything. The server and tunnel wiring is
deliberately not documented in this public repo.

## Everyday work

```bash
cd ~/envcan_wave-dev          # edit, commit here as usual
```

- Commits on `dev` run the same pre-commit hook (secret scan, ruff, asset
  versions, pytest, ESLint) as `main`.
- Changes appear on dev.halibutbank.ca as soon as the file is saved (the
  server reads the working tree). Hard-refresh if needed.
- **The preview reads production data.** If a dev page needs a new JSON
  field, add the export change on `main` first. It ships to the live site
  harmlessly (nothing reads the field yet), and dev picks it up
  immediately via the symlink.

## Traps

1. **Stray test servers.** `tests/playwright/serve.py` serves the `site/` of
   *whichever checkout started it*, and Playwright is configured with
   `reuseExistingServer`. If a server started from `~/envcan_wave` is still
   on port 4173, running the suite in `~/envcan_wave-dev` tests **main's**
   files and passes. Before testing on dev, make sure nothing is listening:
   `ss -ltn | grep 4173` should print nothing.
2. **Rebases skip the pre-commit hook**, so `?v=` hashes can go stale on
   replayed commits. After every rebase run
   `.venv/bin/python scripts/update_asset_versions.py`, and commit if it
   changed anything. `tests/test_asset_versions.py` catches it otherwise.
3. **`?v=` conflicts are not real conflicts.** When `main` and `dev` both
   touched an asset, the HTML `?v=` lines conflict. Take either side, run
   `update_asset_versions.py`, and `git add` the HTML.
4. **Python in the dev worktree imports `lib/` from the main checkout.** The
   venv is the main checkout's, and its editable install points there.
   Harmless for a frontend-only branch, but it is another reason backend
   work stays on `main`.
5. **Never `npm install` in the dev worktree.** `node_modules` is main's.
   Add a dev dependency on `main`.
6. **A branch can be checked out in only one worktree.** `git checkout main`
   inside `~/envcan_wave-dev` fails. Do `main` work in `~/envcan_wave`.
7. **`dev` is not in the nightly backup**, which pushes `main` and tags
   only. Push it yourself (`git push forgejo dev`) after meaningful work.
   Pushing to `origin` publishes the branch.

## Shipping dev to production

`main` keeps moving (the nightly auto-backup, backend work), and merges
are `--ff-only`, so rebase first:

```bash
cd ~/envcan_wave-dev
git rebase main
.venv/bin/python scripts/update_asset_versions.py   # trap 2
npm test                                            # trap 1 first

cd ~/envcan_wave
git merge --ff-only dev       # live the moment this lands:
git push origin main          # production serves this working tree
```

The merge **is** the deploy, so do it when you are ready for users to see
it. Afterwards `dev` equals `main`; keep using it for the next round.

## Rebuilding the worktree

If `~/envcan_wave-dev` is lost, or the repo is re-cloned (which loses
`.git/info/exclude`):

```bash
cd ~/envcan_wave
printf '/site/data\n/.venv\n/node_modules\n' >> .git/info/exclude
git worktree add ~/envcan_wave-dev dev
cd ~/envcan_wave-dev
ln -s ~/envcan_wave/site/data site/data
ln -s ~/envcan_wave/.venv .venv
ln -s ~/envcan_wave/node_modules node_modules
git status --short            # must be empty
```

To retire it: `git worktree remove ~/envcan_wave-dev` (the branch survives).
