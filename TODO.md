# TODO

Mandatory maintenance backlog from the 2026-07-19 repo audit. Work top-down;
check items off as they land. Full context in
`docs/project/MAINTAINABILITY_AUDIT_2026-07-14.md` and
`site/docs/ACCESSIBILITY_AUDIT.md`.

- [x] **Consolidate CLAUDE.md** — one accurate root `CLAUDE.md`; deleted stale
      `docs/project/CLAUDE.md` + `site/docs/CLAUDE.md` (referenced dead
      scripts, pre-monorepo layout). *Done 2026-07-19.*
- [x] **Remove tracked `._codebase_digest.txt`** (2.5 MB generated dump at
      repo root) and gitignore it. *Done 2026-07-19.*
- [x] **Automate cache busting** (audit P4):
      `scripts/update_asset_versions.py` rewrites `?v=` to content hashes;
      pre-commit auto-fixes + stages; `tests/test_asset_versions.py` guards.
      *Done 2026-07-19.*
- [ ] **Deduplicate `storm_surge_page.js` twin chart functions** (audit P2):
      `updateForecastChart` / `updateHindcastChart` (~380 lines each) → one
      parameterized function. Do before building the Salish Sea forecast
      section — that page will be modeled on this one.
- [ ] **`stations-map.js` data-source unification**: resolve the line-414
      TODO (buoy-vs-wind JSON lookup duplicated between marker and popup
      paths); the "see note above (line 316)" comment pointer has drifted.
- [ ] **Repo hygiene** (audit P5): delete `archive/` (56 tracked files
      polluting grep); move `validate_stations.py` and
      `create_lightstation_db.py` off the repo root; point lint/test output
      files at `logs/`; naming drift (`-v4` suffixes, snake vs kebab) — fix
      opportunistically when files are touched anyway.
- [ ] **Hardcoded DB paths → `lib/config.py`**: `fetch_whiterock_weather.py`,
      `export_wind_24hr_timeseries.py`, `export_hindcast_json.py`,
      `migrate_wind_direction_field.py`.
- [ ] **Accessibility remainder** (`site/docs/ACCESSIBILITY_AUDIT.md`): skip
      link, `aria-current` on nav, ECharts `aria: {enabled: true}`, label the
      duplicate footer nav landmark; then the four low-priority items.

Deferred by choice (revisit only if they hurt): `health_check.py` split
(836 lines); HTML `<head>` boilerplate duplication (no build step by design).

---

**Next feature** (not maintenance): Salish Sea forecast upgrade — RDWPS waves
+ CIOPS-SalishSea water levels. Plan: `docs/project/FORECAST_UPGRADE.md`.

## Feature backlog

Consolidated 2026-07-19 from the former `docs/project/TODO.md` (now
`WORKLOG.md`, completed-work history only). Roughly by priority:

- [ ] **Tighten graph margins** (medium): start with the buoys page ECharts
      (grid left/right/top/bottom, container padding), then audit the other
      chart pages. Prefer a shared pattern over per-page one-offs.
- [ ] **Dev branch + preview subdomain** (medium): `dev` branch served at
      `dev.halibutbank.ca` via a second Caddy site block + git worktree
      (`git worktree add ~/envcan_wave-dev dev`), symlink `site/data` for
      live data. Frontend preview only; backend stays on `main`.
- [ ] **Lighthouse performance reports** (medium): automated runs for key
      pages, track perf/a11y/SEO over time.
- [ ] **Back-to-top button** (low-medium; mobile-first): additive only — no
      nav changes, no library. Circular button, bottom-right thumb zone,
      ≥44px target, clears the gesture bar and warning banner. Hidden until
      ~1 viewport of scroll, fades in/out, hides at page bottom. Theme vars
      for light/dark, accessible label, keyboard-activatable,
      `prefers-reduced-motion` → instant jump, `addEventListener` only.
- [ ] **Mobile ECharts touch behavior** (open bug-ish): cursor/tooltip
      interaction is "funky" on mobile across chart pages; investigate
      ECharts touch/tooltip config, test on real devices.
- [ ] **Backend data audit** (low, rainy-day): compare captured fields vs
      what EC SWOB-ML / NOAA feeds actually provide; parser-log error sweep;
      schema/index review; per-station completeness stats.
