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
- [x] **Deduplicate `storm_surge_page.js` twin chart functions** (audit P2):
      shared scaffolding extracted (`baseSurgeChartOption`, `resolveStation`,
      `ensureChart`, `initStationSelector`, …); update functions now hold only
      their unique series logic. Pixel-verified against baseline screenshots.
      *Done 2026-07-19.*
- [x] **`stations-map.js` data-source unification**: marker/popup lookups
      share `latestStationData()`; wave-vs-wind classification + type labels
      moved to `shared/station-meta.js` (`isWaveStation`/`stationTypeLabel`),
      deduping four hardcoded type lists across three files and fixing the
      unlisted `land_based_wind_station` type. DOM-diff verified.
      *Done 2026-07-19.*
- [x] **Repo hygiene** (audit P5): deleted `archive/` (56 tracked files);
      moved `validate_stations.py` (now reads `lib.config.STATIONS_FILE`)
      and `create_lightstation_db.py` to `scripts/utils/`; removed stale
      root lint/test output files (`.gitignore` already covers them).
      *Done 2026-07-19.* Naming drift (`-v4` suffixes, snake vs kebab)
      stays opportunistic — fix when files are touched anyway.
- [x] **Hardcoded DB paths → `lib/config.py`**: added `WEATHER_DATABASE` +
      `LIGHTSTATION_DATABASE`; converted the four listed scripts plus the
      four lightstation scripts, `health_check.py`'s six copies, and
      `parse_lightstation.py`'s hardcoded `~/envcan_wave` data dirs
      (→ `PROJECT_ROOT`). *Done 2026-07-19.*
- [x] **Accessibility remainder** (`site/docs/ACCESSIBILITY_AUDIT.md`): skip
      link, `aria-current`, ECharts aria descriptions, labeled nav
      landmarks, heading-order fixes, visible tide-select focus ring,
      `prefers-reduced-motion` — all landed and runtime-verified;
      analytics.html left as accepted. *Done 2026-07-19.*

Deferred by choice (revisit only if they hurt): `health_check.py` split
(836 lines); HTML `<head>` boilerplate duplication (no build step by design).

---

**Next feature** (not maintenance): Salish Sea forecast upgrade — RDWPS waves
+ CIOPS-SalishSea water levels. Plan: `docs/project/FORECAST_UPGRADE.md`.

## Feature backlog

Consolidated 2026-07-19 from the former `docs/project/TODO.md` (now
`WORKLOG.md`, completed-work history only). Roughly by priority:

- [ ] **"Security cam" wall view for the webcam page** (idea, user
      2026-08-16): a mode that drops all six cams into a dense grid of
      smaller images side by side with a little padding between them, so
      the whole area reads at a glance instead of scrolling one big card
      at a time. Today `webcams-v4.css` is `repeat(auto-fill, minmax(500px,
      1fr))` with a full card (header, timestamp, metadata) per cam — the
      wall would be a second, tighter layout, not a replacement.
      Open questions: is it a toggle (wall ↔ detail) or the default with
      click-to-enlarge; how much chrome survives at small size (probably
      just name + a stale indicator, since `webcam-stale`/`webcam-stale-error`
      states must stay legible); whether a fixed aspect-ratio box with
      `object-fit: cover` is acceptable given the cams don't share an
      aspect ratio. Ships as an unlisted page first per the preview
      decision below, since it reworks an existing surface.
- [ ] **Tests for the wave-forecast pipeline** (user 2026-08-16): the RDWPS
      chain shipped with no tests of its own — `tests/test_forecast_steps.py`
      covers `taper_time_steps()` and nothing else. Worth having, roughly in
      order of value:
      - **Fetch/parse** (`scripts/fetch/fetch_wave_forecast.py`): the 9999
        masked-cell sentinel becomes `status='masked'` with a NULL value, not
        a 9999 reading; a failed fetch writes no row at all, so missing and
        masked stay distinguishable; the schema migration that relaxes
        `value REAL NOT NULL` is idempotent.
      - **Export**: the JSON carries only the allowlisted fields — the
        `site/data/` surface is public and unscanned (see CLAUDE.md), so an
        upstream response must never reach it wholesale.
      - **Frontend** (`tests/js/`, needs the pure helpers exported the way
        `sunlight.js` now does): `heightAxisMax` floors at 1.0 and grows past
        it; `rowsWithin` windows by elapsed time across the hourly→3-hourly
        taper; `toSortedRows` sorts despite the payload being an object;
        `createDirectionArrows` keeps even spacing across the taper.
      A masked-step fixture is the key asset — this summer's data has 67 of
      them, so capture one before retention drops it.
- [ ] **timeanddate.com embed for the sunlight widget** (small, user
      2026-08-16): the tides page now links out to
      <https://www.timeanddate.com/sun/canada/vancouver> under the daylight
      duration. Check whether they publish an embeddable widget or free API
      we could use in place of (or beside) the plain link — unverified;
      their free tier historically is signup-gated and their site scripts
      would need a CSP allowance, so an outbound link may remain the right
      answer. If nothing embeddable exists, close this and keep the link.
- [ ] **Tighten graph margins** (medium): start with the buoys page ECharts
      (grid left/right/top/bottom, container padding), then audit the other
      chart pages. Prefer a shared pattern over per-page one-offs.
- [ ] **Dev branch + preview subdomain** (medium, DEFERRED 2026-08-15):
      `dev` branch served at `dev.halibutbank.ca` via a second Caddy site
      block + git worktree (`git worktree add ~/envcan_wave-dev dev`),
      symlink `site/data` for live data. Frontend preview only; backend
      stays on `main`. **Decision:** new/additive features preview as
      unlisted pages on `main` instead (noindex meta, no nav/sitemap
      link; promotion = nav + sitemap + drop noindex + add to test
      suites). Revisit this item only for risky changes to shared or
      existing surfaces (nav, shared CSS/JS, in-place page reworks).
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
- [ ] **Map marker decluttering** (low): markers overlap at zoomed-out
      levels, and the station count keeps growing (55 plotted as of
      2026-08-13, 3 added that day). Options: offset/spiderfy colliding
      markers, or thin them by zoom level so only major stations show when
      zoomed out. Until this lands, the buoy map's desktop start zoom is a
      straight trade — it moved 8 → 9 on 2026-08-13, which drops 15 of the
      38 initially-visible stations (all of Haro Strait, Juan de Fuca and
      the west coast) in exchange for legible spacing. Solving this reopens
      that choice; see `initStationsMap` in `site/assets/js/stations-map.js`.
- [ ] **Stop guessing timestamps to find EC data** (policy, do first): MSC's
      usage policy says plainly "Do not request directory listings to assess
      the availability of new data, the AMQPS notification service must be
      used for this need". `scripts/fetch/fetch_lightstation.py` does exactly
      that — it walks `dd.weather.gc.ca/today/bulletins/alphanumeric/.../FP/
      CWVR/HH/` hourly, guessing the last two likely report hours. It is also
      probably redundant: `config/sr3/bc_lightstation_obs.conf` already has
      `accept .*FPCN61.*` and FPCN61 files are arriving in
      `data/lightstation_bulletins/`. See the footprint section of
      `docs/DATA_FEEDS.md`.
- [ ] **Audit duplicate lightstation fetching** (medium): a manual backup
      source was added while debugging why the Tofino-area lightstations
      weren't populating, and it was never removed. Work out whether the HTTP
      poller and the sr3 subscription are genuinely redundant paths we rely
      on, or just duplicate work — and if the former, document *why* so it
      doesn't get "cleaned up" later. Pairs with the item above; do them
      together since they touch the same feed.
- [ ] **Why are English Bay and Southern Georgia Strait so chatty?** (low):
      those two buoys publish ~4,300 and ~4,100 files/day against ~715/day for
      Halibut Bank, La Perouse and Sentry Shoal — 6× the others, and 80% of
      our whole Datamart download volume (measured 2026-08-15). Could be
      genuinely higher-rate instruments, could be duplicate postings we could
      filter. Note `bc_wind_stations.conf` carries
      `reject .*minute-swob\.xml.*` but `bc_buoys.conf` has no equivalent.
- [x] **Spread out the storm-surge fetch** *(done 2026-08-16)*: taper landed as
      specified below (hourly to 72 h, then 3-hourly — 129 of 241 steps,
      2,894 → 1,548 req/day) and `FETCH_DELAY` went 0.5 s → 2 s (1.05 → 0.41
      req/s over ~32 min). The taper is now `lib/forecast_steps.py`, shared with
      the wave fetcher and unit-tested in `tests/test_forecast_steps.py`. Two
      things fixed alongside: the stale-lock threshold was 5 min against a
      23-minute run (→ 1 h), and the follow-up `water_level_export` at :35 fired
      mid-fetch and re-read the previous run's file (→ 2:05/14:05). Downstream
      was already safe — `water_level_export.interpolate_surge()` interpolates
      linearly, the hindcast export only queries 38–61 h leads (inside the fine
      window), and the page's x-axis is `type: "time"`. Original analysis:
      `fetch_storm_surge.py` was 2,894 requests/day — 5× the wave forecaster and our largest HTTP load
      by far — because it pulls all 241 hourly steps of the 10-day GDSPS
      forecast for 6 stations. Surge is smooth enough that this is wasted:
      measured over 168 archived forecast series (40,256 hourly steps),
      hour-to-hour change is **mean 1.55 cm, p95 4.10 cm, max 11.3 cm**, and
      the error from sampling coarser then linearly interpolating is:

      | sampling | mean err | p95 | max |
      |---|---|---|---|
      | 2-hourly | 1.07 cm | 2.95 cm | 8.40 cm |
      | 3-hourly | 1.35 cm | 3.63 cm | 9.83 cm |
      | 4-hourly | 1.51 cm | 3.95 cm | 11.20 cm |

      All well inside GDSPS's own error. **Chosen shape (user, 2026-08-15):
      hourly to 72 h, then 3-hourly to 240 h** — 129 of 241 steps, ~1,548
      requests/day (−47%). Note the interpolation error barely moves with the
      fine-window length (mean 1.45 cm at 48 h, 1.50 at 72 h, 1.55 at 96 h)
      because surge variability doesn't decay with lead time — so the fine
      window is purely a choice about how much hourly detail to offer, not an
      accuracy trade. 72 h covers the three-day window people plan around and
      costs ~190 requests/day more than 48 h. Touches a live user-facing
      chart, so eyeball the storm-surge page at the coarser resolution before
      committing.

      **Also raise `FETCH_DELAY` while in there (user, 2026-08-15).** MSC's
      guidance is "about 1 request per second". With ~0.45 s of network per
      request, the current 0.5 s delay puts a burst at **1.05 req/s — right at
      that line — sustained for 23 minutes.** Daily totals were never the risk
      here; the burst rate was. Tapering alone doesn't fix it (fewer requests,
      same rate). At 2 s the tapered run is 0.41 req/s over ~32 min, which is
      free for a job that goes 2×/day. The wave fetcher was moved to 1.5 s
      (0.51 req/s) for the same reason in `d194792`'s follow-up. Nothing
      downstream is time-critical: the water-level export runs every 10 min
      regardless.
- [ ] **Smoothing on the storm-surge plots** (low, **deferred 2026-08-16** —
      explicitly not done with the taper): cosmetic, and safe *if* it can't
      overshoot. Note the forecast series in `storm_surge_page.js` already sets
      `smooth: true`, so the risk below is live today, not hypothetical — and
      the 3-hourly tail past 72 h gives the spline more room to overshoot. ECharts `smooth: true` uses
      a spline that can overshoot at sharp peaks — on a surge chart that would
      invent a higher peak than the model forecast, which is the one thing
      this plot must not do. Use a monotone interpolation or a damped
      `smooth: 0.3` and check a steep event against the raw points. Note that
      at 2-hourly sampling the line already reads smooth at 10-day zoom, so
      this may be unnecessary once the taper lands.
- [ ] **Backend data audit** (low, rainy-day): compare captured fields vs
      what EC SWOB-ML / NOAA feeds actually provide; parser-log error sweep;
      schema/index review; per-station completeness stats.

Frontend polish, added 2026-08-15 — all four **done 2026-08-15**:

- [x] **Forecasts page: "coming soon" flag for RDWPS waves** — `.forecast-coming-soon`
      callout above `#forecast-container` in `forecasts.html` (styles in that
      page's own `<style>` block; remove both when waves ship).
      **Removed 2026-08-16** along with its styles: the wave preview now sits
      at the bottom of the same page, so the callout was promoting content
      one scroll below it.
- [x] **Winds page: condense + collapse the footnote wall** — caveat text in
      `config/stations.json` shortened, and `renderCaveatFootnotes`
      (`wind-stations.js`) now wraps the notes in a collapsed
      `<details class="station-caveat-notes">` ("Station notes (N)"), 29 px
      instead of a four-paragraph wall. A delegated click handler opens the
      `<details>` before an asterisk jump, since a collapsed one hides the
      target from fragment navigation.
- [x] **Forecasts page: reorder Related Resources** — Wind & Pressure Maps now
      precedes Aviation Forecasts.
- [x] **Footer health indicator: reporting-fraction color thresholds** —
      `footer.js` drives the badge from reporting % (green ≥ 93, yellow ≥ 75,
      red below) instead of `overall_status`. `data_freshness` is excluded from
      the escalation path (the fraction already covers it); the other checks —
      storage, database integrity, export freshness — can still force the badge
      worse, so a broken pipeline never shows green.
