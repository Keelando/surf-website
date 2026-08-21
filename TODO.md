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

- [ ] **More marine text-forecast zones than north/south of Nanaimo**
      (user 2026-08-18): we parse one file, `m0000028_en.xml`, which carries
      both Strait of Georgia zones. Adjacent water — Juan de Fuca Strait,
      Haro Strait, Howe Sound, the west coast of Vancouver Island — is
      published as separate zone files we currently discard.
      *Cost is not the obstacle:* this is an AMQP push feed, and
      `config/sr3/marine_forecast.conf` already subscribes to the whole
      `*.WXO-DD.marine_weather.pacific.#` subtopic. Every one of those files
      is already being *announced* to us; the `accept .*m0000028_en\.xml.*`
      line is the only thing stopping the download. So extra zones cost **zero
      HTTP requests** against the 86,400 guidance — they are a parser and UI
      question, not a policy one.

      **Done 2026-08-20 — the parser and UI side.** The pipeline no longer
      cares how many zones arrive:
      - `scripts/parse/parse_marine_forecast.py` parses *every* zone XML on
        disk, newest file per zone code, into a top-level `areas` map.
        `ZONE_MAP` is gone: area and zone keys are slugified straight from the
        XML (`"Strait of Georgia - north of Nanaimo"` →
        `strait_of_georgia_north_of_nanaimo`), so **a new zone needs no parser
        edit at all** — only the `accept` regex. Two zone codes naming the
        same area merge rather than clobber.
      - `site/data/marine_forecast.json` is now
        `{generated_utc, areas: {<area>: {locations: {<zone>: …}}}}`.
      - `site/assets/js/forecasts.js` renders one zone at a time, chosen from
        an `<optgroup>`-by-area `<select>` (`#forecast-zone-select`), with the
        choice persisted in `localStorage` and overridable by URL hash.
      - `site/assets/js/warning-banner.js` walks every area, so a warning in a
        zone you have *not* selected still surfaces sitewide. This is what
        makes one-zone-at-a-time safe; do not regress it.
      - `tests/test_marine_forecast_parse.py` (13 tests) covers slugify,
        unknown zones, warning-only zones, newest-file-per-zone, area merge,
        and unparsable input.

      *Resolved 2026-08-20 — the zone codes.* MSC publishes a lookup table
      mapping every `m#######` code to its region name and domain:
      <https://collaboration.cmc.ec.gc.ca/cmc/cmos/public_doc/msc-data/marine-weather/marine_region_list_en.csv>
      (linked from the marine-weather datamart readme on eccc-msc.github.io;
      ISO-8859 encoded, so read it as latin-1 — grep treats it as binary).
      It confirms `m0000028` = Strait of Georgia, and lists **17 Pacific
      regions total**. This retires the `logLevel debug` enumeration route
      entirely: `logLevel` is back to `info`, the service is restarted, and
      `~/marine_zone_watch.sh` is deleted. The debug-log trick is still the
      right tool when no published table exists — see the note in the
      marine-text-forecast-zones memory.

      **A datamart file is a *bulletin*, not a zone**, and one bulletin can
      carry several zones (`m0000028` carries both Strait of Georgia zones).
      The weather.gc.ca per-zone RSS ids expose the grouping. The Georgia
      Basin is **6 bulletins covering 9 zones**:

      | Code | Region | Zones |
      |------|--------|-------|
      | `m0000028` | Strait of Georgia | north of Nanaimo, south of Nanaimo *(have)* |
      | `m0000009` | Juan de Fuca Strait | east entrance, west entrance, central strait |
      | `m0000064` | Haro Strait | Haro Strait |
      | `m0000102` | Howe Sound | Howe Sound |
      | `m0000010` | Johnstone Strait | Johnstone Strait |
      | `m0000065` | West Coast Vancouver Island South | WCVI South |

      Remaining steps:
      1. widen `accept` to the five additional codes above,
      2. gate the new zones behind a shared vetted-zone allowlist imported by
         **both** `forecasts.js` and `warning-banner.js` — the banner is a
         sitewide surface, so widening `accept` without it would publish
         unvetted zones' warnings to every page,
      3. update the zone count in `docs/DATA_FEEDS.md`.

      *Sizing, measured 2026-08-20 from the live per-zone RSS feeds:* the
      seven zones we do not yet carry total **570 words**, so all nine lands
      around **~720 words** — three entries per zone at roughly 40 words each.
      West Coast Vancouver Island South carries an extra `Waves for…` entry
      the Strait zones do not, which the renderer must not assume away.

      **Shipped 2026-08-20 — the five extra bulletins.**
      `config/sr3/marine_forecast.conf` now accepts all six Georgia Basin
      bulletins (deployed + service restarted). First arrival expected at the
      ~22:51Z issuance; AMQP is push-only, so nothing back-fills.

      **Warning-banner scope (user decision 2026-08-20):** the banner fires for
      **Strait of Georgia north + south of Nanaimo only**. All nine zones render
      on the forecasts page; only home waters interrupt you sitewide. Lives in
      `DEFAULT_BANNER_ZONES` in `site/assets/js/warning-banner.js`. This is
      deliberately *not* a second copy of the carried-zone list — `accept` is
      the only source of truth for what we carry.

- [x] **Map popup layout cleanup** — DONE 2026-08-20. Two faults, both measured
      rather than eyeballed: (1) on a 360x640 phone a full buoy popup was 450px
      of content against a 384px budget, so it scrolled; (2) popup width came
      from whatever the longest line happened to be — twelve popups produced
      **eight** different widths (286-349px), and the ones hitting Leaflet's
      300px cap looked as if they were reserving space for the optional
      "Wave Forecast" button. Fixes: `POPUP_OPTIONS = {minWidth: 280,
      maxWidth: 280}` on all three maps so every popup is one width; a
      `.popup-actions` flex row so one button and two buttons measure the same;
      and a mobile block trimming Leaflet's own `.leaflet-popup-content` margin
      (13px/19px, the biggest single contributor) plus our paddings. Result:
      one width everywhere (329 desktop / 317 mobile), nothing scrolls.
      Also removed the `.view-data-btn` inline style that had been copy-pasted
      five times across three files — CSS already defined all of it.

- [ ] **Let users opt in to warning zones** (user 2026-08-20): the sitewide
      banner currently fires for a hardcoded home-waters pair. Users should be
      able to choose which of the zones we carry may interrupt them. The
      backend for this already exists — `getBannerZones()` in
      `warning-banner.js` is the single extension point; the available-zone
      list comes from `marine_forecast.json` itself, not a hardcoded list.
      Needs: a picker UI, `localStorage` persistence (match the
      `selected_marine_zone` pattern in `forecasts.js`), and a sensible
      default for first-time visitors (`DEFAULT_BANNER_ZONES`).

- [x] **`warning-banner.js` test coverage** — DONE 2026-08-20. It was a classic
      script with no exports, so the zone filter could not be unit-tested. The
      pure logic moved to `site/assets/js/shared/warning-zones.js` (zone
      selection, active-warning collection, severity/icon mapping) with 14
      tests in `tests/js/warning-zones.test.mjs`; `warning-banner.js` kept the
      DOM half and became an ES module to import it, so the seven pages that
      load it now use `<script type="module">`. Playwright 16/16 still pass.

      *Still open — the layout, deferred by the user 2026-08-20.* Whether the
      page keeps the one-zone-at-a-time dropdown (winds-style) or moves to
      collapsible per-area sections (buoys/lightstations-style) is deliberately
      **not decided until every zone is actually flowing** — with two zones the
      wall-of-text tradeoff is theoretical. The dropdown stays in the meantime.
      Note the two pages differ less than they look: every data page on the
      site already uses a `<select>`; what buoys and lightstations add is that
      *all* stations are also on the page as scannable cards, which works
      because cards are dense and differentiable. Prose is neither.

- [ ] **Forecasts page needs more flow** (user 2026-08-20, deferred): the page
      is a long single scroll with nothing to move between its stops. But the
      framing the user gave is the useful part, and it is not "add jump links":

      > the top is **zone forecast**, wind, *text*; the second is **point
      > forecast**, wind + wave, *graphical/tabular*.

      That is the page's real information architecture — two forecasts that
      differ on two axes at once, **spatial scope** (an EC marine area vs a
      single point at Halibut Bank) and **representation** (prose vs data) —
      and the page currently says neither out loud.

      *The root problem is a missing label, not missing navigation.* Section two
      has `<h2>🌊 Wave &amp; Wind Forecast — Halibut Bank</h2>`. Section one has
      **no section heading whatsoever** — tagline, then straight into
      `#forecast-zone-selector` and `#forecast-container`, where the only `<h2>`
      is the *zone name* rendered by `forecasts.js`. So `<h2>` does double duty:
      a section label in one place, a data value in the other. A reader is left
      to infer "point forecast" from the words "Halibut Bank" appearing in one
      heading. Fixing that is also a heading-hierarchy fix — see
      `site/docs/ACCESSIBILITY_AUDIT.md`.

      *Likely shape:* name the two sections for what they are — "Zone Forecast"
      (with the zone name demoted to `<h3>` or folded into the selector) and
      "Point Forecast — Halibut Bank" — and the jump nav then writes itself,
      because there is finally something to jump *between*. Do the labels first
      and re-judge whether a nav strip is even wanted; on a two-stop page it may
      not be.

      *Sequence it AFTER the dropdown-vs-sections layout call*, not before: a
      jump nav and the zone `<select>` compete for the same top-of-page strip,
      and if the page moves to per-area collapsible sections the anchors change
      shape entirely.

      *Watch the hash.* `resolveInitialZone()` in `forecasts.js` reads
      `location.hash` and treats it as a zone key. A `#wave-forecast-section`
      hash simply is not a zone key, so it falls through to the stored/default
      zone and the anchor scroll still works — but that is a *coincidence of
      namespaces*, not a design. Any jump nav sharing the hash with zone deep
      links needs that overlap made explicit (prefix the zone hashes, or route
      both through one handler), or a future zone slug will collide with a
      section id and silently change which zone the page opens on.

- [ ] **Subscribable warning banners** (user 2026-08-20): let the reader choose
      *which* zones' warnings raise the sitewide banner, instead of all of them.
      This follows directly from carrying every Pacific zone. Today
      `collectActiveWarnings` in `site/assets/js/warning-banner.js` walks every
      area deliberately — that is what makes the forecasts page safe to show one
      zone at a time, since a gale two zones over still surfaces. But once the
      feed carries the west coast of Vancouver Island and Queen Charlotte Sound,
      the same behaviour banners a boat in the Strait of Georgia about water it
      will never see, and a banner that cries wolf is a banner people dismiss on
      reflex — which costs exactly the warning that mattered.
      *Shape:* the same `localStorage` pattern the banner already uses for
      dismissals, and the forecasts page now uses for zone choice
      (`selected_marine_zone`). Probably a checklist of zones, defaulting to
      **all zones on** — opting *out* of a marine warning must be a deliberate
      act, never a default. Severity is a possible second axis (storm always
      banners regardless of zone), but zone is the one that matters first.
      *Do not* start this before the zone list is final — the whole point is
      tailoring a list that does not exist yet.

- [ ] **Revisit whether the repo should stay public** (user 2026-08-17):
      decide deliberately rather than by inertia. Audience today is one
      follower and one star (self-awarded), so the outward benefit is close
      to nil in practice.
      *What public currently costs:* every commit is scrutinised, the 07:17
      cron pushes unattended to a public remote, and git history permanently
      holds the expired v1 Windy key plus the Surrey credentials (neither
      needs rotating — see `docs/SECRETS.md` — but they can't be un-published
      short of a history rewrite).
      *What it buys:* the discipline is real and worth keeping either way —
      the pre-commit secret scan, `tests/test_secrets.py`, and the
      two-public-surfaces rule all exist because the repo is public.
      *Things to weigh before flipping:*
      - `site/data/` stays public regardless. halibutbank.ca is the surface
        that actually matters, and going private does nothing for it — so
        none of the export-side hygiene can be relaxed.
      - `site/components/about-generic.html` links to the GitHub repo from
        every page's About section; that link would 404 for visitors.
      - The `forgejo` remote (survivor.local) already mirrors main+tags
        nightly, so backup is not a reason to stay on public GitHub.
      - Reversible in one direction only: private→public later re-exposes
        the whole history, so a future flip back needs the same thinking.
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
- [x] **Track buoy reporting lag over time** (user 2026-08-17): **collecting
      as of 2026-08-17.** `lib/reporting_lag.py` + `reporting_lag.sqlite`,
      one row per observation, written by the buoy export after the JSON
      lands (no new fetch, no new cron — the pipeline already runs it every
      3 min). Lag is split into its two components rather than one number:
      `source_lag` (instrument → our DB, upstream + fetch/parse) and
      `publish_lag` (our DB → site, export cadence). First live readings
      confirm the split is bimodal as expected — AMQP-pushed EC buoys land
      at ~4 min source lag, polled NOAA/Surrey at 25–70 min — so compare a
      station against its own baseline, never a global threshold.
      `stale_seconds` is the slow-degradation signal the 2-hour freshness
      window can only answer yes/no about. Seeded rows (first sighting per
      station) have an inflated publish lag by construction: exclude them
      with `WHERE seeded = 0`.
      **Tide wired too (2026-08-17)**, aimed at Surrey: `export_tide_json.py`
      records only stations that published an *observation*, never ones
      showing prediction-only — a prediction is computed, not measured, and
      it is exactly what masked the stalled Surrey feed on the page before.
      First readings: Surrey ~69–80 min source lag on both the wave and tide
      channels (so it is a FlowWorks-wide publishing delay, not per-channel),
      DFO IWLS 0.6–5.8 min.
      Remaining: (a) extend to wind/lightstation — the writer is
      source-agnostic, each export needs ~3 lines; (b) a per-station
      distribution (median / p90 / worst) on an unlisted page; (c) `source_lag`
      backfill, valid for every source *except* Surrey tides before
      2026-08-17 — see below.
- [x] **Surrey tide observations: stop resetting `recorded_at`**
      (2026-08-17): `fetch_surrey_tides.py` used `INSERT OR REPLACE`, which
      deletes and re-inserts, so every row of the 24 h re-fetch window got a
      fresh `recorded_at` every 20 min — Surrey history read as ~22 h of
      fake lag. Not switched to `INSERT OR IGNORE`, because Surrey genuinely
      revises: a live-vs-stored comparison found the newest point in each
      channel corrected (2 mm on the channel, **32 mm** on the ocean gauge)
      while the other 1,494 points were identical. So it is now an
      `ON CONFLICT DO UPDATE` that updates the value and leaves
      `recorded_at` alone — matching what `fetch_surrey_wave_v2.py` already
      did. Predictions were always `INSERT OR IGNORE` and stay that way;
      astronomical predictions don't change. Pre-2026-08-17 Surrey tide
      `recorded_at` values are unrecoverable. `surrey_geodetic_data` still
      uses REPLACE — harmless today (not lag-tracked), fix if it ever is.
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
- [x] **Stop guessing timestamps to find EC data** *(done 2026-08-21)*: the
      FPCN61 poller was walking `dd.weather.gc.ca/.../FP/CWVR/HH/` hourly,
      guessing the last two likely report hours — the one practice MSC's usage
      policy names outright. `scripts/fetch/fetch_lightstation.py` is deleted
      and `parse_lightstation.py` now reads FPCN61 from
      `data/lightstation_bulletins/`, where sr3 was already putting it.
- [x] **Audit duplicate lightstation fetching** *(done 2026-08-21, with the
      above)*: the poller was a backup added while debugging the Tofino-area
      stations, and it turned out to fill no gaps at all. Over three days sr3
      and the poller received exactly the same set of FPCN61 bulletins, the
      files were byte-identical, and sr3 had each one ~55 minutes sooner (it is
      pushed on publication; the poller could only notice at the next hourly
      tick). Neither path ever saw an 09xx UTC bulletin, so that slot simply is
      not published — it was never a gap the poller could have filled.

      The real gap was a parser bug the backup poller could never have fixed,
      found while retiring it: `is_stale_retransmission()` compared the
      bulletin's **UTC** header day against the **local** day its text names,
      so every bulletin issued 00:00–07:00 UTC (the 00/03/06 slots, which are
      5/8/11 PM the previous day in Pacific time) was discarded as a stale
      retransmission. Measured over the log history: 31 rejections at each of
      those three hours, 0 at the four working hours — 3 of the 8 daily
      bulletins, every day. Fixed by comparing in `America/Vancouver`;
      recovered 89 observations on the first run.
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
