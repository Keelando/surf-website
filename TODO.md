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

**Next bugfix**: none queued. The Ambleside stale-frame bug is fixed
(2026-09-06, dedupe), as is the `target="_blank"` stripping it turned up.
The lightstation parser's two bugs are both fixed — the `FOOT` sea heights on 2026-09-04, the
duplicated observations on 2026-09-06. Write-up:
`docs/project/LIGHTSTATION_PARSE_FIXES.md`.

## Feature backlog

Consolidated 2026-07-19 from the former `docs/project/TODO.md` (now
`WORKLOG.md`, completed-work history only). Roughly by priority:

### Queued 2026-09-23 (frontend session), in the order agreed

- [x] **Performance quick wins, before the redesign.** Lighthouse (live,
      mobile) before → after, 2026-09-23:

      | page          | score   | LCP         | TBT          | CLS         |
      |---------------|---------|-------------|--------------|-------------|
      | home          | 45 → 71 | 5.1 → 4.3 s | 900 → 500 ms | 0.17 → 0    |
      | storm surge   | 63 → 87 | 5.7 → 3.5 s | 570 → 200 ms | 0 → 0.02    |
      | forecasts     | 43 → 94 | 6.6 → 2.6 s | 440 → 120 ms | 0.54 → 0    |
      | winds         | 57 → 82 | 6.7 → 3.8 s | 660 → 260 ms | 0.02 → 0    |
      | lightstations | 54 → 69 | 6.8 → 4.1 s | 970 → 680 ms | 0.02 → 0    |

      What moved it:
      - **Hero photo**: 317 KB JPEG → 960/1600 px AVIF (53/90 KB) with WebP
        fallback, chosen by the same media query in `style-v4.css` and a
        `<link rel="preload" fetchpriority="high">` in every `<head>` (it is
        the LCP element, and as a CSS background it was undiscoverable).
      - **`defer` on every classic script** except `theme-manager.js`. Order
        is preserved (deferred and module scripts share one ordered list);
        no page has inline JS.
      - **Trimmed ECharts** (`scripts/build/build_echarts.sh`, entry
        `scripts/build/echarts-entry.js`): 1,025 → 553 KB raw, 348 → 188 KB
        gzipped. Line + scatter only; pixel-identical A/B against the full
        build on every chart. Trap: esbuild's `--global-name` exposes a
        getter-only namespace, which silently dropped chart-utils'
        `echarts.init` wrapper (aria + lazy); the entry assigns a plain object.
      - **Charts draw on scroll**: the `echarts.init` wrapper in
        `chart-utils-v4.js` queues `setOption`/`clear` until the chart is
        within 300 px of the viewport. One place, every page. The screenshot
        spec scrolls each page first.
      - **CLS → 0 on all five pages**: nav placeholder sized to the real nav
        (`.nav-slot`, 49/71 px); the seven single-use tagline components
        inlined; the home hero panel's height reserved; forecasts and winds
        hide their static sections (`aria-busy`) until the first render
        instead of showing them and shoving them down.
      Left: six render-blocking stylesheets (critical-CSS inlining), Leaflet
      on the home page, and the nav wraps to 89–95 px between 601 and 1279 px
      (the open nav-overflow bug), where a small shift remains.
      **Critical-CSS inlining: measured and deferred, 2026-09-24.** Home was 83 on
      that run; Lighthouse credits ~0.5–0.65 s to the six blocking resources,
      but that is simulated slow 4G. The real trace had a 72 ms first byte, the
      sheets load in parallel (one round trip), and repeat visitors hit the
      1-year immutable cache. Inlining would also cover the nav + hero the
      mockup is about to change. Revisit after the redesign if at all;
      home TBT (400 ms, JS) is the bigger lever.
- [x] **Map "late" and "down"**, 2026-09-23. `lib/report_status.py` derives
      each station's rhythm from its own last 7 days of arrivals
      (`recorded_at`): late = p90 peak age + 2 × delivery cadence (floor
      1 h), down = 12 h or 2 × late. Measured: EC hourly 3.1 h, NOAA/Surrey
      1.7–2.4 h, ten-minute stations 1 h. Buoy + wind exports carry
      `status`, `late_after_minutes`, `down_after_minutes`; lightstations
      `status` from their existing cadence threshold; `stale` unchanged
      (documented on api.html). All three maps: late = amber, marker at
      0.6; down = red, 0.35, "Down: no report since …". `wind-data.js` had
      to pass `status` through its normalizer. The home cards still use
      their own flat 3 h / 12 h (`buoy-card.js`) — a follow-up if wanted.
- [x] **Forecast + storm-surge explanations**, 2026-09-23: one visible line
      above each storm-surge chart, the rest in `<details>`; on forecasts
      the "About these models" paragraph and the provenance block folded,
      the currents-and-channels caveat kept in the open.
- [x] **Lightstation page opened on an empty station** (found 2026-09-23):
      default Merry Island has sent only `N/A` since 2026-09-18, so both
      charts were blank boxes. Now defaults to Merry Island only while it
      reports; and the "No reports" row was stripped by DOMPurify (bare
      `<tr>`), now built with DOM calls.
- [ ] **UI overhaul.** Start with ONE home-page mockup (data first: a compact
      all-station summary up top, map and charts below, hero much smaller,
      explanations folded away), agree the direction, then roll it out.
      User: "I have to scroll a lot through some filler to get to the data."
- [ ] **`reporting_lag` for wind, lightstation and weather.** Only the buoy
      and tide exports call `record_publication`; the schema and CLAUDE.md
      assume all of them do.
- [ ] **Lightstation chart y-axis title reported clipped** (user,
      2026-09-23). Not reproduced at 360/414/600/660/768/1024/1280 px in
      Chromium or Firefox — get the width/browser or a screenshot first.
- [ ] **NOAA buoys "~6 h late"** (user, 2026-09-23). Not reproduced: no NOAA
      buoy was more than 2.6 h behind in the week's `reporting_lag` data. The
      map now draws a labelled dot, not 🌊, when a height has no direction,
      which covers the likely cause (spectral direction lagging the height).

- [x] **Ambleside webcam reported a stale frame as fresh** (user 2026-09-06;
      camera down since ~2026-08-27). The upstream URL kept returning 200 with
      the *same* last-good frame, so the fetch succeeded, `latest.json` was
      rewritten, and the staleness badge stayed green on a picture whose only
      tell was the timestamp burned into it. The machinery already existed and
      Ambleside was simply not opted in: `"dedupe": true` in
      `config/webcams.json`. Confirmed against the archive first — seven
      consecutive frames were byte-identical (same sha256, same 18,692 bytes),
      so both dedupe stages fire.

      The staleness half needed no change: a dedupe skip `sys.exit(0)`s before
      `latest.json` is written, so the timestamp stops advancing and
      `webcams-v4.js` flips the card stale at 3× the interval (60 min here).
      Documented in `config/webcams.example.json` via `_dedupe_note` so the
      next camera gets it by default. Tests: `TestDedupeFrozenCamera` in
      `tests/test_webcam_pipeline.py` — six cases pinning that an identical
      frame never advances `latest.json`, that a changed one still publishes,
      that the HEAD stage skips the transfer, that a stale `Content-Length`
      cannot pin us to an old frame, that annotation does not defeat the hash,
      and that the opt-out still republishes. *Done 2026-09-06.*

      Unrelated but noted: the endpoint served a frame at 21:48 UTC and 404ed
      five minutes later, twice. Worth watching before assuming the feed is
      merely frozen. Only the direct-image cams can use this; a stalled
      YouTube cam re-encodes every frame, so byte equality would be an
      accident there rather than a signal.

- [x] **Lightstation region toggle bars were oversized on mobile** (user
      2026-09-06). `.region-header` was `1.3rem` in `0.75rem 1.5rem` padding
      at every width, and the page's ≤768px block never touched it. Now
      `0.85rem` in `0.4rem 0.7rem` at ≤768px, which matches
      `.station-details-toggle`, the other "this is a control" affordance on
      the card. Bars go 41px → 33px, and "WEST COAST VANCOUVER ISLAND
      (5 stations)" stops wrapping to two lines at 390px (66px → 33px).
      Measured in both engines, light and dark. *Done 2026-09-06.*

- [x] **Map height capped against the viewport, sitewide** (user 2026-09-06).
      `#lightstation-map` was a flat 500px inline style with no mobile
      override, so on a short phone the map filled the screen and there was
      nowhere left to touch that was not the map. The cap lives on
      `.leaflet-container` in `stations-map-v4.css`, so it covers all three
      maps at once: `max-height: calc(100dvh - 10rem)` with a `vh` fallback
      line before it (`dvh` because mobile Safari's `100vh` is the *expanded*
      viewport, which is the case being defended against). The inline style
      moved into the page's `<style>` block while there.

      The popup cap had to move with it: `.leaflet-container` is
      `overflow: hidden`, so a popup taller than its map is clipped rather
      than scrolled. `.leaflet-popup-content-wrapper` is now
      `min(76vh, calc(100dvh - 11rem))` at ≤768px, which resolves to 464px on
      a 360×640 phone — exactly the Trial Island measurement the old 76vh was
      sized for, so nothing regressed. Verified at 360/390/768/1280 in
      Chromium and Firefox: map 480px inside a 640px viewport, popup 290px,
      no clipping at either edge. *Done 2026-09-06.*

- [x] **Per-card source link on the lightstation cards** (user 2026-09-06),
      in the buoy cards' exact format — "🔗 View Source Data" under a rule, on
      one shared CSS rule (`.buoy-source-link-wrap, .ls-card-source-wrap`)
      rather than a second copy of the declarations.

      The worry about FPCN61-only stations having no destination turned out
      to be unfounded: EC's Lightstation Reports page renders the FICN
      bulletins and carries **all 21 stations we show**, including the seven
      that reach us only in FPCN61 (verified 2026-09-06). So one URL serves
      every card. Keep its `?mapID=02&siteID=16200` query string.

      The provenance work was worth doing anyway and shipped as a details-panel
      row: `export_lightstation_json.py` now emits `bulletins` per station,
      measured from 30 days of `source_file` history. It has to be history —
      `report_time_str` names whichever bulletin arrived *first* for an
      observation and the merge deliberately leaves it alone, so for a
      dual-bulletin station it flips with arrival order. Current split: 9
      stations in both products, 5 SXCN-only, 7 FPCN61-only. *Done 2026-09-06.*

- [x] **Each marine text forecast links to its EC page** (user 2026-09-06):
      `renderSourceFooter()` in `forecasts.js` puts a source block below the
      extended forecast, where a reader lands when they have finished reading,
      in addition to the link beside the zone heading. Per-zone URL is
      `forecast_e.html?mapID=03&siteID=<siteID>` from the existing
      `ZONE_SITE_IDS` (strings, leading zeros).

      The second link is the combined bulletin the user found,
      `marine_bulletins_e.html?Bulletin=fqcn13.cwvr` — verified 2026-09-06 to
      carry all 23 BC Pacific marine areas, which makes it both a useful
      compare-all-zones destination and an honest fallback for any zone with
      no siteID. Its Atom feed stays unused: these products already arrive
      over the sr3/AMQP push, so the feed is redundant as transport and would
      only be worth it as a cross-check if the subscription dropped.
      *Done 2026-09-06.*

- [x] **Buoy-map marker text colours unified** (user 2026-09-06). The rule is
      now the user's: **colour by the quantity, not the station.** Wave heights
      blue (`--map-marker-text`), wind speeds near-black (new
      `--map-marker-wind-text`, `#1a1a1a`, fixed in both themes like its
      neighbours), and the arrow left as the only thing encoding station type.
      The wind line under a wave marker is near-black too, so it matches a
      standalone wind label instead of contrasting with it. Before this a wind
      station drew a blue "18kt" over a red arrow: three colours, no rule.
      Pinned by three tests in `tests/js/markers.test.mjs`; verified on the
      live map in both engines. *Done 2026-09-06.*

- [x] **`target="_blank"` was being stripped from every JS-rendered link**
      (found 2026-09-06 while adding the lightstation card link). `target` is
      not in DOMPurify's default allow-list, so every source link built as
      markup and passed through `setSafeHTML` arrived in the DOM without it
      and opened in the same tab — the buoy cards' "View Source Data", the
      forecast zone "View source", the new lightstation link — while their ↗
      and their aria-labels went on promising a new tab. Static HTML links
      were unaffected, which is why it went unnoticed.

      `sanitize-html.js` now passes `ADD_ATTR: ["target"]` and installs a
      DOMPurify `afterSanitizeAttributes` hook stamping
      `rel="noopener noreferrer"` on any link carrying `target` — allowing
      `target` back is only safe with the reverse-tabnabbing guard, and the
      hook is what stops it depending on each call site remembering. Guarded
      by `tests/playwright/external-links.spec.js`; no existing suite could
      have caught it (`test:js` never runs DOMPurify, the console spec only
      watches for errors). *Done 2026-09-06.*

- [x] **Webcam staleness thresholds are wall-clock, and daylight-aware** (user
      2026-09-06, straight after the Ambleside fix). STALE past **1 h**, DOWN
      past **3 h**, replacing "3x this cam's update interval" (which put the
      six cams at four different ages, none meaningful to someone looking at a
      picture) and a 24 h DOWN threshold (a full day of calling a dead camera
      merely stale). Nearest precedent on the site is the winds page, 2 h
      dimmed / 4 h offline; webcams are tighter because they update every
      10-20 min, so an hour is already three to six missed frames.

      The catch, and most of the work: **four of the six cams stop overnight by
      design**, so a flat 3 h DOWN would paint them red every night. Their age
      is now measured from whichever is later, the last frame or the moment the
      capture window opened, with the window read from the already-published
      `/data/sunlight_times.json` and margins mirroring `daylight_margin_minutes`
      (nearest published point per cam: Point Atkinson is ~7 km from Ambleside,
      White Rock ~9 km from Mud Bay — sunrise varies by minutes over that, well
      inside a 60-75 min margin). Off duty, a cam is not flagged at all; just
      after sunrise it can be STALE but not instantly DOWN. Unknown window
      (missing file, unlisted station) falls back to wall-clock age, which
      over-reports rather than hiding a dead camera.

      While in there, the twenty lines computing and rendering this existed in
      two drifted copies (initial render and metadata refresh) — now one
      `renderTimestamp()`. Guarded by `tests/playwright/webcam-staleness.spec.js`
      with both the clock and the frame age pinned. *Done 2026-09-06.*

      **Open, deliberately not changed:** `health_check.py` still uses
      `max(interval x 2, 2 h)` warning / **24 h** error for webcams, so the
      footer health badge can read fine for 21 hours after a card says DOWN.
      That 24 h is commented as being for the daylight-only cams, and the check
      has its own `_webcam_in_scope` daylight logic, so aligning it is a
      backend decision with alerting consequences rather than a copy of these
      numbers.

- [x] **Webcam registry drift closed** (user 2026-09-06: "may come back to
      bite us"). A camera was described in **five** places and they had drifted:
      `config/stations.json` ["webcams"] (tracked, drives the map pin),
      `config/webcams.json` (gitignored, drives the fetch),
      `site/assets/js/webcams-v4.js` (the page cards),
      `scripts/export/export_sunlight_times.py` (a hardcoded table), and
      `config/crontab.txt` (the only authority on cadence). What they disagreed
      about:
      - **Coordinates, by up to 24 km.** `boundarybay` still held the old
        Boundary Bay position in webcams.json and the sunlight export after the
        camera became the White Rock East Beach one; whiterock differed by
        400 m and coxbay by 3 km.
      - **Cadence.** Cron runs both Mud Bay cams every **15** minutes; stations
        .json and webcams-v4.js both said 10, so the page told readers a cadence
        the pipeline never had.
      - **Names**, on three of six cams.
      - The sunlight table's comment claimed it matched `fetch_webcam.py`, which
        stopped being true when that script moved to webcams.json — and it
        covered whiterock/boundarybay/coxbay, **almost exactly the wrong three**:
        the cams that need sunlight times are the daylight-gated ones
        (ambleside, coxbay, mudbay, mudbay_sw), and three of those were absent.

      Fixed: values reconciled to the tracked registry; the hardcoded sunlight
      table replaced by `lib.stations.get_all_webcams()` (new accessor), so all
      six cams now get sunlight times at their own positions. That surfaced one
      more collision — `whiterock` is both a camera id and a tide station key,
      and the tide station, added second, was silently overwriting the camera,
      so anything asking that file where the White Rock camera was got the tide
      gauge. Webcam keys are now namespaced `webcam_<id>`, which also let the
      overnight-staleness code drop its nearest-neighbour approximation and use
      each cam's true position.

      Guarded by `TestWebcamRegistryConsistency` — same roster everywhere, names
      and positions agreeing between the public and private registries, and
      **every stated interval checked against what cron actually runs**.
      Mutation-tested: all five real drift modes caught. *Done 2026-09-06.*

      The duplication itself is still there; the tests only hold the line. That
      is now the first item in the backlog below — **Collapse the webcam
      registries to one owner per field**. Also noted: nothing on the site reads
      the per-camera `sunlight.json` files any more; the set is frozen at the
      three already published rather than growing.

- [x] **Lightstation staleness measured per station, not flat** (user
      2026-09-06, prompted by "we now know the approx publish interval"). The
      threshold was a flat 12 h in four places and wrong at both ends:
      - **Too lax** for the 17 stations that report seven times a day, whose
        longest normal gap is 6 h — they got two whole missed cycles before
        anything was said. Now 9 h.
      - **Too strict** for Cape Mudge, Chatham Point and Pulteney Point, which
        report four times a day in daylight only and are normally silent for
        **15 h** overnight. They were flagged stale every night for behaving
        exactly as they always do — 10 such gaps each in the last 30 days. Now
        18 h. Same shape as the webcam overnight bug found the same day.

      `staleness_threshold_hours()` in `lib/lightstation_schedule.py` derives it
      from the station's own inferred cadence (longest normal gap + one 3 h
      reporting cycle), falling back to the flat 12 h for the four stations with
      too little history to infer from. The export emits `stale_after_hours`,
      and the card badge and both map popups read it instead of hardcoding
      ">12h". `health_check.py` uses the same rule so the footer badge and the
      page cannot disagree. Deleted `FRESHNESS_WINDOW = 21600` from the export —
      unused, and it documented a 6 h rule that never existed.

      **The age is now the prominent thing on the card** (user: "as long as we
      indicate hours old prominently, the staleness colour is a nicety"). It was
      0.85rem muted italic in parentheses at the end of the line — shown, but in
      the least prominent place on the card. Now it leads, at 1rem semibold,
      alert-coloured when the station is overdue: "**1h ago** · Sunday Sep 6,
      14:10". *Done 2026-09-06.*

- [x] **Station labels made readable and unique** (user 2026-09-06: "some of
      the stations use the same shorthand"). Measured first: **24 of 76
      stations rendered under a label shared with another**, and today's actual
      down-list read `La, Entrance, Estevan, Nootka, Langara, McInnes` — "La"
      being La Perouse Bank.

      Three separate causes, all in `footer.js`:
      - It kept its **own hardcoded map** keyed by display name, a second
        source of truth beside `config/stations.json`. Display names are not
        unique: Point Atkinson, Tsawwassen, Tofino and White Rock are each both
        a tide gauge and a wind station, and Entrance Island is both a wind
        station and a lightstation.
      - Its fallback took the **first word** of the name, so Cape Mudge, Cape
        Beale and Cape Scott all rendered as "Cape", and three Crescent
        stations all as "Crescent".
      - Three of its 24 entries had **drifted and matched nothing** (the cams
        had been renamed), so those fell through to the first-word fallback.

      Fixed by deleting the map and reading the registry's `short_name`, which
      already existed and was already consumed by the winds page's mobile
      column and the Windy push. `health_check.py` grew `display_name()` and
      now emits `short_name` in every stale/excluded entry and uses it in its
      log lines, which had the same ambiguity. Two registry values were
      actively wrong and are fixed on the winds page too: `whiterock_east` said
      "White Rock" (it is the East Beach station, and it claimed the tide
      gauge's name — the same silent-wrong-station shape as the Surrey naming
      trap), and `CYAZ` said "Tofino" for Tofino **Airport**, which is not the
      harbour. Thirteen more labels added for the places that needed
      disambiguating or were simply long.

      Guarded by `TestDisplayLabels` (`tests/test_stations.py`) and
      `TestWebcamDisplayLabels` (`tests/test_webcam_pipeline.py`), the second
      checking cams against the stations too since they share the one badge.
      **Exact equality turned out to be too weak a bar** — it passed
      "Entrance Is." beside "Entrance Island" — so the rule is prefix
      distinctness after folding case and punctuation. That immediately caught
      two pairs I had half-fixed by tagging only the tide side
      ("Tsawwassen" is a literal prefix of "Tsawwassen Tide"), which is why the
      tide labels are "Tsaw Tide" and "PtAtk Tide". Mutation-tested: 5 of 6
      deliberate regressions caught, the sixth being a genuinely readable pair.
      *Done 2026-09-06.*

      Noted, not changed: `config/stations.json` carries a `webcams` group
      whose names disagree with `config/webcams.json` ("White Rock Pier Webcam"
      vs "White Rock Pier Cam", "Ambleside Beach" vs "Ambleside (Hollyburn
      Sailing Club)"). `webcams.json` is canonical and is what health_check
      reads, so nothing is broken today, but that is a third naming source.

- [x] **Em-dashes removed from UI text sitewide** (user 2026-09-06). 30 in
      rendered HTML prose, 20 in JS-rendered strings, and 3 in
      `config/stations.json` `reporting_note` fields, replaced case by case
      with a colon, comma, semicolon, full stop or parentheses as the sentence
      wanted — not a blanket swap. Deliberately left alone: the `"—"`
      missing-value placeholder (not prose, and `stations-map.js` /
      `chart-utils-v4.js` compare against it), the `—` peak-value placeholders
      in `storm_surge.html`, en-dashes in ranges like `3–9 km`, and code
      comments, which are not UI text. *Done 2026-09-06.*

- [x] **Collapse the webcam registries to one owner per field** (*done
  2026-09-07*). A camera was described in four places; it is now described in
  two, one owner per field, and `lib/webcam/registry.py` (`load_webcams()`) is
  the only thing that reads either.
  - `config/stations.json` ["webcams"] — tracked and public: `name`,
    `short_name`, `location`, `lat`, `lon`, `source`,
    `update_frequency_minutes`, `stream_delay_minutes`, `daylight_only`,
    `daylight_margin_minutes`, `page_url`.
  - `config/webcams.json` — gitignored: URLs, referers, UA/From, crop,
    `archive_dir`/`website_dir`, `prefix`, `max_height`, `cron_offset`,
    `annotate_timestamp`, `dedupe`, `disabled_in_cron`. Nothing else.

  All four consumers converted in the planned order:
  `storage_metrics_to_mqtt.load_webcam_archives`,
  `health_check._load_webcam_config`, `webcams-v4.js` (its array now carries
  only `id`/`region`/`dataPath`/`attribution`/`conditions` and hydrates the
  rest from `/data/stations.json`), then `fetch_webcam.py` last — verified with
  a real capture on both a YouTube cam and a daylight-only direct-image cam.

  Three duplications beyond the plan went with it: `source_text` (the fetcher's
  richer attribution string became the registry's `source`, so `latest.json`
  and the map agree), the `daylightMargins` map hardcoded in `webcams-v4.js`,
  and the four per-camera `/data/<dir>/` URLs, now derived from one `dataPath`.
  `daylight_only`/`daylight_margin_minutes` were added to the export allowlist
  so the page reads the same capture policy `fetch_webcam.py` acts on.

  The three positional and naming assertions in
  `TestWebcamRegistryConsistency` are gone as structurally impossible; what
  replaces them is `test_the_private_file_holds_no_identity_fields` (belt and
  braces — the loader already ignores a stray copy with a warning) plus the
  roster and crontab-cadence checks, which stay useful because only the
  crontab actually sets a cadence. `TestWebcamRegistryLoader` covers the merge,
  the ignored stray, path resolution, an unregistered camera, and the
  fresh-clone case where the private file is absent.

- [x] **Audit the `reporting: false` lightstation flags** (*done 2026-09-07*).
  Two of the four were wrong, and the field is load-bearing:
  `_reporting_lightstations()` uses it to set the health check's denominator,
  so a wrong `false` quietly dropped a station that *is* reporting out of both
  halves of the footer fraction.
  - **Chatham Point** and **Green Island** both had recent observations
    (Chatham 10 rows, last 2026-09-04, via FPCN61; Green one on 2026-09-07 via
    SXCN23). The flag is removed from both. They are sparse, not silent, so
    they now carry `intermittent: true` instead: counted as stations, logged at
    `info`, and unable to turn the overall status red. Count went 72 → 74.
  - **Egg Island** and **Estevan Point** stay flagged; neither has produced a
    row. The notes now say only what the record supports — the database began
    2026-08-27 and raw bulletins are kept a day — not that the station never
    reports.

  `INTERMITTENT_STATIONS`, a hardcoded dict in `health_check.py`, is gone: it
  was a second answer to a question `config/stations.json` already had a field
  for. The registry's `intermittent` flag is the only one now, and the note
  shown beside a station comes from its own `reporting_note`.

- [x] **Subscribe to SXCN24** (*done 2026-09-07*).
  `config/sr3/bc_lightstation_obs.conf` skipped it on the stated grounds that
  "all its stations are already in FPCN61", which is not true of Egg Island —
  it appears in no product this site read, which is the whole reason it has
  never produced an observation — and Chatham Point reached us only sparsely.
  The accept line is added, the config deployed to
  `~/.config/sr3/subscribe/` and `sr3-bc-lightstation-obs` restarted.

  The parser gained the SXCN24 roster (Chatham, Scarlett, Pine, Egg, Cape
  Scott, Quatsino, Pulteney) and `SXCN_REGIONS["24"]`, without which the whole
  bulletin is skipped. **The abbreviations are inferred** from the pattern the
  other three bulletins follow, not observed — no SXCN24 has been read yet. So
  `parse_sxcn_station_line` no longer falls through to the raw bulletin name
  when an abbreviation is unmapped: it logs a warning and skips the row.
  Storing a phantom "EGG" would file the reading under a station the registry
  has never heard of and render it as a card that matches nothing, which is a
  worse failure than a missing row. **Check
  `logs/lightstation_parse.log` for "Unmapped SXCN station abbreviation" after
  the first SXCN24 arrives**, and fix the map if one is wrong.

- [ ] **14 days of buoy and wind history on the front end** (user 2026-09-07,
  explicitly not for today). The data is already there: `BUOY_RETENTION_DAYS`
  and `WIND_RETENTION_DAYS` are both 30, so SQLite holds a month. The
  frontend only ever sees 48 hours of it —
  `site/data/buoy_timeseries_48h.json` (1.2 MB) and
  `wind_timeseries_48hr.json` (0.4 MB), from `export_24hr_timeseries.py` and
  `export_wind_24hr_timeseries.py`.

  The obvious version is a bigger window on the same exports, and the obvious
  problem is payload size: 14 days is roughly 7x, so ~8 MB and ~3 MB, fetched
  on every page load by every visitor, on pages that currently render fast.
  Worth thinking about before implementing —
  - a separate longer-window file fetched only when a reader asks for the
    longer view, leaving the 48h file as the page's default load;
  - decimation for the older part of the window (a buoy reporting every 10
    minutes does not need 10-minute resolution at 12 days old);
  - both files are already served by `/api/v1`, so whatever shape this takes
    is a public contract — see `docs/PUBLIC_API.md` and the cache tiers.

  **User's steer (2026-09-07):** 14 days on an ordinary page load is too heavy;
  the shape to aim for is loading the extra history *on demand, for one
  station*. That points at per-station history files rather than one big
  bundle — a reader looking at Halibut Bank fetches Halibut Bank's 14 days and
  nothing else — which also caches better per-station and keeps the default
  page load exactly as it is today.

  Also decide what the pages actually do with it: the buoy cards and charts
  are built around a 48-hour axis, and 14 days on the same chart is a
  different picture, not a longer one.

- [ ] **North coast coverage** (added 2026-09-03, prompted by a mariner out of
  Kitimat who emailed about the McInnes Island position error). The
  lightstations are currently the *only* north-coast data this site carries,
  which is presumably why a north-coast reader was the one who spotted it.
  Measured gaps:
  - **Wind stations: 22 carried, none north of 50.5°N.** The northernmost is
    Sisters Islets at 49.49°N; Kitimat is 54°N. Candidates to add along
    Hecate Strait, Douglas Channel and Chatham Sound.
  - **Marine text forecasts: 6 areas, all Georgia Basin and south** (Haro
    Strait, Howe Sound, Johnstone Strait, Juan de Fuca, Strait of Georgia,
    WCVI South). Nothing north of Johnstone Strait — no Hecate Strait, Dixon
    Entrance, Douglas Channel, Principe Channel or Queen Charlotte Sound.
    The zone plumbing already handles nine zones and needs no parser change
    to add more (see `docs/project/` marine-forecast notes).
  - **Automated forecast points: 6, none north of 48.8°N** (La Perouse Bank).
    One or two RDWPS/HRDPS points on the north coast would pair with the
    lightstations already there. Mind the fetch footprint — see the
    `FETCH_DELAY` sizing note in `scripts/fetch/fetch_wave_forecast.py`.

  Worth keeping in touch with the correspondent: a working mariner in the area
  is a better source on which points matter than a map is.

- [x] **Lightstation regions come from the registry** (*done 2026-09-07*).
      `region` was stored per observation and the two bulletin products
      disagree — SXCN can only name the area its whole bulletin covers, so it
      filed the central-coast lights under Hecate Strait and Trial Island under
      Strait of Georgia, and six stations visibly swung between groups
      depending on which feed wrote last. Both exports now read
      `config/stations.json` via `get_lightstation_by_report_name()`; the
      column stays in the database as a record of what each bulletin claimed
      and is no longer published.

      The vocabularies were reconciled the other way too. The registry had 13
      values against the page's 5, so `region` was coarsened to six display
      groups ordered south to north (Strait of Georgia, Juan de Fuca Strait,
      West Coast Vancouver Island, **Johnstone & Queen Charlotte Strait** (new
      section), Central Coast, North Coast & Haida Gwaii). The finer geography
      that would have been lost — Milbanke Sound, Queen Charlotte Sound/Strait,
      Inside Passage — moved into each station's `location`, which is where a
      place belongs; `region` now means "which section of the page".

      The ordering itself was duplicated between `lightstation-page.js` and
      `lightstation-charts.js`, and both iterated it as a whitelist — the same
      shape that hid 11 of 23 stations from the dropdown before 2026-09-03.
      Both now call `orderRegions()` in `shared/station-meta.js`, which appends
      an unnamed region rather than dropping it. Triple Island proves it works:
      it reports on bulletins we parse but is not in the registry yet, so it
      renders in a trailing Hecate Strait section and the export logs a warning
      naming it. `TestLightstationRegions` keeps the registry vocabulary and
      the JS ordering in step in both directions.

- [ ] **A "View source" link on every rendered dataset** (added 2026-09-03).
  The forecasts page has had per-zone source links for a while and the
  lightstations page just got one; nothing else does. Two reasons to finish
  the job. First, it is the honest thing for a site that re-presents someone
  else's bulletins: a reader can always reach the original. Second, and the
  reason to prioritise it — **it crowdsources the accuracy vetting.** The
  McInnes Island position error sat there until a mariner happened to notice;
  a reader who can put our rendering beside the source in one click is a
  reader who can catch the next one. Pages needing links: buoys/index, winds,
  tides, storm surge, webcams, and the wave-forecast points. Some sources have
  no per-station public page (NOAA NDBC does, DFO tides does, the RDWPS model
  output does not) — link the closest honest thing and say what it is, rather
  than linking a landing page and implying more precision than exists. Note
  the EC lightstation URL needs its `?mapID=02&siteID=16200` query string or
  the page renders an "incorrect web address" banner over the content.

- [ ] **Store the extra lightstation fields we already receive** (rewritten
  2026-09-07; was "Parse the FICN31/32/33 bulletins", which rested on two
  wrong premises).

  **Correction first.** There is no FICN subscription: no accept line, no `FI`
  subtopic in `config/sr3/bc_lightstation_obs.conf`, no
  `data/lightstation_ficn/`, and not one FICN file ever received. There briefly
  was one — `3fc5dc3` added it on 2026-04-11 at 19:13 UTC, it received zero
  messages because `FI` was the wrong topic prefix, and `9bf6498` replaced it
  with `SX` + `FP` at 21:57 the same evening. The doc section survived those two
  hours and forty minutes and described the subscription as live-but-pending
  ever since.

  **And the fields are already arriving.** The reason to want FICN was
  visibility, cloud cover and temperature. SXCN carries visibility, cloud,
  pressure and sea-water temperature today, on bulletins we subscribe to and
  parse — `parse_lightstation.py` reads past them because
  `lightstation_observation` has no columns to put them in. So this is a schema
  change and a parser extension, not a new feed:

  - add `visibility`, `cloud_cover`, `pressure_hpa`, `sea_water_temp_c`,
    `weather` to `lightstation_observation`;
  - extend the SXCN line parser to fill them (the FPCN61 branch leaves them
    null — the two products genuinely differ, and the merge at insert has to
    keep whichever product supplied a field);
  - decide what the page shows. Visibility is the one a mariner asks for.

  **Triple Island** is the other loose end and is independent of the above: it
  has been reporting on SXCN23 the whole time but is absent from
  `config/stations.json`, so it renders in a trailing region section and both
  lightstation exports log a warning naming it. Registering it is small — take
  the position from the CCG *List of Lights* PDF, per the coord audit.

  Only reopen FICN if something establishes it carries a field SXCN does not.

- [ ] **Accuracy audit: forecasts and lightstations pages** (added 2026-09-03).
  Both pages re-present someone else's bulletins, and the 2026-09-03 session
  found four separate ways this one had drifted from its source (a dropdown
  that silently hid 11 of 23 stations, a window too short for the slowest
  stations, an empty details panel, a station missing from the registry). Walk
  each rendered field back to the upstream bulletin and confirm it says what
  the source says. Overlaps with the queued end-to-end feed audit — do them
  together.

- [ ] **Infer the human forecasters' issue cadence** (added 2026-09-03), the
  same way lightstation schedules now work. Mariners plan around when the next
  EC bulletin lands, and the page currently says nothing about it. The pieces
  are in place: `issued_utc` is already parsed per location into
  `marine_forecast.json`, and `lib/lightstation_schedule.py` is generic over
  "timestamps in, slots out" — it would need renaming and a small
  generalization rather than a rewrite. The missing half is history: unlike
  lightstation observations there is no table of past issue times, so a
  lightweight record of them has to accumulate first. Note the cadence differs
  from the lightstations' — text bulletins are issued a few times a day with
  amendments in between, so amended reissues must not be mistaken for
  scheduled slots.

- [ ] **Decide a license for the Dataset structured data** (deferred by the
  user 2026-08-27 — "I don't know much about Dataset license"). The JSON-LD
  `Dataset` block in `site/api.html` omits `license`, which Google lists as
  recommended (not required) for Dataset Search. Valid and indexable without
  it, so there is no deadline.

  The reason this is not a quick pick: **most of the data is not yours to
  license.** Observations belong to ECCC, NOAA NDBC and DFO under their own
  terms — `api-catalog.json`'s `terms.attribution` already says attribution
  to the original producers is expected and sometimes required. What you can
  license is the aggregation: the schema, derived fields, unit conversions.
  A blanket CC-BY on the whole feed would overclaim.

  Likely shape if picked up: license the *compilation* (CC-BY-4.0 is the
  usual choice) while keeping the existing per-producer attribution note,
  and mirror whatever is chosen into `terms` in
  `site/assets/api-catalog.json` and the fair-use section of `api.html` so
  there is one story in three places.

- [ ] **Add the GitHub Sponsors button to the API page** (user 2026-08-27):
  Ko-fi is live (`ko-fi.com/keeldude`) in the support block in
  `site/api.html`. The Sponsors button is commented out right beside it,
  pending GitHub Sponsors actually being enabled on the account — a dead
  Sponsors link has the same broken-page problem the placeholder did. Once
  enabled, uncomment it (likely `https://github.com/sponsors/Keelando`, but
  confirm) and consider adding `.github/FUNDING.yml` so the repo grows a
  Sponsor button too. API page only; user did not want it in the shared
  footer where casual visitors would see it.

- [ ] **Verification panel: say when the model side is missing** (user
      2026-08-26, for 2026-08-27). Neah Bay and La Perouse Bank opt out of the
      HRDPS wind fetch (`STATIONS` in `scripts/fetch/fetch_wave_forecast.py`),
      so on the Wind view their forecast series is empty while the observed
      series is full — 0 forecast points against ~251 observed at Neah Bay.
      That reads as *the model predicted dead calm*, when in fact we never
      asked. The forward wind chart and the wind table columns have the same
      problem.

      This is the trap the verification code already reasons about one level
      down: `renderVerificationMode()` in `site/assets/js/wave-forecast.js`
      leaves gusts out precisely because "a near-empty line beside a
      continuous observed one reads as the model predicting calm rather than
      the model not being asked." Same failure, now visible at station scope.

      Two directions, and the choice is genuinely open:
      - **Note it.** Read `models` from the forecast payload (already there,
        and it names one model rather than two at these stations) and either
        hide the Wind toggle or show an explicit "wind not forecast at this
        station" note. Zero fetch cost. Downside: the toggle appears and
        disappears as the reader switches stations.
      - **Fetch wind everywhere, for completeness.** Flip both stations to
        `{"wind": True}`. Costs **+1,584 requests/day** and ~5 min more
        runtime per run. Budget check before doing this: the current footprint
        is 6,310 ECCC req/day (7.3% of MSC's 86,400 guidance), so the total is
        not the binding constraint — but runtime is, both as publication lag
        and against the 2 h stale-lock threshold, and the burst-rate ceiling
        of 1/`FETCH_DELAY` is what the guidance actually cares about. Re-read
        the sizing comment above `FETCH_DELAY` before changing anything.

      A note is worth adding regardless of which way the fetch decision goes —
      any station whose archive is younger than the 48 h window has the same
      thin-forecast-line problem while it fills.

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

- [x] **Let users opt in to warning zones** (user 2026-08-20): **DONE
      2026-08-23 — plan and outcome in `docs/project/WARNING_ZONE_OPT_IN.md`.**
      That doc superseded both this entry and "Subscribable warning banners"
      below, which disagreed with each other about the default. Shipped as
      planned: default stays the Strait of Georgia pair, *plus* storm warnings
      from any carried zone always banner (`clearsSeverityFloor` in
      `shared/warning-zones.js` — the picker cannot switch it off); the picker
      lives on the forecasts page and nowhere else, discovered via an inline
      "alert me about this zone" toggle in the zone card; zero zones selected
      is allowed silently. Stored under `warning_banner_zones` as a JSON array,
      so `[]` stays distinguishable from "never chosen".
      Follow-up the same day, from using it: static picker summary, a collapse
      control, session-scoped zone memory, a capped banner for the
      many-warnings case, and an unfiltered jump-to-warning strip on the
      forecasts page (which supersedes the deferred "quiet count of suppressed
      warnings" and most of the all-alerts page). Two bugs fell out — the storm
      banner's `scale()` pulse overflowed the viewport horizontally, and the
      gale chip failed AA contrast.

- [ ] **`aria-label` on bare chart `<div>`s** (found 2026-08-23): ECharts'
      `aria.enabled`, turned on centrally in `chart-utils-v4.js`, writes an
      `aria-label` onto the container div, which needs `role="img"` to be a
      valid attribute there. axe reports `aria-prohibited-attr`; it affects
      every chart on every page. One central fix in the `setOption` wrapper's
      neighbourhood, but it wants verification across all chart pages rather
      than a drive-by change.

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

- [x] **Subscribable warning banners** (user 2026-08-20): **superseded — see
      "Let users opt in to warning zones" above, shipped 2026-08-23.** Kept for
      the reasoning that produced the decision. Let the reader choose
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

      **Resolved 2026-08-23:** the "default all zones on" instinct recorded
      above lost to the cry-wolf argument in the same paragraph — widening the
      default trades a miss for a dismiss-on-reflex habit, and the habit is
      worse. Severity carries the safety case instead: storm warnings banner
      from every carried zone regardless of the reader's selection. Full plan
      in `docs/project/WARNING_ZONE_OPT_IN.md`; shipped the same day.

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
      - **Changed 2026-08-23: Forgejo mirrors onward to a private Codeberg
        repo on push.** This *helps* the case for going private rather than
        complicating it — there is already an offsite, off-LAN, private home
        for the code, so GitHub is no longer carrying the offsite-backup
        argument on its own. GitHub remains the only public destination, so
        flipping it private would genuinely make the repo private.
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
      **Marker offsets (user 2026-08-31):** some pairs sit close enough that
      no zoom separates them — White Rock East Beach's wind marker and the
      webcam pin there are indistinct at *every* zoom level, because they
      are effectively the same coordinate. Zoom-gating cannot fix that; it
      needs an offset (nudge one marker along a fixed bearing, or spiderfy
      the pair on click). The wind-label collision test added the same day
      (`refreshWindLabels`, same file) already projects every marker to
      pixels and finds overlapping boxes — that is the input an offset pass
      would need, so build on it rather than starting again.

      **Make the offset dynamic per zoom (user 2026-09-06).** A fixed nudge in
      map coordinates is the wrong unit: two stations 300 m apart need a large
      angular separation at zoom 8 and none at zoom 14, so a constant offset
      either fails to separate them when zoomed out or visibly lies about their
      positions when zoomed in. The offset has to be computed in *pixels* and
      recomputed on `zoomend`, which is the same space `refreshWindLabels`
      already works in — project both markers, and only if their boxes overlap
      at the current zoom, push them apart along the line joining them, by just
      enough to clear. At a zoom where they no longer collide the offset falls
      to zero on its own and every marker sits on its true position.

      Two things to get right. The displacement must be signposted, not silent:
      a reader deciding where to launch a boat should not be given a marker
      that is 200 m from where the station is, so a leader line back to the
      true point (or restoring the true position on hover/open) is part of the
      feature, not a nicety. And the popup anchor has to follow the moved
      marker or the popup will point at empty water. Applies to all three maps
      (`stations-map.js`, `winds-map.js`, `lightstation-map.js`), which each
      build their own markers but share `shared/markers.js`.
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
