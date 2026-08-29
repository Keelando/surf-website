# Public developer API (`/api/v1`)

Free, unauthenticated, read-only JSON API over the same exports the site
itself reads. Human docs live at `site/api.html` (published at
<https://halibutbank.ca/api.html>); the machine-readable catalog is
`site/assets/api-catalog.json`, served at `/api/v1/`.

## Why an alias layer instead of renaming files

`/api/v1/*` is a set of Caddy rewrites onto `site/data/*.json`. Nothing is
copied or duplicated — one file on disk, two URLs.

That indirection is the whole point:

- **File names are archaeology.** `latest_buoy_v2.json` carries a schema
  version from a migration years ago; `buoy_timeseries_48h.json` and
  `wind_timeseries_48hr.json` disagree on `h` vs `hr`. None of that should
  become a public contract.
- **`/data/` is not a contract, `/api/v1/` is.** The site's own pages keep
  fetching `/data/`, and those paths stay free to change. External consumers
  are pointed at `/api/v1/`, which does not move.
- **Renaming the files would break the frontend** and every cache-busted
  reference, for no gain.

## Where the config lives

The Caddyfile (`/etc/caddy/Caddyfile`) is **not tracked in this repo** — it
also configures two unrelated sites whose filesystem paths should not be
published to a public GitHub repo. The API block is reproduced in
`docs/caddy-api-block.txt` for reference; the live file is authoritative.

Backups: `~/caddy_backups/Caddyfile.<timestamp>`.

To change the API surface: edit `/etc/caddy/Caddyfile`, then

```bash
caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
sudo systemctl reload caddy
```

## The allowlist guard is load-bearing

`handle_path` strips the `/api/v1` prefix and hands what remains to
`file_server`. Without a guard, that makes **every file under `site/` reachable
through the API prefix** — `/api/v1/data/system_health.json` served operational
telemetry, and `/api/v1/data/latest_buoy_v2.json` let callers bind to the
internal paths the alias layer exists to hide.

The `route { @disallowed ... }` block fixes this by 404-ing anything not on an
explicit allowlist, and it is inside a `route` specifically so it executes
**before** the rewrites (Caddy otherwise sorts directives by its own order,
which would run the rewrites first and defeat the check).

**When adding an endpoint, add it to the allowlist regexp as well as adding a
`rewrite`.** A rewrite alone will 404.

### Everything that must run before the rewrites belongs in that `route`

The same ordering trap bit the method guard, which sat directly inside
`handle_path` rather than inside the `route`. Caddy sorts `respond` *after*
`route` in its own directive order, so both guards were dead code: until
2026-08-29 `POST`, `PUT`, `DELETE`, `PATCH` and `TRACE` all returned **200
with the full payload**, and `OPTIONS` returned 200 plus the whole body
instead of an empty 204.

Nothing was writable — these are static files behind `file_server` — so the
exposure was a false 405 contract and a wasted full payload on every CORS
preflight, not a data-integrity problem. The guards now sit at the top of the
`route`, ahead of the allowlist.

The rule to carry forward: **inside `handle_path`, anything that must run
before the rewrites goes inside the `route` block.** Only the `header`
directives that set cache tiers are safe outside it, because they act on the
response rather than on dispatch.

`GET`, `HEAD` and `OPTIONS` are the whole method set. `HEAD` stays because
RFC 9110 requires it wherever `GET` is supported and monitors rely on it;
`OPTIONS` stays because a 405 there breaks CORS preflight for any consumer
sending a custom header.

## Deliberately excluded

`site/data/system_health.json` is **not** aliased. It is operational telemetry
— disk capacity, mount state, stale-station diagnostics — not marine data. It
remains readable at `/data/system_health.json` because the site's own footer
badge fetches it.

## Cache tiers

Lifetimes are derived from each feed's export cadence in `config/crontab.txt`,
not guessed. Both `Cache-Control` (browsers) and `Cloudflare-CDN-Cache-Control`
(edge) are emitted.

| Tier | max-age | Endpoints |
|------|---------|-----------|
| fast | 60 s | `/buoys/latest` (pipeline runs every 3 min) |
| wind | 120 s | `/wind/latest` (export every 5 min) |
| med | 300 s | timeseries, tides, water level, forecast, weather, wave verification, `/storm-surge/observed` |
| hourly | 600 s | lightstations |
| slow | 1800 s | wave forecast, `/storm-surge`, `/storm-surge/verification` |
| stations | 3600 s | `/stations`, catalog |
| daily | 21600 s | `/sunlight` |

If you change a cron cadence, revisit the matching tier.

**Tier matchers must be disjoint.** When two `header` matchers both match a
path, the later one wins, and the tiers are not naturally exclusive: the fast
10-minute feeds `/wave-forecast/verification/*` and `/storm-surge/observed`
sit *underneath* prefixes the slow tier claims. Until 2026-08-29 both were
silently served with `max-age=1800` — six times staler than documented for
verification, and a 30-minute cache on a 10-minute feed for observed surge.

`@slow` therefore carries an explicit `not path /wave-forecast/verification/*`
and enumerates `/storm-surge` and `/storm-surge/verification` instead of using
`/storm-surge/*`. `tests/test_public_api.py::TestCacheTiers` pins this.

## Cloudflare: the cache only works if a Cache Rule exists

Cloudflare's default cache-eligible set is **extension-based and does not
include `.json`**. Origin `Cache-Control` alone will be honoured by browsers
but Cloudflare will still return `cf-cache-status: DYNAMIC` and pass every
request through to this box.

**DONE 2026-08-27.** A Cache Rule named "Public API Caching" is deployed on
the zone: *URI Path starts with `/api/`* → Eligible for cache, Edge TTL from
the origin's cache-control, Browser TTL respect origin. Confirmed working —
`/buoys/latest` shows a clean 60-second sawtooth (age climbs to ~55, resets
to 0), so Cloudflare honours the per-feed TTLs rather than substituting a
default, and the origin sees one request per minute no matter how many
clients poll. `/data/` and the HTML pages stay `DYNAMIC`, so the site's own
pages are unaffected.

Verify with:

```bash
curl -sI https://halibutbank.ca/api/v1/buoys/latest | grep -i cf-cache-status
# want: HIT (or MISS then HIT on a second call), not DYNAMIC
```

### `Vary` must stay `Accept-Encoding` only

Cloudflare will not cache a response whose `Vary` header lists anything
beyond `Accept-Encoding`. The API block originally sent
`Vary: Accept-Encoding, Origin`, which would have made the cache rule look
correctly configured while every request still returned `BYPASS`.

Nothing here varies by origin — `Access-Control-Allow-Origin` is a static
`*`, not reflected per caller — so the `Origin` token bought nothing and cost
the entire edge cache. **If you ever add per-origin CORS, you lose edge
caching**; keep the wildcard.

**Do not enable Bot Fight Mode** on this zone — it challenges non-browser
clients and would break every `curl` and server-side consumer of the API.

## Rate limiting

Caddy has no built-in rate limiting without a plugin, so this lives at the
edge. With the cache rule live, ordinary polling never reaches the origin, so
the rate limit is about capping abuse of *cache-missing* paths — someone
walking `/api/v1/wave-forecast/<random>` with a fresh random string each
time. Each distinct URL is a distinct cache key, so those miss at the edge by
definition and reach the origin every time. **The rate limit is the only
defence against that shape; caching cannot help, because there is nothing to
re-serve.** See "Unknown paths are already cacheable" below.

**DONE 2026-08-27.** One Cloudflare rate-limiting rule (the free-tier
allowance) is deployed and Active:

| Setting | Value |
|---------|-------|
| Expression | `starts_with(http.request.uri.path, "/api/")` |
| Characteristic | IP |
| Period | 10 s |
| Requests | 100 |
| Action | Block |
| Mitigation timeout | 10 s |

Verified by burst: 150 parallel requests to `/api/v1/buoys/latest` returned
99 × `200` then 51 × `429` (the 100th was an earlier smoke-test request), and
a normal request succeeded again once the 10-second timeout elapsed.

Two properties worth remembering:

- **Counting happens at the edge, so cache hits count too.** The threshold is
  against *all* API requests from an IP, not just origin-bound ones. 100 per
  10 s is far above any legitimate consumer, but it is not a measure of
  origin load.
- **Block, not a challenge.** A JavaScript or Managed Challenge breaks `curl`
  and every server-side consumer — the same failure mode as Bot Fight Mode
  (above). Keep the action as Block.

### Unknown paths are already cacheable

Both classes of 404 already carry `Cache-Control`, so *repeated* requests for
the *same* unknown path are absorbed at the edge and never reach this box.
Verified 2026-08-27: three requests for one bogus path returned
`cf-cache-status: HIT` with `age` climbing.

| Request | Handled by | Status | Body | `max-age` |
|---------|-----------|--------|------|-----------|
| `/api/v1/nope-xyz` (fails the allowlist) | `handle @disallowed` | 404 | JSON error pointing at the catalog | 300 |
| `/api/v1/wave-forecast/<unknown>` (fails the allowlist) | `handle @disallowed` | 404 | JSON error pointing at the catalog | 300 |

**Both rows now behave the same.** They did not until 2026-08-29: the
allowlist used to admit `wave-forecast/[A-Za-z0-9_-]+` as a wildcard, so an
unknown *station* passed the guard, fell through the rewrites to
`file_server`, and got a bare bodiless 404 with the 1800 s `@slow` tier — no
explanation for a consumer who simply typo'd a station id.

The fix was to **enumerate the six station ids in the allowlist** rather than
add a `handle_errors` block. `handle_errors` turned out not to be usable here
anyway (it is not a plain handler directive, so Caddy rejects it both inside
`route` and inside `handle_path`; only a site-level block works, which would
have changed the main site's error pages too). Enumerating is better on three
counts: the JSON body comes for free from the existing guard, an unknown
station never reaches the disk, and random-path probes are answered by
`respond` instead of a `file_server` stat.

The cost is that the station list now lives in the Caddyfile as well as in
`STATIONS` (`scripts/fetch/fetch_wave_forecast.py`) and the catalog.
`tests/test_public_api.py::TestWaveStationIds` fails if the three drift, so
adding a wave-forecast station means updating all three and running pytest.

Note that neither row helps against *randomised* paths, per the rate-limiting
section above — each distinct URL is a distinct cache key.

## The `/stations` payload is filtered, not copied

`site/data/stations.json` is public twice over: directly at
`/data/stations.json` and as `/api/v1/stations`. Until 2026-08-29
`export_stations_json.py` copied `config/stations.json` wholesale
(`for key in data: output[key] = data[key]`), which is exactly the pattern the
"two public surfaces" note in `CLAUDE.md` warns against — **every field ever
added to the registry was published on the next hourly export.**

It now filters through `PUBLIC_STATION_FIELDS`, an explicit per-section
allowlist, so a new registry field stays private until it is named there
deliberately. Withheld today:

- `channels`, `fallback_channels`, `flowworks_site_id` — internal FlowWorks
  sensor plumbing, meaningless without our credentials
- `url` — the upstream endpoint we poll. `source_url`, the human-facing
  station page, *is* published; `url` stays private so consumers bind to our
  contract rather than to our upstreams
- `_metadata.notes.flowworks_api` — documented the upstream auth scheme and
  carried a `credentials` field. Those Surrey credentials are genuinely
  public (Surrey publishes them), so this was never an incident, but operator
  documentation has no place in a marine-data payload

`tests/test_public_api.py::TestStationsExportIsFiltered` fails if any of them
reappear.

## Testing

`tests/test_public_api.py` is the regression suite. It pins the three
hand-maintained descriptions of the API to each other — the Caddy allowlist
and rewrites (via the `docs/caddy-api-block.txt` reference copy), the catalog,
and the `api.html` table — so a rewrite without an allowlist entry, an
endpoint documented in one place but not the other, or a station id list that
has drifted all fail at commit time rather than in production. It also covers
the method guard's position, tier disjointness, and the stations field filter.

**Keep `docs/caddy-api-block.txt` in sync when you edit the live Caddyfile**,
or the tests are checking a stale copy.

`tests/playwright/console.spec.js` includes `/api.html`. Endpoint smoke test:

```bash
for e in stations buoys/latest wind/latest tides/latest wave-forecast sunlight; do
  printf "%-22s %s\n" "$e" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/api/v1/$e)"
done
# and the guard:
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8090/api/v1/data/system_health.json  # want 404
```
