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
| med | 300 s | timeseries, tides, water level, forecast, weather, verification |
| hourly | 600 s | lightstations |
| slow | 1800 s | wave forecast, storm surge |
| stations | 3600 s | `/stations`, catalog |
| daily | 21600 s | `/sunlight` |

If you change a cron cadence, revisit the matching tier.

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

## Rate limiting (not yet configured)

Caddy has no built-in rate limiting without a plugin, so this belongs at the
edge. With the cache rule live, ordinary polling never reaches the origin, so
a rate limit is about capping abuse of *cache-missing* paths — note that a
bogus `/api/v1/wave-forecast/<random>` 404s at the origin on every request,
because the allowlist guard runs before `file_server` rather than being
cached.

Free tier allows one rate-limiting rule. Suggested starting point:
expression `starts_with(http.request.uri.path, "/api/")`, characteristic IP,
~100 requests per 10 seconds, action Block or Managed Challenge.

Do **not** use a JavaScript challenge — it breaks `curl` and every
server-side consumer, the same failure mode as Bot Fight Mode.

## Testing

`tests/playwright/console.spec.js` includes `/api.html`. Endpoint smoke test:

```bash
for e in stations buoys/latest wind/latest tides/latest wave-forecast sunlight; do
  printf "%-22s %s\n" "$e" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/api/v1/$e)"
done
# and the guard:
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8090/api/v1/data/system_health.json  # want 404
```
