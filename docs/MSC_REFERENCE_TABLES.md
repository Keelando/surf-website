# MSC Reference Tables

**Snapshot fetched 2026-08-20.** Lookup tables MSC publishes that map the
opaque codes in our feeds to human names — for answering "what is `m0000009`?"
without downloading a file to find out.

> **This is a reference, not an authority.** It was accurate on the date
> above and MSC changes these lists without telling us. If you are looking
> for a station you believe exists, **its absence here proves nothing** —
> refetch from the canonical URL before concluding it isn't there.
> `config/stations.json` is our station registry; the sr3 configs are the
> subscription source of truth. Neither is derived from this file.

Snapshots live in `docs/reference/`.

## The tables

| Table | Snapshot | Canonical URL |
|-------|----------|---------------|
| Marine forecast regions | `reference/marine_region_list_en.csv` | <https://collaboration.cmc.ec.gc.ca/cmc/cmos/public_doc/msc-data/marine-weather/marine_region_list_en.csv> |
| Land SWOB stations | `reference/swob-xml_station_list.csv` | <https://dd.meteo.gc.ca/today/observations/doc/swob-xml_station_list.csv> |
| Marine SWOB stations | `reference/swob-marine-stations.json` | <https://api.weather.gc.ca/collections/swob-marine-stations/items?&f=json&limit=10000> |
| Partner SWOB stations | *(not vendored — we use no partner stations)* | <https://api.weather.gc.ca/collections/swob-partner-stations/items?&f=json&limit=10000> |

The two CSVs are byte-exact as served. The marine JSON is pretty-printed so a
refetch diffs line-by-line instead of as one 19 KB line.

## Gotchas

- **`marine_region_list_en.csv` is ISO-8859, not UTF-8.** `grep` sees it as
  binary and silently matches *nothing* — no error, no output. Read it as
  latin-1 in Python, or use `grep -a`. This one will waste your afternoon.
- **A marine file is a *bulletin*, not a zone.** One `m#######` can carry
  several forecast zones: `m0000028` carries both Strait of Georgia zones,
  `m0000009` carries all three Juan de Fuca sub-zones. The parser merges
  multi-zone files correctly; do not assume one file means one zone.
- **The marine list's `wmo_id` is stale for older buoys.** EC padded buoy IDs
  from the old 5-digit WMO form to the current 7-digit form a couple of years
  ago, and the station list was never fully back-filled. So Halibut Bank
  appears as `wmo_id: 46146` even though the live SWOB-ML path — and
  `config/sr3/bc_buoys.conf` — uses `4600146`. The two newest buoys,
  `4600303` and `4600304`, were registered after the change and carry the
  correct 7-digit form (their Title Case names give away the different
  vintage; every older entry is UPPER CASE).
  **7-digit is the real format. Match buoys by name or `msc_id`, never by
  `wmo_id`, and never "correct" a 7-digit id to match this list.**
- Land stations key on `IATA_ID` (`CWGT`), which *is* what our registry uses.
  All 15 EC wind stations resolved cleanly on the fetch date. A few have an
  empty `WMO_ID` (`CVTF`, `CZBB`, `CYAZ`, `CWZO`) — `MSC_ID` is always
  populated.

## Pacific marine regions (17 as of the fetch date)

| Code | Region |
|------|--------|
| `m0000009` | Juan de Fuca Strait |
| `m0000010` | Johnstone Strait |
| `m0000028` | Strait of Georgia |
| `m0000043` | West Coast Vancouver Island North |
| `m0000063` | Queen Charlotte Sound |
| `m0000064` | Haro Strait |
| `m0000065` | West Coast Vancouver Island South |
| `m0000079` | West Coast Haida Gwaii |
| `m0000087` | Explorer |
| `m0000098` | Dixon Entrance West |
| `m0000102` | Howe Sound |
| `m0000106` | Hecate Strait |
| `m0000112` | Queen Charlotte Strait |
| `m0000124` | Dixon Entrance East |
| `m0000140` | Central Coast from McInnes Island to Pine Island |
| `m0000152` | Douglas Channel |
| `m0000164` | Bowie |

The Georgia Basin — the six bulletins covering the nine zones a Salish Sea
boat cares about — is `m0000028`, `m0000009`, `m0000064`, `m0000102`,
`m0000010`, `m0000065`. See `TODO.md` for which of these we carry.

## Our EC buoys in the marine list

Registry ids are the current 7-digit form and are correct. The `wmo_id` column
shows what the list happens to say — stale 5-digit for the older three.

| Registry id | Name | Marine list `wmo_id` | `msc_id` |
|-------------|------|----------------------|----------|
| `4600146` | Halibut Bank | 46146 *(stale)* | 9100552 |
| `4600131` | Sentry Shoal | 46131 *(stale)* | 9100624 |
| `4600206` | La Perouse Bank | 46206 *(stale)* | 9100580 |
| `4600303` | Southern Georgia Strait | 4600303 | 9102000 |
| `4600304` | English Bay | 4600304 | 9102001 |

EC wind stations resolve by `IATA_ID` in the land CSV — read that file directly
rather than duplicating 15 rows here.
