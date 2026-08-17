# Webcam ML Training Dataset — Planning

**Status:** Brainstorm / planning phase
**Started:** 2026-05-25

---

## Concept

Build a paired image + marine-conditions dataset suitable for training a model that can infer wave height (and possibly wind/tide regime) from webcam imagery alone. Primary signal is **image → wave height**; wind and tide are candidate auxiliary features that may improve learning or be useful as multi-task targets.

The dataset is the prerequisite. The model is downstream — we don't need to commit to architecture or framing (regression vs. classification, single-cam vs. multi-cam, etc.) before we start capturing pairs.

## Why this is coming up now

Discovered during a retention review:

- Tide DB is 105M; turns out `surrey_geodetic_data` has no retention (~290k rows, ~161 days). Separate issue, tracked elsewhere.
- The retention conversation prompted the question: should we be *keeping* more data for ML purposes?

Conclusion: operational DBs should stay lean (short retention serves the live site). ML training data belongs in a **separate, append-only archive** that grows linearly and isn't touched by purges.

## Data Sources

Webcams (image side — already archived to `/mnt/storage/<cam>_cam/` as originals, audited 2026-05-25):

| Cam | Images | Size | Earliest |
|---|---|---|---|
| whiterock | 23,683 | 3.6 G | 2025-12-01 |
| boundarybay | 7,180 | 962 M | 2025-12-03 |
| coxbay | 10,258 | 771 M | 2025-12-10 |
| mudbay | 3,710 | 692 M | 2026-01-13 |
| ambleside | 4,969 | 95 M | 2026-01-18 |

Total ~50k images, ~6 GB. `/mnt/storage` is 220 G with 198 G free — storage is not a constraint for years.

Note the naming trap: the `boundarybay` key is the **White Rock East Beach**
YouTube livestream, not the boundarybayweather.com cam below. Don't reuse the
key.

### Pending permissions

More streams are wanted, both to populate the site and to widen the training
set — more viewpoints and sea states is the main lever on a wave height model,
more than any amount of extra history from the cams already listed. Acquiring
one is a permissions problem, not a technical one.

| Cam | Status (2026-08-17) |
|---|---|
| Boundary Bay (`boundarybayweather.com`) | Permission requested; awaiting a response. Nothing is captured from this cam. |

Two distinct permissions, and they are worth asking for separately — an
operator may grant one and refuse the other:

1. **Retain stills privately for training.** All this project needs. Images
   never leave the server; model output is wave heights, not their content.
2. **Display/mirror on the site.** Bigger ask, and unnecessary here.

Aggregators are not a shortcut for (1). A cam listed on Windy can be *displayed*
via their Webcams API, but the terms forbid storing or archiving the images, so
that route can never feed this dataset — and an operator's blessing doesn't
amend the agreement with Windy. If permission comes through, pull the archive
from the operator's own server.

Marine/met conditions (label side — candidates for pairing):

- **Wave height** — primary target. Nearest buoys: Halibut Bank (`4600146`), English Bay (`4600304`) for the WR/BB cams; Tofino/Neah Bay buoys for Cox Bay.
- **Wind** — speed, gust, direction. Nearest wind stations vary per cam.
- **Tide** — water level at the closest tide station. Big visual effect on shoreline framing for WR/BB.
- **Time-of-day / solar position** — derivable from timestamp + cam location. Useful for the model to disambiguate lighting from sea state.
- **Optional later**: pressure tendency, marine forecast warnings active at the time, swell vs wind wave components (NOAA spectral cams only).

## Proposed Storage Approach

A new SQLite DB — `~/.local/share/webcam_training.sqlite` — with one row per webcam capture:

```
CREATE TABLE training_pair (
    capture_time INTEGER NOT NULL,
    webcam_id TEXT NOT NULL,
    image_path TEXT NOT NULL,             -- relative path to archived image
    -- Primary target
    wave_height_m REAL,
    wave_period_s REAL,
    wave_direction_deg REAL,
    wave_source_buoy TEXT,                -- which buoy was used
    wave_age_seconds INTEGER,             -- staleness of the reading at capture
    -- Wind
    wind_speed_kmh REAL,
    wind_gust_kmh REAL,
    wind_direction_deg REAL,
    wind_source_station TEXT,
    wind_age_seconds INTEGER,
    -- Tide
    tide_height_m REAL,
    tide_source_station TEXT,
    tide_age_seconds INTEGER,
    -- Solar
    sun_elevation_deg REAL,
    sun_azimuth_deg REAL,
    PRIMARY KEY (webcam_id, capture_time)
);
```

Append-only. No retention. Image archive lives in a parallel directory (not `site/data/`, which gets overwritten).

### Cadence (decided 2026-05-25): row per image

One row per webcam capture, not one row per buoy/wind/tide update. Even when consecutive images share the same buoy reading, each row records its own timestamp, tide level, solar position, and `*_age_seconds` per source — so rows are rarely true duplicates, and training-time queries are a simple `SELECT WHERE webcam_id=...` without window joins. ~50 MB/year of labels across all cams; storage is not a constraint.

### Capture every tick vs. threshold-only

Capture **every webcam tick**, not "only when waves exceed threshold." Reasons:

1. A model needs calm/baseline frames as negative examples; thresholding biases the dataset.
2. The whole point of inferring wave height from images is to handle the full range — training only on storms produces a model that can't tell calm from moderate.
3. Storage cost is negligible: ~3 cams × 6/hr × 24h × 365 = ~158k rows/year. Even at ~200 bytes/row → ~30 MB/year for the SQLite side.

Images are the bulk of the storage, not the labels.

### Image storage

Largely settled — originals are already being archived to `/mnt/storage/<cam>_cam/` by the live pipeline (`scripts/fetch/fetch_webcam.py` reads `archive_dir` from `config/webcams.json`). At current growth rates (~6 GB / 6 months across 5 cams) and 198 G free, originals can stay indefinitely. Downsampling for a model can happen at training time, not at archive time.

## Open Questions

- **Which buoy/wind station maps to which cam?** Needs to be picked deliberately, not "nearest by lat/lon" — Cox Bay's wave regime is Pacific-open, not Salish.
- **What freshness window is acceptable?** The 2h operational freshness is too loose for training labels. A 30-min cap with `*_age_seconds` recorded so we can filter later is probably right.
- **Backfill from existing buoy/wind/tide DBs against archived webcam images** — feasible: user confirms a sizeable archive of past webcam images already exists on disk (was being held for a third party). Worth auditing the date range and per-cam coverage before designing the pair-generation script, and tweaking the live pipeline if anything is currently being overwritten rather than archived.
- **Is wave height alone the right target, or also period and direction?** Period is harder to infer from a still image; direction may be visible at some cams. Worth capturing all three and deciding at training time.
- **Single-frame vs. short-clip input?** A 5-frame burst captures wave motion. Out of scope for now but would change capture cadence if pursued.

## Recommended v1: Cox Bay only

Start with a single cam rather than building the harness for all five at once. **Cox Bay** is the right one:

- **Oceanic, west-facing, full Pacific exposure** — sees the widest dynamic range of wave heights of any of our cams (calm summer days through 4 m+ winter swells). Salish cams compress most of the year into a narrow 0.5–1.5 m band, which gives a less useful label distribution.
- **Single regime** — one offshore buoy, one tide station, no Salish/Pacific cross-regime decisions to make. Cleanest possible starting point.
- **Existing archive is solid** — ~10k images going back to 2025-12-10, covers a full winter storm season.

If the Cox Bay pipeline produces a usable model, Salish cams become a straightforward extension. If it doesn't, we haven't built 5× the wiring.

### Station mapping for Cox Bay (decided 2026-05-25)

- **Wave buoy:** La Pérouse Bank (`4600206`, EC). Audit shows our DB only has it from 2026-05-06 onward — either recent ingest or extended outage; needs investigating before backfill. **Fallback** for backfill of the pre-2026-05-06 image window: Neah Bay (`46087`, NOAA), tagged in the `wave_source_buoy` field so mixed-source rows are filterable later.
- **Wind:** Tofino Airport (`CYAZ`), 49.0821°N -125.772°W, elev 24.4 m, ~7 km from Cox Bay. **Wired into the live wind pipeline 2026-05-26**: subtopic added to `config/sr3/bc_wind_stations.conf`, registry entry added to `config/stations.json` under `wind`, sr3 service restarted. First observation parsed at 00:00 UTC 2026-05-26 (AWOS schema handled by existing `scripts/parse/wind_to_sqlite.py` — same format as CZBB). `latest_wind.json` and the wind page pick it up automatically via `get_all_wind()`.
- **Tide:** Tofino (`08615` / `5cebf1e23d0f4a073c4bc07c`). Already in our station registry; no work needed beyond pointing at it.
- **Solar:** Compute from cam lat/lon + timestamp at row-write time. No external source.

## Next Steps (when ready to start)

1. Confirm Cox Bay station mappings (pick wave buoy, wind source, tide station; document).
2. Stub `webcam_training.sqlite` + `populate_training_pair_coxbay.py` that scans `/mnt/storage/coxbay_cam/` and joins against the operational buoy/wind/tide DBs by timestamp (one-shot backfill).
3. Wire a small post-fetch hook so new Cox Bay captures get a row automatically.
4. Run for a month of new captures + the full backfill, audit data quality (null rates, age distributions, label coverage across wave-height bins).
5. Generalize to Salish cams once the Cox Bay version is proven.

## Related

- Retention review that surfaced this (2026-05-25 conversation).
- `lib/webcam/storage.py` — current image saving logic.
- `config/webcams.json` — cam list and cadences.
