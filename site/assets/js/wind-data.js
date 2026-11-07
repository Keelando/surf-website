/**
 * Wind Data Store (ES module)
 * Fetches all wind-related JSON data once and normalizes buoy data
 * to match wind station format. Imported by wind-stations.js and winds-map.js.
 *
 * fetchWithTimeout comes from chart-utils-v4.js (classic script, loaded
 * before the module entry points).
 */

import { isWaveStation } from "./shared/station-meta.js";

const store = {
  stations: null, // stations.json
  latestWind: null, // latest_wind.json (wind stations only)
  latestBuoy: null, // latest_buoy_v2.json (raw, for non-wind fields)
  latestAll: null, // merged & normalized: all stations with wind data
  timeseries: null, // merged & normalized: wind + buoy timeseries
  _readyResolve: null,
};

store.ready = new Promise((resolve) => {
  store._readyResolve = resolve;
});

export const windData = store;

/**
 * Normalize a single buoy's latest data to wind station field names.
 * Also resolves the best observation_time for wind fields.
 */
function normalizeBuoyLatest(buoy, stationMeta) {
  const windDir = buoy.wind_direction_deg ?? buoy.wind_direction ?? null;
  if (buoy.wind_speed == null && windDir == null) return null;

  // Use field-specific timestamp for wind if available
  let windObsTime = buoy.observation_time;
  if (buoy.field_times && (buoy.field_times.wind_speed || buoy.field_times.wind_direction)) {
    windObsTime = buoy.field_times.wind_speed || buoy.field_times.wind_direction;
  }

  const isWindType = !isWaveStation(stationMeta);

  return {
    name: buoy.name,
    wind_speed_kt: buoy.wind_speed,
    wind_gust_kt: buoy.wind_gust ?? null,
    wind_direction_deg: windDir,
    wind_direction_cardinal: buoy.wind_direction_cardinal ?? null,
    air_temp_c: buoy.air_temp ?? null,
    pressure_hpa: buoy.pressure ?? null,
    observation_time: windObsTime,
    stale: buoy.stale ?? false,
    status: buoy.status ?? null, // ok | late | down (shared/staleness.js reportStatus)
    late_after_minutes: buoy.late_after_minutes ?? null,
    down_after_minutes: buoy.down_after_minutes ?? null,
    humidity_percent: buoy.humidity ?? null,
    _sourceType: isWindType ? "land" : "buoy",
    _isWindType: isWindType,
  };
}

/**
 * Normalize a single wind station's latest data to a consistent shape.
 */
function normalizeWindLatest(station) {
  return {
    name: station.name,
    wind_speed_kt: station.wind_speed_kt ?? null,
    wind_gust_kt: station.wind_gust_kt ?? null,
    wind_direction_deg: station.wind_direction ?? null,
    wind_direction_cardinal: station.wind_direction_cardinal ?? null,
    air_temp_c: station.air_temp_c ?? null,
    pressure_hpa: station.pressure_hpa ?? null,
    observation_time: station.observation_time,
    stale: station.stale ?? false,
    status: station.status ?? null,
    late_after_minutes: station.late_after_minutes ?? null,
    down_after_minutes: station.down_after_minutes ?? null,
    humidity_percent: station.humidity_percent ?? null,
    out_of_service: station.out_of_service ?? null, // {since, reason}; overrides status
    _sourceType: "land",
    _isWindType: true,
  };
}

/**
 * Unwrap buoy timeseries format: { data: [{time,value}] } → [{time,value}]
 */
function normalizeTimeseries(ts) {
  if (!ts) return {};
  const out = {};
  for (const key of Object.keys(ts)) {
    const val = ts[key];
    if (Array.isArray(val)) {
      out[key] = val; // already flat (wind station format)
    } else if (val && Array.isArray(val.data)) {
      out[key] = val.data; // unwrap buoy format
    }
  }
  return out;
}

// The 48-hour timeseries are ~1.8 MB together, the latest files ~10 KB each.
// A page left open refreshes every five minutes (wind-stations.js); fetching
// everything each time cost ~20 MB an hour per open tab. So the latest files
// come every time, the timeseries only once they are this old, and the
// registry once per page load.
const TIMESERIES_REFRESH_MS = 30 * 60 * 1000;
let timeseriesLoadedAt = 0;
let rawTimeseries = null;

/**
 * Fetch the data, normalize it into the store, and resolve the ready promise
 * (the first time). Throws on failure; the callers decide what that means.
 */
async function fetchAll() {
  {
    const wantTimeseries =
      !rawTimeseries || Date.now() - timeseriesLoadedAt >= TIMESERIES_REFRESH_MS;
    const [stations, windLatest, buoyLatest, freshTs] = await Promise.all([
      store.stations ?? fetchWithTimeout(`/data/stations.json?t=${Date.now()}`),
      fetchWithTimeout(`/data/latest_wind.json?t=${Date.now()}`),
      fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`),
      wantTimeseries
        ? Promise.all([
            fetchWithTimeout(`/data/wind_timeseries_48hr.json?t=${Date.now()}`),
            fetchWithTimeout(`/data/buoy_timeseries_48h.json?t=${Date.now()}`),
          ])
        : null,
    ]);
    if (freshTs) {
      rawTimeseries = freshTs;
      timeseriesLoadedAt = Date.now();
    }
    // Consumers compare this to skip re-rendering what the timeseries feed.
    store.timeseriesLoadedAt = timeseriesLoadedAt;
    const [windTs, buoyTs] = rawTimeseries;

    store.stations = stations;
    store.latestWind = windLatest;
    store.latestBuoy = buoyLatest;

    // Build merged latest: all stations with wind data, normalized field names
    const latestAll = {};
    // Wind stations
    Object.entries(windLatest)
      .filter(([key]) => key !== "_meta")
      .forEach(([id, station]) => {
        latestAll[id] = normalizeWindLatest(station);
      });
    // Buoys with wind data
    Object.entries(buoyLatest)
      .filter(([key]) => key !== "_meta")
      .forEach(([id, buoy]) => {
        const meta = stations.buoys?.[id];
        const normalized = normalizeBuoyLatest(buoy, meta);
        if (normalized) {
          latestAll[id] = normalized;
        }
      });
    latestAll._meta = windLatest._meta || null;
    store.latestAll = latestAll;

    // Build merged timeseries: wind stations + buoys, normalized format
    const timeseries = {};
    // Wind stations (already flat format)
    Object.entries(windTs)
      .filter(([key]) => key !== "_meta")
      .forEach(([id, station]) => {
        timeseries[id] = {
          name: station.name,
          timeseries: normalizeTimeseries(station.timeseries),
        };
      });
    // Buoys with wind timeseries
    Object.entries(buoyTs)
      .filter(([key]) => key !== "_meta")
      .forEach(([id, buoy]) => {
        if (buoy.timeseries?.wind_speed?.data && buoy.timeseries.wind_speed.data.length > 0) {
          const meta = stations.buoys?.[id];
          timeseries[id] = {
            name: isWaveStation(meta) ? buoy.name + " \u{1F30A}" : buoy.name,
            timeseries: normalizeTimeseries(buoy.timeseries),
          };
        }
      });
    timeseries._meta = windTs._meta || null;
    store.timeseries = timeseries;
    store.loadedAt = Date.now();
  }
}

let reloading = null;

/**
 * Fetch everything again, for a page left open or resumed from a phone's
 * home screen (the home page refreshes on the same rhythm, main.js). On
 * success the store is replaced and `winds:data-updated` fires on document;
 * on failure the store keeps what it had and the next call tries again.
 * One reload at a time.
 */
store.reload = function reload() {
  reloading ??= fetchAll()
    .then(() => document.dispatchEvent(new CustomEvent("winds:data-updated")))
    .catch((error) => console.warn("WindDataStore: refresh failed, keeping the last data", error))
    .finally(() => {
      reloading = null;
    });
  return reloading;
};

store.loadedAt = 0;

// Module scripts are deferred and the fetches don't touch the DOM —
// start loading immediately. Resolve either way so consumers don't hang.
fetchAll()
  .catch((error) => console.error("WindDataStore: failed to load data", error))
  .finally(() => store._readyResolve(store));
