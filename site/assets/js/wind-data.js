/**
 * Wind Data Store
 * Fetches all wind-related JSON data once and normalizes buoy data
 * to match wind station format. Shared by wind-stations.js and winds-map.js.
 */

(function () {
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

    const isWindType =
      stationMeta?.type === "wind_monitoring_station" ||
      stationMeta?.type === "weather_station" ||
      stationMeta?.type === "c_man_station" ||
      stationMeta?.type === "land_station";

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

  /**
   * Fetch all data, normalize, and resolve the ready promise.
   */
  async function load() {
    try {
      const [stations, windLatest, buoyLatest, windTs, buoyTs] = await Promise.all([
        fetchWithTimeout(`/data/stations.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/latest_wind.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/wind_timeseries_48hr.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/buoy_timeseries_48h.json?t=${Date.now()}`),
      ]);

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
            const isWindType =
              meta?.type === "wind_monitoring_station" ||
              meta?.type === "weather_station" ||
              meta?.type === "c_man_station" ||
              meta?.type === "land_station";
            timeseries[id] = {
              name: isWindType ? buoy.name : buoy.name + " \u{1F30A}",
              timeseries: normalizeTimeseries(buoy.timeseries),
            };
          }
        });
      timeseries._meta = windTs._meta || null;
      store.timeseries = timeseries;

      store._readyResolve(store);
    } catch (error) {
      console.error("WindDataStore: failed to load data", error);
      store._readyResolve(store); // resolve anyway so consumers don't hang
    }
  }

  // Expose on window
  window.windData = store;

  // Start loading when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", load);
  } else {
    load();
  }
})();
