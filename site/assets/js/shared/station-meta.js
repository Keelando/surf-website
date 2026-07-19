/**
 * Station-metadata predicates (ES module).
 *
 * Pure helpers over a station entry from stations.json (`buoys` section).
 * These replace the inline station-ID checks that were scattered through
 * main.js / stations-map.js under drifting names (isBoundaryBay / isSurrey /
 * isCrescentStation were all the same set) — see
 * docs/project/BUOY_CARD_REFACTOR.md.
 *
 * `config/stations.json` is the single source of truth; it ships to the
 * frontend verbatim as /data/stations.json. Every helper tolerates a
 * missing meta object (unknown station → EC-style defaults).
 */

/** NOAA NDBC station (badge, spectral details, dominant-period wording). */
export function isNoaaStation(meta) {
  return meta?.source === "NOAA NDBC";
}

/**
 * Wave-measuring station (wave buoy or pile-mounted sensor). Everything
 * else — the `wind` section's weather/land/C-MAN types — is a wind station.
 * This split decides which export feeds a station: wave stations publish to
 * latest_buoy_v2.json, wind stations to latest_wind.json (disjoint ID sets).
 * Unknown/missing type defaults to wave, matching the map's fallback class.
 */
export function isWaveStation(meta) {
  const type = meta?.type;
  return type == null || type === "wave_buoy" || type === "pile_mounted_wave_station";
}

const WIND_STATION_LABELS = {
  wind_monitoring_station: "Wind Monitoring Station",
  c_man_station: "C-MAN Station",
  land_station: "Land Station",
  land_based_wind_station: "Land-Based Wind Station",
};

/** Human-readable station type ("Wave Buoy", "C-MAN Station", …). */
export function stationTypeLabel(meta) {
  if (isWaveStation(meta)) {
    return isPileStation(meta) ? "Pile-Mounted Wave Station" : "Wave Buoy";
  }
  return WIND_STATION_LABELS[meta.type] ?? "Weather Station";
}

/** Surrey FlowWorks station (badge/border styling). */
export function isSurreyStation(meta) {
  return meta?.source === "Surrey FlowWorks";
}

/**
 * Pile-mounted wave station (Crescent Beach/Channel): near-shore sensors
 * reporting small waves — drives 2-decimal heights and 1-decimal temps.
 */
export function isPileStation(meta) {
  return meta?.type === "pile_mounted_wave_station";
}

/**
 * Station whose cards/history show swell instead of combined sig wave
 * (Neah Bay: open-ocean site where swell is the representative metric).
 * Driven by the `wave_display: "swell"` config field.
 */
export function usesSwellDisplay(meta) {
  return meta?.wave_display === "swell";
}

/**
 * NOAA stations report a "dominant" period (DPD) rather than a significant
 * one — cards tag the period and add a footnote. Swell-display stations
 * show swell period instead, so they're excluded.
 */
export function usesDominantPeriod(meta) {
  return isNoaaStation(meta) && !usesSwellDisplay(meta);
}

/** Decimal places for wave heights (pile stations resolve centimetres). */
export function waveHeightPrecision(meta) {
  return isPileStation(meta) ? 2 : 1;
}

/**
 * Reports more often than hourly — history tables filter these to
 * on-the-hour rows so 12h fits the table.
 */
export function reportsSubHourly(meta) {
  return (meta?.update_frequency_minutes ?? 60) < 60;
}

/** Upstream source page URL, or null when the station has none. */
export function sourceUrl(meta) {
  return meta?.source_url ?? null;
}

/**
 * Field holding the wave height this station displays. Swell-display sites
 * headline swell; everyone else headlines significant height.
 */
export function waveHeightField(meta) {
  return usesSwellDisplay(meta) ? "swell_height" : "wave_height_sig";
}

/**
 * Wave-period fields in priority order, each with the tag the UI shows when
 * that field is the one used ("dominant"/"avg"/"peak"; null = no tag, i.e.
 * the value is the expected significant/swell period).
 *
 * One list drives both readers: the card's compact line reads it against a
 * snapshot object, the history table against timeseries arrays. Previously
 * each re-derived the same priority separately.
 *
 * EC buoys publish significant period under two SWOB names, hence the pair;
 * avg/peak are fallbacks for non-EC stations (e.g. Crescent's radar sensor).
 */
export function wavePeriodFields(meta) {
  if (usesSwellDisplay(meta)) {
    return [{ field: "swell_period", tag: null }];
  }
  if (usesDominantPeriod(meta)) {
    // NOAA stores DPD in wave_period_sig.
    return [{ field: "wave_period_sig", tag: "dominant" }];
  }
  return [
    { field: "wave_period_sig", tag: null },
    { field: "wave_period_sig_basic", tag: null },
    { field: "wave_period_avg", tag: "avg" },
    { field: "wave_period_peak", tag: "peak" },
  ];
}

/**
 * First non-null wave period on a snapshot object, with its tag.
 * Returns {value: null, tag: null} when the station reports none.
 */
export function pickWavePeriod(b, meta) {
  for (const { field, tag } of wavePeriodFields(meta)) {
    const value = b?.[field];
    if (value != null) return { value, tag };
  }
  return { value: null, tag: null };
}
