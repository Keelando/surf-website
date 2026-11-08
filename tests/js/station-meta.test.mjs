import assert from "node:assert/strict";
import { test } from "node:test";
import {
  airTempIsProxy,
  airTempLabel,
  caveatCovers,
  caveatFootnote,
  caveatNameMark,
  caveatStar,
  isNoaaStation,
  isPileStation,
  isSurreyStation,
  isWaveStation,
  stationTypeLabel,
  pickWavePeriod,
  reportsSubHourly,
  sourceUrl,
  usesDominantPeriod,
  usesSwellDisplay,
  waveHeightField,
  waveHeightPrecision,
  wavePeriodFields,
} from "../../site/assets/js/shared/station-meta.js";

// Trimmed real entries from stations.json (buoys section)
const ecBuoy = {
  source: "Environment Canada",
  type: "wave_buoy",
  update_frequency_minutes: 60,
  source_url: "https://weather.gc.ca/marine/...",
};
const neahBay = {
  source: "NOAA NDBC",
  type: "wave_buoy",
  update_frequency_minutes: 60,
  wave_display: "swell",
  source_url: "https://www.ndbc.noaa.gov/station_page.php?station=46087",
};
const newDungeness = {
  source: "NOAA NDBC",
  type: "wave_buoy",
  update_frequency_minutes: 60,
  source_url: "https://www.ndbc.noaa.gov/station_page.php?station=46088",
};
const crescent = {
  source: "Surrey FlowWorks",
  type: "pile_mounted_wave_station",
  update_frequency_minutes: 10,
  source_url: "https://developers.flowworks.com/",
};

test("isWaveStation splits wave stations from the wind-section types", () => {
  assert.equal(isWaveStation(ecBuoy), true);
  assert.equal(isWaveStation(crescent), true);
  for (const type of [
    "weather_station",
    "wind_monitoring_station",
    "c_man_station",
    "land_station",
    "land_based_wind_station",
  ]) {
    assert.equal(isWaveStation({ type }), false, type);
  }
  // Unknown station defaults to wave, matching the map's fallback class
  assert.equal(isWaveStation(undefined), true);
  assert.equal(isWaveStation({}), true);
});

test("stationTypeLabel names every registry type, generic fallback for the rest", () => {
  assert.equal(stationTypeLabel(ecBuoy), "Wave Buoy");
  assert.equal(stationTypeLabel(crescent), "Pile-Mounted Wave Station");
  assert.equal(stationTypeLabel({ type: "c_man_station" }), "C-MAN Station");
  assert.equal(stationTypeLabel({ type: "land_station" }), "Land Station");
  assert.equal(stationTypeLabel({ type: "land_based_wind_station" }), "Land-Based Wind Station");
  assert.equal(stationTypeLabel({ type: "weather_station" }), "Weather Station");
  assert.equal(stationTypeLabel({ type: "some_future_station" }), "Weather Station");
});

test("source predicates split EC / NOAA / Surrey", () => {
  assert.equal(isNoaaStation(neahBay), true);
  assert.equal(isNoaaStation(ecBuoy), false);
  assert.equal(isSurreyStation(crescent), true);
  assert.equal(isSurreyStation(neahBay), false);
});

test("pile stations get 2-decimal wave heights, buoys 1", () => {
  assert.equal(isPileStation(crescent), true);
  assert.equal(isPileStation(ecBuoy), false);
  assert.equal(waveHeightPrecision(crescent), 2);
  assert.equal(waveHeightPrecision(neahBay), 1);
});

test("swell display comes from the wave_display config field", () => {
  assert.equal(usesSwellDisplay(neahBay), true);
  assert.equal(usesSwellDisplay(newDungeness), false);
});

test("dominant period: NOAA stations except swell-display ones", () => {
  assert.equal(usesDominantPeriod(newDungeness), true);
  assert.equal(usesDominantPeriod(neahBay), false);
  assert.equal(usesDominantPeriod(ecBuoy), false);
});

test("sub-hourly reporting drives the history hourly filter", () => {
  assert.equal(reportsSubHourly(crescent), true);
  assert.equal(reportsSubHourly(ecBuoy), false);
});

test("sourceUrl passes through, null when absent", () => {
  assert.equal(sourceUrl(crescent), "https://developers.flowworks.com/");
  assert.equal(sourceUrl({}), null);
});

test("unknown station (missing meta) gets EC-style defaults", () => {
  assert.equal(isNoaaStation(undefined), false);
  assert.equal(isSurreyStation(undefined), false);
  assert.equal(usesSwellDisplay(undefined), false);
  assert.equal(usesDominantPeriod(undefined), false);
  assert.equal(waveHeightPrecision(undefined), 1);
  assert.equal(reportsSubHourly(undefined), false);
  assert.equal(sourceUrl(undefined), null);
});

// --- displayed-field priorities (shared by the card line and history table) ---

test("waveHeightField follows the station's display convention", () => {
  assert.equal(waveHeightField(ecBuoy), "wave_height_sig");
  assert.equal(waveHeightField(neahBay), "swell_height");
  assert.equal(waveHeightField(undefined), "wave_height_sig");
});

test("wavePeriodFields tags only non-significant periods", () => {
  // Swell display and plain significant periods need no tag
  assert.deepEqual(wavePeriodFields(neahBay), [{ field: "swell_period", tag: null }]);
  assert.deepEqual(wavePeriodFields(ecBuoy).map((f) => f.tag), [null, null, "avg", "peak"]);
});

test("wavePeriodFields maps NOAA's DPD onto wave_period_sig", () => {
  assert.deepEqual(wavePeriodFields(newDungeness), [
    { field: "wave_period_sig", tag: "dominant" },
  ]);
});

test("pickWavePeriod walks the priority chain", () => {
  assert.deepEqual(pickWavePeriod({ wave_period_sig: 5 }, ecBuoy), { value: 5, tag: null });
  // sig absent under both SWOB names -> avg, tagged
  assert.deepEqual(pickWavePeriod({ wave_period_avg: 7 }, ecBuoy), { value: 7, tag: "avg" });
  assert.deepEqual(pickWavePeriod({ wave_period_peak: 9 }, ecBuoy), { value: 9, tag: "peak" });
  // sig_basic is still a significant period, so it stays untagged
  assert.deepEqual(pickWavePeriod({ wave_period_sig_basic: 6 }, ecBuoy), {
    value: 6,
    tag: null,
  });
});

test("pickWavePeriod reports nothing when the station has no period", () => {
  assert.deepEqual(pickWavePeriod({}, ecBuoy), { value: null, tag: null });
  // A NOAA station only reads its one field, never the EC fallbacks
  assert.deepEqual(pickWavePeriod({ wave_period_avg: 7 }, newDungeness), {
    value: null,
    tag: null,
  });
});

// --- caveats: the asterisk rule ---------------------------------------------

const crescentCaveat = {
  caveat: "Air temperature reads high on sunny afternoons (unshielded sensor).",
  caveat_fields: ["air_temp"],
};
const colebrook = {
  caveat: "The temperature is the sensor enclosure's, not the open air.",
  air_temp_label: "Enclosure temp",
};
const hilltop = { caveat: "Hilltop site: pressure reads ~8 hPa low.", caveat_fields: ["pressure"] };

test("caveatCovers follows caveat_fields, and an air_temp_label counts as air_temp", () => {
  assert.equal(caveatCovers(crescentCaveat, "air_temp"), true);
  assert.equal(caveatCovers(crescentCaveat, "pressure"), false);
  assert.equal(caveatCovers(hilltop, "pressure"), true);
  assert.equal(caveatCovers(colebrook, "air_temp"), true, "label without caveat_fields");
  assert.equal(caveatCovers(undefined, "air_temp"), false);
  assert.equal(caveatCovers({ caveat: "A note." }, "air_temp"), false, "a caveat alone stars no value");
});

test("caveatStar is the asterisk or nothing", () => {
  assert.match(caveatStar(hilltop, "pressure"), /class="caveat-star"/);
  assert.equal(caveatStar(hilltop, "air_temp"), "");
});

test("airTempLabel / airTempIsProxy", () => {
  assert.equal(airTempLabel(colebrook), "Enclosure temp");
  assert.equal(airTempLabel(crescentCaveat), "Air");
  assert.equal(airTempIsProxy(colebrook), true);
  assert.equal(airTempIsProxy(crescentCaveat), false);
});

test("caveatFootnote names the reading once, and only for covered readings", () => {
  assert.equal(caveatFootnote(crescentCaveat, "air_temp", "sea_temp"), `* ${crescentCaveat.caveat}`);
  assert.equal(caveatFootnote(colebrook, "air_temp"), `* Enclosure temp: ${colebrook.caveat}`);
  assert.equal(caveatFootnote(hilltop, "air_temp", "pressure"), `* Pressure: ${hilltop.caveat}`);
  assert.equal(caveatFootnote(hilltop, "air_temp"), null);
});

test("caveatNameMark escapes the caveat into its title and speaks a note", () => {
  const mark = caveatNameMark({ caveat: 'Reads "high" <in sun>' });
  assert.match(mark, /title="Reads &quot;high&quot; &lt;in sun&gt;"/);
  assert.match(mark, /has a note on its readings/);
  assert.equal(caveatNameMark({}), "");
});
