import assert from "node:assert/strict";
import { test } from "node:test";
import {
  isNoaaStation,
  isPileStation,
  isSurreyStation,
  reportsSubHourly,
  sourceUrl,
  usesDominantPeriod,
  usesSwellDisplay,
  waveHeightPrecision,
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
