import assert from "node:assert/strict";
import { test } from "node:test";
import {
  formatForecastTimestamp,
  formatFullTimestamp,
  formatMonthDayTime,
  formatNumericDayTime,
  formatTimeHM,
  formatTimeWithDate,
  getAgeString,
} from "../../site/assets/js/shared/format-time.js";

// 2026-07-14T21:30:00Z = 14:30 PDT (summer); 2026-01-15T20:05:00Z = 12:05 PST (winter)
const SUMMER = "2026-07-14T21:30:00Z";
const WINTER = "2026-01-15T20:05:00Z";

test("formatTimeHM renders Pacific 24h time", () => {
  assert.equal(formatTimeHM(SUMMER), "14:30");
  assert.equal(formatTimeHM(WINTER), "12:05");
});

test("formatTimeHM accepts Date objects", () => {
  assert.equal(formatTimeHM(new Date(SUMMER)), "14:30");
});

test("formatTimeWithDate appends numeric month/day", () => {
  assert.equal(formatTimeWithDate(SUMMER), "14:30 7/14");
  assert.equal(formatTimeWithDate(WINTER), "12:05 1/15");
});

test("formatMonthDayTime renders short month with time", () => {
  assert.equal(formatMonthDayTime(SUMMER), "Jul 14, 14:30");
  assert.equal(formatMonthDayTime(WINTER), "Jan 15, 12:05");
});

test("formatNumericDayTime strips the comma", () => {
  assert.equal(formatNumericDayTime(SUMMER), "7/14 14:30");
  assert.equal(formatNumericDayTime(WINTER), "1/15 12:05");
});

test("formatFullTimestamp includes year and zone abbreviation", () => {
  assert.equal(formatFullTimestamp(SUMMER), "Jul 14, 2026, 14:30 PDT");
  assert.equal(formatFullTimestamp(WINTER), "Jan 15, 2026, 12:05 PST");
});

test("formatForecastTimestamp uses 12h clock, pinned to Pacific", () => {
  assert.equal(formatForecastTimestamp(SUMMER), "Tue, Jul 14, 2026, 02:30 PM PDT");
  assert.equal(formatForecastTimestamp(WINTER), "Thu, Jan 15, 2026, 12:05 PM PST");
});

test("date rolls over at the Pacific midnight boundary, not UTC", () => {
  // 2026-07-15T06:59:00Z is still 23:59 July 14 in Vancouver
  assert.equal(formatTimeWithDate("2026-07-15T06:59:00Z"), "23:59 7/14");
  assert.equal(formatTimeWithDate("2026-07-15T07:00:00Z"), "00:00 7/15");
});

test("getAgeString buckets ages", () => {
  const now = new Date("2026-07-14T12:00:00Z");
  const ago = (mins) => new Date(now - mins * 60000);
  assert.equal(getAgeString(ago(0), now), "just now");
  assert.equal(getAgeString(ago(1), now), "1 minute ago");
  assert.equal(getAgeString(ago(45), now), "45 minutes ago");
  assert.equal(getAgeString(ago(60), now), "1 hour ago");
  assert.equal(getAgeString(ago(60 * 5), now), "5 hours ago");
  assert.equal(getAgeString(ago(60 * 24), now), "1 day ago");
  assert.equal(getAgeString(ago(60 * 24 * 3), now), "3 days ago");
});

test("getAgeString accepts ISO strings", () => {
  const now = new Date("2026-07-14T12:00:00Z");
  assert.equal(getAgeString("2026-07-14T11:30:00Z", now), "30 minutes ago");
});
