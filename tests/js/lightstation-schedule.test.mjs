import assert from "node:assert/strict";
import test from "node:test";

import {
  describeMissedReports,
  describeNextReport,
  describeSchedule,
  nextReportTime,
} from "../../site/assets/js/shared/lightstation-schedule.js";

// A confident 7x-daily station on the 04:40 UTC cycle.
const REGULAR = {
  confident: true,
  reports_per_day: 7,
  longest_gap_hours: 6.0,
  slots_utc: ["04:40", "07:40", "10:40", "13:40", "16:40", "19:40", "22:40"],
};

test("next report names the day, not just the time", () => {
  // 2026-09-06 20:00 UTC is 13:00 PDT; the next slot is 22:40 UTC = 15:40 PDT.
  const from = new Date("2026-09-06T20:00:00Z");
  assert.equal(describeNextReport(REGULAR, false, from), "Today ~15:40");
});

test("a slot past local midnight is labelled Tomorrow", () => {
  // 2026-09-07 06:00 UTC is 23:00 PDT on the 6th. The next slot, 07:40 UTC,
  // is 00:40 PDT on the 7th — the case a bare "~00:40" reads as tonight.
  const from = new Date("2026-09-07T06:00:00Z");
  const described = describeNextReport(REGULAR, false, from);
  assert.equal(described, "Tomorrow ~00:40");
});

test("no next-report promise from a station that has stopped", () => {
  // The Nootka card: "5 days ago" above "Next report ~16:40" is the card
  // contradicting itself.
  const from = new Date("2026-09-06T20:00:00Z");
  assert.equal(describeNextReport(REGULAR, true, from), null);
  assert.ok(describeNextReport(REGULAR, false, from));
});

test("missed-report count separates late from off air", () => {
  const now = new Date("2026-09-06T20:00:00Z");
  const fiveDaysAgo = new Date("2026-09-01T20:00:00Z");
  assert.equal(describeMissedReports(REGULAR, fiveDaysAgo, now), "35 scheduled reports missed");
});

test("missed count is singular at one, and silent below one", () => {
  const now = new Date("2026-09-06T20:00:00Z");
  const oneSlotAgo = new Date(now.getTime() - (24 / 7) * 3600 * 1000);
  assert.equal(describeMissedReports(REGULAR, oneSlotAgo, now), "1 scheduled report missed");
  const recent = new Date(now.getTime() - 60 * 1000);
  assert.equal(describeMissedReports(REGULAR, recent, now), null);
});

test("missed count says nothing without a confident cadence", () => {
  const now = new Date("2026-09-06T20:00:00Z");
  const old = new Date("2026-09-01T20:00:00Z");
  assert.equal(describeMissedReports({ confident: false, reports_per_day: 7 }, old, now), null);
  assert.equal(describeMissedReports(null, old, now), null);
  assert.equal(describeMissedReports(REGULAR, null, now), null);
});

test("a daylight-only station still gets a next slot", () => {
  const daylight = {
    confident: true,
    reports_per_day: 4,
    longest_gap_hours: 15.0,
    slots_utc: ["15:10", "18:10", "21:10", "00:10"],
  };
  const from = new Date("2026-09-06T16:00:00Z");
  assert.equal(describeNextReport(daylight, false, from), "Today ~11:10");
  assert.ok(nextReportTime(daylight, from) > from);
});

test("schedule summary leads with the count, not a fake interval", () => {
  assert.equal(describeSchedule(REGULAR), "7× daily, up to 6 h apart");
});
