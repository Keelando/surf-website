import assert from "node:assert/strict";
import { test } from "node:test";
import { dayLabel, maxDayOffset, pacificDate } from "../../site/assets/js/tides-modules/utils.js";

// 2026-10-24 05:30 UTC is still Friday Oct 23 in Vancouver (PDT, UTC-7).
const FRI_EVENING_PACIFIC = new Date("2026-10-24T05:30:00Z");

test("navigation reaches the last day the export covers", () => {
  assert.equal(maxDayOffset({ _meta: { days_included: 7 } }), 6);
  assert.equal(maxDayOffset({ _meta: { days_included: 3 } }), 2);
});

test("an export without days_included keeps the old three days", () => {
  assert.equal(maxDayOffset({}), 2);
  assert.equal(maxDayOffset(null), 2);
  assert.equal(maxDayOffset({ _meta: { days_included: 0 } }), 2);
});

test("days count from the Pacific date, not the UTC one", () => {
  const d = pacificDate(0, FRI_EVENING_PACIFIC);
  assert.deepEqual([d.getFullYear(), d.getMonth(), d.getDate()], [2026, 9, 23]);
  const week = pacificDate(6, FRI_EVENING_PACIFIC);
  assert.deepEqual([week.getMonth(), week.getDate()], [9, 29]);
});

test("day labels carry the weekday", () => {
  assert.equal(dayLabel(0, FRI_EVENING_PACIFIC), "Today (Fri Oct 23)");
  assert.equal(dayLabel(1, FRI_EVENING_PACIFIC), "Tomorrow (Sat Oct 24)");
  assert.equal(dayLabel(6, FRI_EVENING_PACIFIC), "Thu Oct 29");
});

test("month boundaries roll over", () => {
  assert.equal(dayLabel(9, FRI_EVENING_PACIFIC), "Sun Nov 1");
});
