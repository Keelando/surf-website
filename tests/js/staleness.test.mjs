import assert from "node:assert/strict";
import { test } from "node:test";
import {
  STALE_MARKER_OPACITY,
  staleDataWarningHTML,
  stalePopupTheme,
} from "../../site/assets/js/shared/staleness.js";

test("fresh theme uses info colours and plain header", () => {
  const t = stalePopupTheme(false);
  assert.equal(t.bg, "var(--color-callout-info-bg, #f0f8ff)");
  assert.equal(t.border, "var(--color-primary)");
  assert.equal(t.headingColor, "var(--map-popup-heading, var(--color-primary-dark))");
  assert.equal(t.headerText, "Latest Conditions:");
});

test("stale theme uses danger colours and threshold header", () => {
  const t = stalePopupTheme(true);
  assert.equal(t.bg, "var(--color-callout-danger-bg, #fff5f5)");
  assert.equal(t.border, "var(--color-accent-red)");
  assert.equal(t.headingColor, "var(--color-accent-red)");
  assert.equal(t.headerText, "Latest Conditions (STALE - >3h old):");
});

test("label and threshold are configurable (lightstations, winds)", () => {
  assert.equal(
    stalePopupTheme(true, { threshold: ">12h" }).headerText,
    "Latest Conditions (STALE - >12h old):",
  );
  const winds = { label: "Current Wind", staleLabel: "Last Wind" };
  assert.equal(stalePopupTheme(true, winds).headerText, "Last Wind (STALE - >3h old):");
  assert.equal(stalePopupTheme(false, winds).headerText, "Current Wind:");
});

test("stale marker opacity matches the site-wide value", () => {
  assert.equal(STALE_MARKER_OPACITY, 0.35);
});

test("stale warning line is red and bold", () => {
  const html = staleDataWarningHTML();
  assert.match(html, /STALE DATA/);
  assert.match(html, /var\(--color-accent-red\)/);
  assert.match(html, /font-weight: 600/);
});
