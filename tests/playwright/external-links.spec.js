// Every "view the original" link on the site must actually leave the site.
//
// Why this needs a browser rather than a unit test: the links are built as
// markup strings and rendered through `setSafeHTML` → `sanitize-html.js` →
// DOMPurify, and `target` is not in DOMPurify's default allow-list. So a link
// written correctly in the source arrived in the DOM stripped of its `target`
// and opened in the same tab, while its ↗ and its aria-label went on promising
// a new one. That was true of every JS-rendered source link on the site until
// 2026-09-06 and no existing suite noticed: `test:js` never runs DOMPurify,
// and the console spec only watches for errors.
//
// The second assertion is the safety half. Allowing `target` back is only safe
// with `rel="noopener"` — without it the opened page gets a `window.opener`
// handle to ours. sanitize-html.js stamps that on via a DOMPurify hook so no
// call site has to remember; this is what proves the hook is still installed.
const { test, expect } = require("@playwright/test");

const sourceLinks = [
  { route: "/", selector: ".buoy-source-link", label: "buoy card source" },
  { route: "/lightstations.html", selector: ".ls-card-source-link", label: "lightstation card source" },
  { route: "/forecasts.html", selector: ".forecast-source-link", label: "forecast source footer" },
  { route: "/forecasts.html", selector: ".forecast-zone h2 a", label: "forecast zone heading source" },
];

test.describe("Sanitized external links", () => {
  for (const { route, selector, label } of sourceLinks) {
    test(`${label} opens externally and is opener-safe`, async ({ page }) => {
      await page.goto(route, { waitUntil: "networkidle" });
      const links = page.locator(selector);
      await expect(links.first()).toBeAttached({ timeout: 10000 });

      const attrs = await links.evaluateAll((nodes) =>
        nodes.map((a) => ({ href: a.getAttribute("href"), target: a.target, rel: a.rel })),
      );
      expect(attrs.length, `no ${label} links rendered`).toBeGreaterThan(0);

      for (const a of attrs) {
        expect(a.href, `${label} href`).toMatch(/^https?:\/\//);
        expect(a.target, `${label} target — DOMPurify strips this without ADD_ATTR`).toBe("_blank");
        expect(a.rel, `${label} rel — the afterSanitizeAttributes hook sets this`).toContain(
          "noopener",
        );
      }
    });
  }
});
