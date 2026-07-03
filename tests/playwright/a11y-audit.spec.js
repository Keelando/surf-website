// Temporary accessibility audit spec — run with:
//   npx playwright test tests/playwright/a11y-audit.spec.js --project=chromium
const { test } = require("@playwright/test");
const AxeBuilder = require("@axe-core/playwright").default;
const path = require("path");
const fs = require("fs");

const routes = [
  { name: "home", path: "/" },
  { name: "tides", path: "/tides.html" },
  { name: "winds", path: "/winds.html" },
  { name: "storm_surge", path: "/storm_surge.html" },
  { name: "lightstations", path: "/lightstations.html" },
  { name: "webcams", path: "/webcams.html" },
  { name: "forecasts", path: "/forecasts.html" },
  { name: "guide", path: "/guide.html" },
  { name: "analytics", path: "/analytics.html" },
];

const OUT_DIR =
  process.env.A11Y_OUT ||
  path.join(__dirname, "..", "..", "a11y-results");

async function scan(page, route, theme) {
  await page.goto(route.path, { waitUntil: "networkidle" });
  if (theme === "dark") {
    await page.evaluate(() => {
      document.documentElement.setAttribute("data-theme", "dark");
      window.dispatchEvent(
        new CustomEvent("themechange", {
          detail: { theme: "dark", preference: "dark" },
        }),
      );
    });
    await page.waitForTimeout(300);
  }
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "best-practice"])
    .analyze();
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const slim = results.violations.map((v) => ({
    id: v.id,
    impact: v.impact,
    help: v.help,
    tags: v.tags.filter((t) => t.startsWith("wcag") || t === "best-practice"),
    nodes: v.nodes.map((n) => ({
      target: n.target,
      html: n.html.slice(0, 300),
      summary: n.failureSummary && n.failureSummary.slice(0, 400),
    })),
  }));
  fs.writeFileSync(
    path.join(OUT_DIR, `${route.name}-${theme}.json`),
    JSON.stringify(slim, null, 2),
  );
}

for (const theme of ["light", "dark"]) {
  test.describe(`axe - ${theme}`, () => {
    for (const route of routes) {
      test(`${route.name}`, async ({ page }) => {
        await scan(page, route, theme);
      });
    }
  });
}
