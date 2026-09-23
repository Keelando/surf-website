const { test } = require("@playwright/test");
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
];

const OUTPUT_DIR = path.join(__dirname, "..", "screenshots");

// Charts draw only once they near the viewport (chart-utils-v4.js), and a
// full-page capture does not scroll, so walk the page first or every chart
// below the fold comes out blank.
async function drawAllCharts(page) {
  await page.evaluate(async () => {
    for (let y = 0; y < document.documentElement.scrollHeight; y += 400) {
      window.scrollTo(0, y);
      await new Promise((resolve) => requestAnimationFrame(() => setTimeout(resolve, 50)));
    }
    window.scrollTo(0, 0);
  });
  // Let the last charts finish their entry animation.
  await page.waitForTimeout(1200);
}

test.describe("Screenshots - light", () => {
  for (const route of routes) {
    test(route.name, async ({ page }) => {
      await page.goto(route.path, { waitUntil: "networkidle" });
      await drawAllCharts(page);
      const dir = path.join(OUTPUT_DIR, "light");
      fs.mkdirSync(dir, { recursive: true });
      await page.screenshot({ path: path.join(dir, `${route.name}.png`), fullPage: true });
    });
  }
});

test.describe("Screenshots - dark", () => {
  for (const route of routes) {
    test(route.name, async ({ page }) => {
      await page.goto(route.path, { waitUntil: "networkidle" });
      await page.evaluate(() => {
        document.documentElement.setAttribute("data-theme", "dark");
        window.dispatchEvent(
          new CustomEvent("themechange", {
            detail: { theme: "dark", preference: "dark" },
          }),
        );
      });
      await drawAllCharts(page);
      const dir = path.join(OUTPUT_DIR, "dark");
      fs.mkdirSync(dir, { recursive: true });
      await page.screenshot({ path: path.join(dir, `${route.name}.png`), fullPage: true });
    });
  }
});
