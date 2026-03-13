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

test.describe("Screenshots - light", () => {
  for (const route of routes) {
    test(route.name, async ({ page }) => {
      await page.goto(route.path, { waitUntil: "networkidle" });
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
      // Allow chart theme listeners to re-render
      await page.waitForTimeout(300);
      const dir = path.join(OUTPUT_DIR, "dark");
      fs.mkdirSync(dir, { recursive: true });
      await page.screenshot({ path: path.join(dir, `${route.name}.png`), fullPage: true });
    });
  }
});
