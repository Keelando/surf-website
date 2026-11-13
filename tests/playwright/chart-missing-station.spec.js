// Buoys page charts: picking a station with nothing in buoy_timeseries_48h.json
// (an offline buoy) must swap the charts for "No … readings" notes, not leave
// the previously picked station's charts on screen under the new name.
//
// Southern Georgia Strait dropped out of the file on 2026-11-13 and showed
// Halibut Bank's charts. The station is removed from the served file here so
// the test doesn't depend on which buoys happen to be down today.
const { test, expect } = require("@playwright/test");

const MISSING = "4600303"; // Southern Georgia Strait
const PRESENT = "4600146"; // Halibut Bank

test("a station absent from the timeseries shows notes, not stale charts", async ({
  page,
}) => {
  await page.route("**/data/buoy_timeseries_48h.json*", async (route) => {
    const response = await route.fetch();
    const body = JSON.parse(await response.text());
    delete body[MISSING];
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto("/");

  const select = page.locator("#chart-buoy-select");
  // Charts are drawn lazily, once on screen.
  await page.locator("#wave-chart").scrollIntoViewIfNeeded();
  await select.selectOption(PRESENT);
  await expect(page.locator("#wave-chart canvas").first()).toBeVisible();

  await select.selectOption(MISSING);
  for (const [id, what] of [
    ["wave-chart", "wave"],
    ["wind-chart", "wind"],
    ["temp-chart", "temperature"],
  ]) {
    await expect(page.locator(`#${id}`)).toBeHidden();
    await expect(page.locator(`#${id}-empty`)).toHaveText(
      new RegExp(`^No ${what} readings from Southern Georgia Strait`),
    );
  }

  // Back to a station with data: the charts return.
  await select.selectOption(PRESENT);
  await expect(page.locator("#wave-chart")).toBeVisible();
  await expect(page.locator("#wave-chart-empty")).toBeHidden();
});
