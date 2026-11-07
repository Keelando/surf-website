#!/usr/bin/env node
// Rasterize the page-background textures (sea + linen weave) into one tile per
// theme. Called by scripts/utils/make_page_background.py --write, which owns
// the SVG and the CSS; run that, not this.
//
//   node scripts/utils/render_page_background.mjs <svg-dir> <png-dir>
//
// Why bitmaps: as SVG filter tiles (fractal noise + lighting) the textures
// were re-rendered on the raster threads for every page tile scrolled into
// view, doubling raster work on a touch tablet (measured 2026-11-07). A bitmap
// is decoded once and copied. Rendered at 2x so the weave stays crisp on
// high-density screens; the CSS draws it at 720x480 CSS px.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { chromium } from "playwright";

const [svgDir, pngDir] = process.argv.slice(2);
if (!svgDir || !pngDir) {
  console.error("usage: render_page_background.mjs <svg-dir> <png-dir>");
  process.exit(2);
}

const W = 720;
const H = 480;
const uri = (file) => `data:image/svg+xml;base64,${readFileSync(join(svgDir, file)).toString("base64")}`;

const browser = await chromium.launch();
try {
  const page = await browser.newPage({ viewport: { width: W, height: H }, deviceScaleFactor: 2 });
  for (const theme of ["light", "dark"]) {
    // Fabric (240px tile) repeats 3x2 inside the sea tile, so the composite
    // still tiles seamlessly at 720x480.
    await page.setContent(
      `<html><body style="margin:0;background:transparent">` +
        `<div id="t" style="width:${W}px;height:${H}px;` +
        `background-image:url('${uri(`fabric-${theme}.svg`)}'),url('${uri(`sea-${theme}.svg`)}');` +
        `background-size:240px 240px,${W}px ${H}px"></div></body></html>`,
    );
    await page.waitForTimeout(200);
    await page.locator("#t").screenshot({ path: join(pngDir, `texture-${theme}.png`), omitBackground: true });
    console.log(`rendered texture-${theme}.png`);
  }
} finally {
  await browser.close();
}
