#!/usr/bin/env node
// Rasterize the halibut mark (site/assets/images/brand/*.svg) into the icon
// files browsers ask for by convention: favicon.svg, favicon.ico (16/32/48),
// apple-touch-icon.png (180, iOS) and the web manifest's icons (192/512, plus
// maskable ones for Android launchers). Re-run after editing a mark:
//
//   node scripts/utils/render_brand_icons.mjs
//
// Each size uses the cut drawn for it (the 16px one has its eyes snapped to
// whole pixels), so the ICO is assembled from per-size PNGs rather than one
// image scaled down.

import { copyFileSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const BRAND = join(ROOT, "site", "assets", "images", "brand");
const SITE = join(ROOT, "site");

// Web manifest icons (site/manifest.webmanifest lists the same files).
// "any" is the rounded-square mark as drawn; "maskable" is full bleed with the
// fish shrunk into the safe circle, since Android crops to its own shape.
const MANIFEST_ICONS = [
  { file: "icon-192.png", size: 192, svg: "halibut-32.svg" },
  { file: "icon-512.png", size: 512, svg: "halibut-32.svg" },
  { file: "icon-maskable-192.png", size: 192, svg: "halibut-maskable.svg" },
  { file: "icon-maskable-512.png", size: 512, svg: "halibut-maskable.svg" },
];

const ICO_SIZES = [
  { size: 16, svg: "halibut-16.svg" },
  { size: 32, svg: "halibut-32.svg" },
  { size: 48, svg: "halibut-32.svg" },
];

async function render(page, svgFile, size) {
  const svg = readFileSync(join(BRAND, svgFile));
  const uri = `data:image/svg+xml;base64,${svg.toString("base64")}`;
  await page.setViewportSize({ width: size, height: size });
  await page.setContent(
    `<body style="margin:0"><img src="${uri}" width="${size}" height="${size}" style="display:block"></body>`,
  );
  await page.locator("img").evaluate((img) => img.decode());
  return page.screenshot({ omitBackground: true });
}

// An ICO holding PNG images: a 6-byte header, a 16-byte entry per image,
// then the PNGs back to back (supported by every browser since Vista-era IE).
function buildIco(images) {
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0); // reserved
  header.writeUInt16LE(1, 2); // type: icon
  header.writeUInt16LE(images.length, 4);

  let offset = 6 + 16 * images.length;
  const entries = images.map(({ size, png }) => {
    const entry = Buffer.alloc(16);
    entry.writeUInt8(size % 256, 0); // width (0 means 256)
    entry.writeUInt8(size % 256, 1); // height
    entry.writeUInt8(0, 2); // palette colours
    entry.writeUInt8(0, 3); // reserved
    entry.writeUInt16LE(1, 4); // colour planes
    entry.writeUInt16LE(32, 6); // bits per pixel
    entry.writeUInt32LE(png.length, 8);
    entry.writeUInt32LE(offset, 12);
    offset += png.length;
    return entry;
  });

  return Buffer.concat([header, ...entries, ...images.map(({ png }) => png)]);
}

const browser = await chromium.launch();
try {
  const page = await browser.newPage({ deviceScaleFactor: 1 });

  const icoImages = [];
  for (const { size, svg } of ICO_SIZES) {
    icoImages.push({ size, png: await render(page, svg, size) });
  }
  writeFileSync(join(SITE, "favicon.ico"), buildIco(icoImages));

  writeFileSync(join(SITE, "apple-touch-icon.png"), await render(page, "halibut-touch.svg", 180));

  for (const { file, size, svg } of MANIFEST_ICONS) {
    writeFileSync(join(SITE, file), await render(page, svg, size));
  }

  // Browsers draw the SVG favicon at 16 CSS px, so it is the 16px cut.
  copyFileSync(join(BRAND, "halibut-16.svg"), join(SITE, "favicon.svg"));

  const written = [
    "favicon.ico",
    "favicon.svg",
    "apple-touch-icon.png",
    ...MANIFEST_ICONS.map((icon) => icon.file),
  ];
  console.log(`wrote ${written.map((f) => `site/${f}`).join(", ")}`);
} finally {
  await browser.close();
}
