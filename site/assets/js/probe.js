/* -----------------------------
   Scroll probe (ES module), home page only, loaded by main.js only when the
   URL has ?probe. Nobody else downloads it.

   For finding what makes the summary scroll slowly on a device emulation
   can't stand in for (site/docs/FRONTEND_CHANGELOG.md, 2026-10-29: headless
   Chromium paints in software and misjudges drawing cost). Each switch adds
   one stylesheet rule; switches combine (?probe=noblur,webp). The readout
   prints the browser, the viewport and which layout rules apply, and after
   each scroll a rough frame count from requestAnimationFrame.

   The readout sits in the page flow, not in a fixed overlay, since a fixed
   layer is itself a suspect.
   ----------------------------- */

// Each switch: the rule it adds, and a few words on what it tests.
const SWITCHES = {
  fixedphoto: {
    label: "Phone-style photo",
    about: "photo on a screen-sized fixed layer, as phones already get",
    css: `.summary { position: relative; isolation: isolate; clip-path: inset(0); background-image: none; }
      .summary::before { content: ""; position: fixed; inset: 0; z-index: -1; background-color: #1d3a52;
        background-image: var(--summary-tint), image-set(
          url("/assets/images/noaa-waves-1989-960.avif") type("image/avif"),
          url("/assets/images/noaa-waves-1989-960.webp") type("image/webp"));
        background-blend-mode: overlay, normal; background-position: center 60%; background-size: cover; }`,
  },
  webp: {
    label: "WebP photo",
    about: "WebP instead of AVIF, which is slower to decode",
    css: `.summary { background-image: var(--summary-tint), url("/assets/images/noaa-waves-1989-1600.webp"); }`,
  },
  small: {
    label: "Small photo",
    about: "the 960px photo at every width",
    css: `.summary { background-image: var(--summary-tint), image-set(
      url("/assets/images/noaa-waves-1989-960.avif") type("image/avif"),
      url("/assets/images/noaa-waves-1989-960.webp") type("image/webp")); }`,
  },
  nophoto: {
    label: "No photo",
    about: "plain colour behind the panel",
    css: `.summary, .summary::before { background-image: none !important; }`,
  },
  noblend: {
    label: "No blend mode",
    about: "blue tint layered normally, not blended",
    css: `.summary, .summary::before { background-blend-mode: normal !important; }`,
  },
  noblur: {
    label: "No blur",
    about: "panel's backdrop-filter off (same translucency)",
    css: `.summary-panel { -webkit-backdrop-filter: none; backdrop-filter: none; }`,
  },
  opaque: {
    label: "Opaque panel",
    about: "no blur and a solid panel",
    css: `.summary-panel { -webkit-backdrop-filter: none; backdrop-filter: none;
      background: var(--summary-panel-fallback-bg); }`,
  },
  noswell: {
    label: "No swell lines",
    about: "hides the masked wave line in every row",
    css: `.sl-swell { display: none !important; }`,
  },
  nosticky: {
    label: "Nav not sticky",
    about: "the top bar scrolls away (it is only sticky below 900px)",
    css: `.main-nav { position: static !important; }`,
  },
  nomap: {
    label: "No map",
    about: "removes the map section, so Leaflet never loads",
    css: `#map-section { display: none !important; }`,
  },
};

const active = (new URLSearchParams(location.search).get("probe") || "")
  .split(",")
  .filter((name) => SWITCHES[name]);

function hrefWith(names) {
  const url = new URL(location.href);
  url.searchParams.set("probe", names.join(","));
  url.hash = "";
  return url.pathname + url.search;
}

function layoutNotes() {
  const w = innerWidth;
  const notes = [];
  if (matchMedia("(hover: none) and (pointer: coarse)").matches)
    notes.push("touch rules (no shadows, still waves)");
  notes.push(
    w <= 599 ? "phone photo layer (≤599)" : w <= 768 ? "960px photo (≤768)" : "1600px photo (>768)",
  );
  notes.push(w <= 899 ? "sticky nav (≤899)" : "two columns, static nav (≥900)");
  if (matchMedia("(prefers-reduced-motion: reduce)").matches) notes.push("reduced motion");
  return notes.join(" · ");
}

function buildReadout() {
  const box = document.createElement("section");
  box.className = "probe";
  box.setAttribute("aria-label", "Scroll probe");
  box.style.cssText =
    "font: 14px/1.4 system-ui, sans-serif; padding: 0.75rem 1rem; background: #fff8d6; color: #222; border-bottom: 2px solid #c9a400;";

  const info = document.createElement("p");
  info.style.margin = "0 0 0.4rem";
  info.textContent = `${navigator.userAgent} — viewport ${innerWidth}×${innerHeight} @${devicePixelRatio}x — ${layoutNotes()}`;

  const status = document.createElement("p");
  status.style.margin = "0 0 0.4rem";
  status.style.fontWeight = "600";
  status.textContent = active.length
    ? `On: ${active.map((n) => SWITCHES[n].label).join(", ")}. Scroll the summary to measure.`
    : "Nothing switched off. Scroll the summary to measure.";

  const links = document.createElement("p");
  links.style.cssText = "margin: 0; display: flex; flex-wrap: wrap; gap: 0.4rem;";
  const add = (text, href, title, on) => {
    const a = document.createElement("a");
    a.href = href;
    a.textContent = text;
    a.title = title;
    a.style.cssText = `padding: 0.35rem 0.6rem; border: 1px solid #c9a400; border-radius: 6px; color: #222; text-decoration: none; background: ${on ? "#c9a400" : "#fff"};`;
    links.append(a);
  };
  add("Reset", hrefWith([]), "everything back on", false);
  for (const [name, sw] of Object.entries(SWITCHES)) {
    const on = active.includes(name);
    const next = on ? active.filter((n) => n !== name) : [...active, name];
    add(sw.label, hrefWith(next), sw.about, on);
  }

  box.append(info, status, links);
  document.getElementById("main-content")?.prepend(box);
  return status;
}

// Rough smoothness: frame intervals from requestAnimationFrame while a scroll
// is going on, reported once it has been still for 400ms. It sees frames the
// page didn't get to draw, not every compositor hiccup, so compare runs on
// the same device rather than reading the numbers as absolute.
function measureScrolls(status) {
  let frames = [];
  let last = 0;
  let idleTimer = 0;
  let running = false;

  const tick = (now) => {
    if (!running) return;
    if (last) frames.push(now - last);
    last = now;
    requestAnimationFrame(tick);
  };

  const report = () => {
    running = false;
    last = 0;
    if (frames.length < 5) return;
    const sorted = [...frames].sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];
    const slow = frames.filter((f) => f > 1.5 * median).length;
    const worst = sorted.at(-1);
    const fps = Math.round(1000 / median);
    status.textContent =
      `${active.length ? active.map((n) => SWITCHES[n].label).join(", ") : "Everything on"} — last scroll: ` +
      `${frames.length} frames, ~${fps} fps, ${Math.round((100 * slow) / frames.length)}% late, worst ${Math.round(worst)} ms`;
    frames = [];
  };

  addEventListener(
    "scroll",
    () => {
      if (!running) {
        running = true;
        requestAnimationFrame(tick);
      }
      clearTimeout(idleTimer);
      idleTimer = setTimeout(report, 400);
    },
    { passive: true },
  );
}

if (active.length) {
  const style = document.createElement("style");
  style.textContent = active.map((name) => SWITCHES[name].css).join("\n");
  document.head.append(style);
}
measureScrolls(buildReadout());
