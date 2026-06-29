/**
 * Guide page enhancements.
 *
 * Renders directional arrows in the "Reading the Direction Arrows" examples
 * using the shared getDirectionalArrow() helper from chart-utils-v4.js, so the
 * arrows match exactly what the maps draw (meteorological FROM direction → the
 * arrow points the opposite, travelling-toward way).
 *
 * Markup: <span class="dir-arrow" data-dir="292.5" data-type="wind"></span>
 */
document.addEventListener("DOMContentLoaded", () => {
  if (typeof getDirectionalArrow !== "function") return;

  for (const el of document.querySelectorAll(".dir-arrow[data-dir]")) {
    const degrees = Number.parseFloat(el.dataset.dir);
    if (Number.isNaN(degrees)) continue;
    const arrowType = el.dataset.type === "wave" ? "wave" : "wind";
    el.innerHTML = getDirectionalArrow(degrees, arrowType);
  }
});
