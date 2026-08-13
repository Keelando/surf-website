/**
 * Leaflet fullscreen control (ES module).
 *
 * A small custom control rather than a vendored plugin: the page CSP blocks
 * external hosts, and the native Fullscreen API covers what we need in ~40
 * lines. Sits under the zoom control, matching Leaflet's own button styling.
 *
 * Safari still ships the API prefixed, so both spellings are probed. Where
 * neither exists (older iOS Safari refuses fullscreen on non-video elements)
 * the control is simply not added — no dead button.
 */

/** Whether this browser can put an arbitrary element into fullscreen. */
export function fullscreenSupported(element = document.documentElement) {
  return Boolean(element.requestFullscreen || element.webkitRequestFullscreen);
}

/** The element currently in fullscreen, if any (vendor-prefix tolerant). */
function currentFullscreenElement() {
  return document.fullscreenElement || document.webkitFullscreenElement || null;
}

function requestFullscreen(element) {
  const request = element.requestFullscreen || element.webkitRequestFullscreen;
  // May reject if not user-initiated; the button click satisfies that.
  return request ? Promise.resolve(request.call(element)).catch(() => {}) : Promise.resolve();
}

function exitFullscreen() {
  const exit = document.exitFullscreen || document.webkitExitFullscreen;
  return exit ? Promise.resolve(exit.call(document)).catch(() => {}) : Promise.resolve();
}

/**
 * Add a fullscreen toggle to a Leaflet map.
 *
 * @param {L.Map} map - initialized Leaflet map
 * @param {Object} [options]
 * @param {string} [options.position="topleft"] - Leaflet control corner
 * @param {string} [options.title="View fullscreen"] - button tooltip/label
 * @returns {L.Control|null} the control, or null when unsupported
 */
export function addFullscreenControl(map, options = {}) {
  const { position = "topleft", title = "View fullscreen" } = options;

  // The map's own container is what goes fullscreen, so the tiles, markers
  // and popups all come along.
  const container = map.getContainer();
  if (!fullscreenSupported(container)) return null;

  const control = L.control({ position });

  control.onAdd = () => {
    const wrapper = L.DomUtil.create("div", "leaflet-bar leaflet-control map-fullscreen-control");
    const button = L.DomUtil.create("a", "map-fullscreen-btn", wrapper);
    button.href = "#";
    button.title = title;
    button.setAttribute("role", "button");
    button.setAttribute("aria-label", title);
    button.innerHTML = '<span class="map-fullscreen-icon" aria-hidden="true"></span>';

    // Without this a click also pans/zooms the map underneath.
    L.DomEvent.disableClickPropagation(wrapper);
    L.DomEvent.on(button, "click", (event) => {
      L.DomEvent.preventDefault(event);
      if (currentFullscreenElement() === container) {
        exitFullscreen();
      } else {
        requestFullscreen(container);
      }
    });

    return wrapper;
  };

  control.addTo(map);

  // Leaflet caches the container size; entering or leaving fullscreen changes
  // it out from under the map, so re-measure or the tiles come back clipped.
  // Also covers Esc, which fires the event without going through our button.
  const onChange = () => {
    const isFullscreen = currentFullscreenElement() === container;
    const button = container.querySelector(".map-fullscreen-btn");
    if (button) {
      const label = isFullscreen ? "Exit fullscreen" : title;
      button.title = label;
      button.setAttribute("aria-label", label);
      button.classList.toggle("is-fullscreen", isFullscreen);
    }
    L.DomUtil[isFullscreen ? "addClass" : "removeClass"](container, "map-is-fullscreen");
    // invalidateSize before the browser has finished the transition reads the
    // old box, so defer a frame.
    requestAnimationFrame(() => map.invalidateSize());
  };

  document.addEventListener("fullscreenchange", onChange);
  document.addEventListener("webkitfullscreenchange", onChange);

  return control;
}
