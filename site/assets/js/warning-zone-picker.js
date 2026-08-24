/**
 * Warning-zone picker — the DOM half of the per-zone banner opt-in.
 *
 * Two controls, one preference:
 *
 *   1. A collapsed `<details>` under the zone `<select>`, listing every zone
 *      we carry as a checkbox, grouped by area. Its summary is static copy in
 *      forecasts.html — it names the control and states the storm floor, which
 *      is the one thing a reader needs before opening it.
 *   2. An inline "display warning banners for this zone" checkbox inside the
 *      rendered zone card — the discovery mechanism. A reader who cares about
 *      Howe Sound has to come here to read Howe Sound's forecast at all, which
 *      puts the control for banners about it directly under their eyes. That
 *      is why no footer link, banner gear, or settings page is needed.
 *
 * Both write the same key and re-render each other, so the two never disagree.
 * Resolution and the storm floor live in `shared/warning-zones.js`; storage in
 * `shared/warning-preferences.js`. This file only draws and listens.
 *
 * See docs/project/WARNING_ZONE_OPT_IN.md.
 */

import { orderZonesForDisplay, pickerZoneLabel } from "./shared/marine-zones.js";
import { readBannerZones, writeBannerZones } from "./shared/warning-preferences.js";
import { DEFAULT_BANNER_ZONES, getBannerZones } from "./shared/warning-zones.js";

const PICKER_ID = "warning-zone-picker";
const PICKER_LIST_ID = "warning-zone-picker-list";
const PICKER_COLLAPSE_ID = "warning-zone-picker-collapse";
const INLINE_TOGGLE_ID = "zone-alert-toggle";

/** The zone list for the current document, set by renderWarningZoneControls(). */
let currentZones = [];
/** The zone the page is currently showing, for the inline toggle. */
let currentZoneKey = null;
/**
 * The last non-empty selection, so unticking "no banners" is an undo rather
 * than a reset. Session-scoped on purpose: it is a courtesy for the reader who
 * ticked the box to see what it did, not a second preference to persist beside
 * the real one. On a later visit the box unticks back to the default pair.
 */
let lastNonEmptySelection = null;

/**
 * The zones currently allowed to raise the banner.
 * @returns {Array<string>} Zone keys
 */
function effectiveZoneKeys() {
  const available = currentZones.map((zone) => zone.zoneKey);
  return getBannerZones(readBannerZones(localStorage), available);
}

/**
 * Persist a selection and bring both controls (and the banner) into line.
 *
 * @param {Iterable<string>} zoneKeys - Zones that may raise the banner
 * @returns {void}
 */
function applySelection(zoneKeys) {
  const next = Array.from(zoneKeys);

  if (next.length > 0) {
    lastNonEmptySelection = next;
  }

  if (!writeBannerZones(localStorage, next)) {
    logger.warn("WarningZones", "Could not store warning zone selection");
  }

  render();
  // The banner is on this page too; without this it would keep showing the
  // previous selection until the next page load, and the control would look
  // broken.
  window.dispatchEvent(new CustomEvent("warning-zones:changed"));
}

/**
 * Add or remove one zone from the selection and persist it.
 *
 * Toggling for the first time materialises the default as an explicit choice,
 * which is what it already was in effect — the reader is editing the set they
 * can see, not a hidden one.
 *
 * @param {string} zoneKey - Zone to toggle
 * @param {boolean} wanted - Whether the reader wants alerts for it
 * @returns {void}
 */
function setZoneSelected(zoneKey, wanted) {
  const next = new Set(effectiveZoneKeys());
  if (wanted) {
    next.add(zoneKey);
  } else {
    next.delete(zoneKey);
  }

  applySelection(next);
}

/**
 * Turn every zone off, or put a selection back.
 *
 * The empty set was always reachable — untick all nine — but only as the end
 * of a chore, and nothing in the control said it was a supported state rather
 * than a mistake. This names it and makes it one click. It is honest about
 * what it cannot do: the storm floor in `shared/warning-zones.js` is not the
 * picker's to switch off, so the row says so on its face.
 *
 * @param {boolean} wanted - True for no banners, false to restore
 * @returns {void}
 */
function setNoBanners(wanted) {
  if (wanted) {
    // Captured *before* the write, so a reader with a hand-picked set of five
    // zones gets those five back when they untick, not the default pair.
    const current = effectiveZoneKeys();
    if (current.length > 0) lastNonEmptySelection = current;
    applySelection([]);
    return;
  }

  // Unticking is an undo, not a reset: the reader gets back the zones they had
  // a moment ago, falling back to the default pair on a later visit when the
  // in-memory copy is gone.
  applySelection(lastNonEmptySelection || DEFAULT_BANNER_ZONES);
}

/**
 * Human-readable name for a zone, matching the `<select>`'s vocabulary.
 *
 * Delegated to `pickerZoneLabel()` rather than decided here: the two controls
 * sit inches apart, and the moment they name the same water differently the
 * checkbox list stops looking like the dropdown's twin.
 *
 * @param {Object} zone - Zone record from listZones()
 * @param {boolean} areaHasSiblings - Whether the zone shares its area
 * @returns {string} Display label
 */
function zoneLabel(zone, areaHasSiblings) {
  return pickerZoneLabel(zone, areaHasSiblings);
}

/**
 * Build the "no banners" row that heads the list.
 *
 * First, not last: it is the answer to "how do I make this stop", and a reader
 * looking for that should not have to read nine zone names to find it.
 *
 * @param {Array<string>} selected - Effective zone keys
 * @returns {HTMLLabelElement}
 */
function makeNoBannersOption(selected) {
  const label = document.createElement("label");
  label.className = "warning-zone-option warning-zone-none";

  const input = document.createElement("input");
  input.type = "checkbox";
  input.checked = selected.length === 0;
  input.addEventListener("change", (event) => {
    setNoBanners(event.target.checked);
  });

  const text = document.createElement("span");
  text.textContent = "No banners for any zone";

  // The floor stated where the choice is made, not only in the summary above:
  // a reader who ticks this and then sees a storm banner anyway must have been
  // told here, or the control looks broken.
  const note = document.createElement("span");
  note.className = "warning-zone-note";
  note.textContent = "Storm Warnings (50kn+) will still be displayed";

  text.appendChild(note);
  label.append(input, text);
  return label;
}

/**
 * Draw the checkbox list inside the `<details>`.
 * @param {Array<string>} selected - Effective zone keys
 * @returns {void}
 */
function renderPickerList(selected) {
  const list = document.getElementById(PICKER_LIST_ID);
  if (!list) return;

  list.textContent = "";

  list.appendChild(makeNoBannersOption(selected));

  const makeOption = (zone, areaHasSiblings) => {
    const label = document.createElement("label");
    label.className = "warning-zone-option";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = zone.zoneKey;
    input.checked = selected.includes(zone.zoneKey);
    input.addEventListener("change", (event) => {
      setZoneSelected(zone.zoneKey, event.target.checked);
    });

    const text = document.createElement("span");
    text.textContent = zoneLabel(zone, areaHasSiblings);

    label.append(input, text);
    return label;
  };

  orderZonesForDisplay(currentZones).forEach((group) => {
    // An area with a single zone would get a heading identical to its one row
    // ("HOWE SOUND" over "Howe Sound"), so it becomes a bare checkbox instead
    // — the same call the `<select>` makes about optgroups, for the same
    // reason.
    if (group.zones.length === 1) {
      list.appendChild(makeOption(group.zones[0], false));
      return;
    }

    const fieldset = document.createElement("fieldset");
    fieldset.className = "warning-zone-group";

    const legend = document.createElement("legend");
    legend.textContent = group.areaName;
    fieldset.appendChild(legend);

    group.zones.forEach((zone) => fieldset.appendChild(makeOption(zone, true)));
    list.appendChild(fieldset);
  });
}

/**
 * Draw the inline toggle inside the rendered zone card.
 *
 * The mount point is an empty div emitted by forecasts.js and the control is
 * built with DOM calls rather than markup, because the card HTML goes through
 * DOMPurify — a form control's attributes are not something to leave at the
 * mercy of a sanitiser's defaults.
 *
 * @param {Array<string>} selected - Effective zone keys
 * @returns {void}
 */
function renderInlineToggle(selected) {
  const mount = document.getElementById(INLINE_TOGGLE_ID);
  if (!mount) return;

  mount.textContent = "";
  if (!currentZoneKey) return;

  const label = document.createElement("label");
  label.className = "zone-alert-toggle-label";

  const input = document.createElement("input");
  input.type = "checkbox";
  input.checked = selected.includes(currentZoneKey);
  input.addEventListener("change", (event) => {
    setZoneSelected(currentZoneKey, event.target.checked);
  });

  const text = document.createElement("span");
  text.textContent = "Display warning banners for this zone.";

  label.append(input, text);
  mount.appendChild(label);
}

/** Redraw both controls from storage. */
function render() {
  const selected = effectiveZoneKeys();

  renderPickerList(selected);
  renderInlineToggle(selected);
}

/**
 * Render both zone controls for the current document and selected zone.
 *
 * Called by forecasts.js after every re-render, since the inline toggle lives
 * inside markup the page rebuilds wholesale.
 *
 * @param {Array<Object>} zones - Zone list from shared/marine-zones.js
 * @param {string|null} selectedZoneKey - Zone the page is showing
 * @returns {void}
 */
export function renderWarningZoneControls(zones, selectedZoneKey) {
  currentZones = Array.isArray(zones) ? zones : [];
  currentZoneKey = selectedZoneKey;

  const picker = document.getElementById(PICKER_ID);
  // One zone is not worth a picker, matching the `<select>` next to it.
  if (picker) picker.hidden = currentZones.length < 2;

  const collapse = document.getElementById(PICKER_COLLAPSE_ID);
  if (picker && collapse && !collapse.dataset.listenerAttached) {
    collapse.addEventListener("click", () => {
      picker.open = false;
      // Closing a tall control can leave it above the viewport, so the page
      // appears to have jumped somewhere else. Bring it back only if it is
      // already out of view.
      picker.scrollIntoView({ block: "nearest" });
    });
    collapse.dataset.listenerAttached = "true";
  }

  render();
}
