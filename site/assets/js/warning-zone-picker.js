/**
 * Warning-zone picker — the DOM half of the per-zone banner opt-in.
 *
 * Two controls, one preference:
 *
 *   1. A collapsed `<details>` under the zone `<select>`, listing every zone
 *      we carry as a checkbox, grouped by area.
 *   2. An inline "alert me about this zone" checkbox inside the rendered zone
 *      card — the discovery mechanism. A reader who cares about Howe Sound has
 *      to come here to read Howe Sound's forecast at all, which puts the
 *      control to *be alerted about* it directly under their eyes. That is why
 *      no footer link, banner gear, or settings page is needed.
 *
 * Both write the same key and re-render each other, so the two never disagree.
 * Resolution and the storm floor live in `shared/warning-zones.js`; storage in
 * `shared/warning-preferences.js`. This file only draws and listens.
 *
 * See docs/project/WARNING_ZONE_OPT_IN.md.
 */

import { orderZonesForDisplay, shortZoneLabel } from "./shared/marine-zones.js";
import { readBannerZones, writeBannerZones } from "./shared/warning-preferences.js";
import { getBannerZones } from "./shared/warning-zones.js";

const PICKER_ID = "warning-zone-picker";
const PICKER_LIST_ID = "warning-zone-picker-list";
const PICKER_SUMMARY_ID = "warning-zone-picker-summary";
const INLINE_TOGGLE_ID = "zone-alert-toggle";

/** The zone list for the current document, set by renderWarningZoneControls(). */
let currentZones = [];
/** The zone the page is currently showing, for the inline toggle. */
let currentZoneKey = null;

/**
 * The zones currently allowed to raise the banner.
 * @returns {Array<string>} Zone keys
 */
function effectiveZoneKeys() {
  const available = currentZones.map((zone) => zone.zoneKey);
  return getBannerZones(readBannerZones(localStorage), available);
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
 * Human-readable name for a zone, matching the `<select>`'s vocabulary.
 *
 * @param {Object} zone - Zone record from listZones()
 * @param {boolean} areaHasSiblings - Whether the zone shares its area
 * @returns {string} Display label
 */
function zoneLabel(zone, areaHasSiblings) {
  return areaHasSiblings ? shortZoneLabel(zone.zoneName, zone.areaName) : zone.zoneName;
}

/**
 * One line stating the current selection, shown on the collapsed summary.
 *
 * Grouped by area so "Strait of Georgia north of Nanaimo, south of Nanaimo"
 * reads as one body of water rather than two orphaned fragments.
 *
 * @param {Array<string>} selected - Effective zone keys
 * @returns {string} Summary text
 */
function summaryText(selected) {
  const groups = orderZonesForDisplay(currentZones.filter((z) => selected.includes(z.zoneKey)));

  const phrases = groups.map((group) => {
    const areaZones = currentZones.filter((z) => z.areaName === group.areaName);
    if (group.zones.length === 1 && areaZones.length === 1) {
      return group.zones[0].zoneName;
    }
    const parts = group.zones.map((zone) => {
      const short = shortZoneLabel(zone.zoneName, group.areaName);
      return short === zone.zoneName ? short : short.charAt(0).toLowerCase() + short.slice(1);
    });
    return `${group.areaName} ${parts.join(", ")}`;
  });

  if (phrases.length === 0) {
    return "Alerting on: storm warnings only, anywhere";
  }
  return `Alerting on: ${phrases.join("; ")} — plus storms anywhere`;
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
  text.textContent = "Alert me sitewide about warnings here.";

  label.append(input, text);
  mount.appendChild(label);
}

/** Redraw both controls from storage. */
function render() {
  const selected = effectiveZoneKeys();

  const summary = document.getElementById(PICKER_SUMMARY_ID);
  if (summary) summary.textContent = summaryText(selected);

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

  render();
}
