/* -----------------------------
   Buoy Card Builders (ES module)

   Pure HTML-string builders for the index-page buoy cards, extracted from
   the 693-line loadBuoyData() in main.js (step 2 of
   docs/project/BUOY_CARD_REFACTOR.md). Every builder takes the snapshot
   object `b` and, where per-station behaviour differs, the stations.json
   metadata object `meta` — never a station ID. main.js keeps the fetching,
   region grouping, and event wiring.

   degreesToCardinal and getDirectionalArrow still come from the classic
   script chart-utils-v4.js loaded before the module graph, same as main.js.
   ----------------------------- */

import { createAngularSpreadVector } from "./shared/markers.js";
import { formatDataAge } from "./shared/staleness.js";
import {
  isNoaaStation,
  isPileStation,
  isSurreyStation,
  sourceUrl,
  usesDominantPeriod,
  usesSwellDisplay,
  waveHeightPrecision,
} from "./shared/station-meta.js";

/**
 * Data-freshness state for a snapshot.
 * - up to 3 hours: no warning
 * - 3-12 hours: age warning, data still shown
 * - over 12 hours: "STATION DOWN", data hidden from the compact view
 *
 * @param {Object} b - Buoy snapshot entry
 * @returns {{ageMinutes: number, isDown: boolean, isStale: boolean}}
 */
export function freshnessState(b) {
  const ageMinutes = b.age_minutes || 0;
  const ageHours = ageMinutes / 60;
  const isDown = ageHours > 12;
  return { ageMinutes, isDown, isStale: ageHours > 3 && !isDown };
}

/** Source badge shown next to the station name on each card. */
export function sourceBadge(meta) {
  if (isNoaaStation(meta)) {
    return ` <span style="font-size: 0.7em; color: var(--color-source-noaa-text); font-weight: normal;">🇺🇸 NOAA</span>`;
  }
  if (isSurreyStation(meta)) {
    return ` <span style="font-size: 0.7em; color: var(--color-accent-green); font-weight: normal;">🏛️ Surrey (FlowWorks)</span>`;
  }
  return ` <span style="font-size: 0.7em; color: var(--color-source-envcan-text); font-weight: normal;">🇨🇦 Env Canada</span>`;
}

/** Source-coloured left border, applied to the card element itself. */
export function applyCardBorder(card, meta) {
  if (isNoaaStation(meta)) {
    card.style.borderLeft = "4px solid var(--color-source-noaa-border)";
  } else if (isSurreyStation(meta)) {
    card.style.borderLeft = "4px solid var(--color-accent-green)";
  }
}

/** "🔗 View Source Data" footer link, or "" when the station has no URL. */
export function buildSourceLink(meta) {
  const stationLink = sourceUrl(meta);
  if (!stationLink) return "";
  return `
          <p style="margin-top: 0.75rem; margin-bottom: 0; padding-top: 0.5rem; border-top: 1px solid var(--color-border); text-align: center;">
            <a href="${stationLink}" target="_blank" rel="noopener noreferrer" style="
              font-size: 0.85em;
              color: var(--color-primary-dark);
              text-decoration: none;
              font-weight: 500;
            ">
              🔗 View Source Data
            </a>
          </p>
        `;
}

/**
 * Minimal card for a station with no data in the DB at all (e.g. buoy
 * offline and records purged).
 */
export function buildNoDataCard(b, id, meta) {
  const stationLink = sourceUrl(meta);
  let html = `<div class="buoy-card-inner"><h2>${b.name || id}`;
  html += sourceBadge(meta);
  html += `</h2>
            <p class="buoy-metric" style="margin: 1rem 0; padding: 1rem; background: var(--color-callout-danger-bg); border-left: 4px solid var(--color-accent-red); border-radius: 4px; color: var(--color-error-text); font-weight: 600;">
              🔴 Station offline — no data available
            </p>`;
  if (stationLink) {
    html += `<p style="margin-top: 0.75rem; margin-bottom: 0; padding-top: 0.5rem; border-top: 1px solid var(--color-border); text-align: center;">
              <a href="${stationLink}" target="_blank" rel="noopener noreferrer" style="font-size: 0.85em; color: var(--color-primary-dark); text-decoration: none; font-weight: 500;">🔗 View Source Data</a>
            </p>`;
  }
  html += `</div>`;
  return html;
}

/** Station name + source badge + "Last Update" line with any age warning. */
export function buildCardHeader(b, id, meta, freshness, formatTimestamp) {
  const { ageMinutes, isDown, isStale } = freshness;
  const updated = b.observation_time ? formatTimestamp(b.observation_time) : "—";

  let ageWarning = "";
  if (isDown) {
    ageWarning = ` <span style="color: var(--color-accent-red); font-weight: bold; background: var(--color-callout-danger-bg); padding: 0.2rem 0.5rem; border-radius: 3px;">🔴 STATION DOWN (${formatDataAge(ageMinutes)})</span>`;
  } else if (isStale) {
    ageWarning = ` <span style="color: var(--color-error-text); font-weight: bold;">⚠️ STALE (${formatDataAge(ageMinutes)})</span>`;
  }

  let html = `<h2>${b.name || id}`;
  html += sourceBadge(meta);
  html += `</h2>`;
  html += `<p style="font-size: 0.9em; color: var(--color-text-muted); margin-top: -0.5rem;">Last Update: ${updated}${ageWarning}</p>`;
  return html;
}

/** Compact wind line — "WNW 15 G 20 kn (350°) ↓". */
export function buildWindLine(b) {
  const windSpeed = b.wind_speed != null ? Math.round(b.wind_speed) : "—";
  const windGust = b.wind_gust != null ? Math.round(b.wind_gust) : "—";

  let windDisplay = "No data";
  if (windSpeed !== "—") {
    const windCardinal = b.wind_direction_cardinal ?? "—";
    const windDir = b.wind_direction_deg || b.wind_direction;
    const windDegrees = windDir != null ? ` (${Math.round(windDir)}°)` : "";
    const gustPart = windGust !== "—" ? ` G ${windGust}` : "";
    windDisplay = `${windCardinal} ${windSpeed}${gustPart} kn${windDegrees} ${getDirectionalArrow(windDir, "wind")}`;
  }
  return `<p class="buoy-metric" style="margin: 0.5rem 0;"><b>💨 Wind:</b> ${windDisplay}</p>`;
}

/** Small muted tag noting which period type is shown. */
function periodTag(label) {
  return ` <span style="color: var(--color-text-muted); font-size: 0.85em;">${label}</span>`;
}

/**
 * Compact wave line — "Sig Wave: WSW 0.9m @ 4.1s (251°) ➤".
 *
 * Height is always significant; the period type is tagged when it isn't the
 * significant value (e.g. NOAA dominant, or an avg/peak fallback). Includes
 * the "dominant" footnote on the NOAA stations that need it.
 */
export function buildWaveLine(b, meta) {
  const heightPrecision = waveHeightPrecision(meta);
  let waveDisplay = "No data";
  let waveLabel = "🌊 Sig Wave:";

  if (usesSwellDisplay(meta)) {
    // Neah Bay (NOAA) - show swell info (continuous open-ocean swell)
    waveLabel = "🌊 Swell:";
    const swellHeight = b.swell_height != null ? b.swell_height.toFixed(heightPrecision) : "—";
    const swellPeriod = b.swell_period != null ? b.swell_period.toFixed(1) : null;
    const swellDir = b.swell_direction_cardinal ?? null;
    const swellDegrees = b.swell_direction != null ? ` (${Math.round(b.swell_direction)}°)` : "";
    if (swellHeight !== "—") {
      const dirDisplay = swellDir ? `${swellDir} ` : "";
      const arrowDisplay =
        b.swell_direction != null ? ` ${getDirectionalArrow(b.swell_direction, "wave")}` : "";
      const periodDisplay = swellPeriod != null ? ` @ ${swellPeriod}s` : "";
      waveDisplay = `${dirDisplay}${swellHeight}m${periodDisplay}${swellDegrees}${arrowDisplay}`;
    }
  } else if (usesDominantPeriod(meta)) {
    // NOAA (New Dungeness, Angeles Point) - sig height + dominant period (DPD).
    // NOAA stores DPD in wave_period_sig; tag it "dominant" so it's explicit.
    const waveHeight = b.wave_height_sig != null ? b.wave_height_sig.toFixed(heightPrecision) : "—";
    const wavePeriod = b.wave_period_sig != null ? b.wave_period_sig.toFixed(1) : null;
    const waveDir = b.wave_direction_peak_cardinal ?? b.wave_direction_avg_cardinal ?? null;
    const waveDirectionValue = b.wave_direction_peak ?? b.wave_direction_avg;
    const waveDegrees = waveDirectionValue != null ? ` (${Math.round(waveDirectionValue)}°)` : "";
    if (waveHeight !== "—") {
      const dirDisplay = waveDir ? `${waveDir} ` : "";
      const arrowDisplay =
        waveDirectionValue != null ? ` ${getDirectionalArrow(waveDirectionValue, "wave")}` : "";
      const periodDisplay = wavePeriod != null ? ` @ ${wavePeriod}s${periodTag("dominant")}` : "";
      waveDisplay = `${dirDisplay}${waveHeight}m${periodDisplay}${waveDegrees}${arrowDisplay}`;
    }
  } else {
    // EC buoys - significant height + significant period. The two buoy families
    // publish sig period under different SWOB names (wave_period_sig /
    // wave_period_sig_basic), so coalesce. Fall back to avg/peak only for
    // non-EC stations, and tag any fallback explicitly.
    const waveHeight = b.wave_height_sig != null ? b.wave_height_sig.toFixed(heightPrecision) : "—";
    let wavePeriodValue = b.wave_period_sig ?? b.wave_period_sig_basic;
    let periodType = "sig";
    if (wavePeriodValue == null) {
      if (b.wave_period_avg != null) {
        wavePeriodValue = b.wave_period_avg;
        periodType = "avg";
      } else if (b.wave_period_peak != null) {
        wavePeriodValue = b.wave_period_peak;
        periodType = "peak";
      }
    }
    const wavePeriod = wavePeriodValue != null ? wavePeriodValue.toFixed(1) : null;
    const waveDir = b.wave_direction_peak_cardinal ?? b.swell_direction_cardinal ?? null;
    const waveDirectionValue = b.wave_direction_peak ?? b.swell_direction;
    const waveDegrees = waveDirectionValue != null ? ` (${Math.round(waveDirectionValue)}°)` : "";

    if (waveHeight !== "—") {
      const dirDisplay = waveDir ? `${waveDir} ` : "";
      const arrowDisplay =
        waveDirectionValue != null ? ` ${getDirectionalArrow(waveDirectionValue, "wave")}` : "";
      // Only tag the period when it isn't the significant value
      const periodSuffix = periodType === "sig" ? "" : periodTag(periodType);
      const periodDisplay = wavePeriod != null ? ` @ ${wavePeriod}s${periodSuffix}` : "";
      waveDisplay = `${dirDisplay}${waveHeight}m${periodDisplay}${waveDegrees}${arrowDisplay}`;
    }
  }

  let html = `<p class="buoy-metric" style="margin: 0.5rem 0;"><b>${waveLabel}</b> ${waveDisplay}</p>`;

  // NOAA reports a "dominant" period rather than a significant one; readers
  // won't know the term, so add a small footnote on these cards.
  if (usesDominantPeriod(meta)) {
    html += `<p style="margin: -0.25rem 0 0.5rem 0; font-size: 0.7em; color: var(--color-text-muted); line-height: 1.3;">Dominant = the wave period with the most energy (NOAA's term for peak period).</p>`;
  }
  return html;
}

/** Always-visible condensed view: wind + wave lines, or a station-down notice. */
export function buildCompactView(b, meta, freshness) {
  let html = `<div class="card-compact-view">`;

  if (freshness.isDown) {
    html += `
          <p class="buoy-metric" style="margin: 1rem 0; padding: 1rem; background: var(--color-callout-danger-bg); border-left: 4px solid var(--color-accent-red); border-radius: 4px; color: var(--color-error-text); font-weight: 600;">
            🔴 Station Down - No recent data available
          </p>
          <p style="font-size: 0.85em; color: var(--color-text-muted); text-align: center; margin-top: 0.5rem;">
            Last data received ${formatDataAge(freshness.ageMinutes)}
          </p>
        `;
  } else {
    html += buildWindLine(b);
    html += buildWaveLine(b, meta);
  }

  html += `</div>`;
  return html;
}

/** The "Show Details" / "Show History" button pair. */
export function buildToggleButtons() {
  return `
        <div style="display: flex; gap: 0.5rem; margin-top: 0.75rem;">
          <button class="toggle-details-btn" style="
            flex: 1;
            padding: 0.5rem;
            background: var(--color-card-muted-bg);
            border: 1px solid var(--color-card-muted-border);
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            color: var(--color-primary-dark);
            font-weight: 600;
            transition: background 0.2s;
          ">
            ▼ Show Details
          </button>
          <button class="toggle-history-btn" style="
            flex: 1;
            padding: 0.5rem;
            background: var(--color-card-muted-bg);
            border: 1px solid var(--color-card-muted-border);
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            color: var(--color-primary-dark);
            font-weight: 600;
            transition: background 0.2s;
          ">
            📈 Show History (12h)
          </button>
        </div>
      `;
}

/** Down/stale callout shown at the top of the expanded details section. */
export function buildStalenessCallout(freshness) {
  const { ageMinutes, isDown, isStale } = freshness;
  if (isDown) {
    return `
          <div style="margin-bottom: 1rem; padding: 0.75rem; background: var(--color-callout-warning-bg); border-left: 4px solid var(--color-accent-orange); border-radius: 4px;">
            <p style="margin: 0; color: var(--color-warning-text); font-weight: 600;">⚠️ Station Down - Showing Last Known Data</p>
            <p style="margin: 0.25rem 0 0 0; font-size: 0.85em; color: var(--color-warning-text);">
              This data is from ${formatDataAge(ageMinutes)} and does not reflect current conditions.
            </p>
          </div>
        `;
  }
  if (isStale) {
    return `
          <div style="margin-bottom: 1rem; padding: 0.75rem; background: var(--color-callout-warning-bg); border-left: 4px solid var(--color-accent-orange); border-radius: 4px;">
            <p style="margin: 0; color: var(--color-warning-text); font-weight: 600;">⚠️ Stale Data Warning</p>
            <p style="margin: 0.25rem 0 0 0; font-size: 0.85em; color: var(--color-warning-text);">
              This data is ${formatDataAge(ageMinutes)} old. Newer data may not be available.
            </p>
          </div>
        `;
  }
  return "";
}

/** NOAA spectral wave breakdown: combined, wind waves, swell, peak. */
export function buildNoaaWaveDetails(b) {
  let html = `<p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">Detailed Wave Metrics</p>`;

  html += `
          <p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">📊 Significant Wave (Combined)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Sig Height:</b> ${b.wave_height_sig ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Dominant Period:</b> ${b.wave_period_sig ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Period:</b> ${b.wave_period_avg ?? "—"} s</p>
        `;

  html += `
          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">💨 Wind Waves (Local Chop)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Height:</b> ${b.wind_wave_height ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Period:</b> ${b.wind_wave_period ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Direction:</b> ${b.wind_wave_direction_cardinal ?? "—"} (${b.wind_wave_direction ?? "—"}°) ${getDirectionalArrow(b.wind_wave_direction, "wave")}</p>

          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">🌊 Ocean Swell (Long Period)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Height:</b> ${b.swell_height ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Period:</b> ${b.swell_period ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Direction:</b> ${b.swell_direction_cardinal ?? "—"} (${b.swell_direction ?? "—"}°) ${getDirectionalArrow(b.swell_direction, "wave")}</p>

          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">📈 Peak Metrics</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Direction:</b> ${b.wave_direction_peak_cardinal ?? "—"} (${b.wave_direction_peak ?? "—"}°) ${getDirectionalArrow(b.wave_direction_peak, "wave")}</p>
        `;
  return html;
}

/** Descriptor + colour for the peak-frequency angular spread. */
function peakSpreadDescriptor(peakSpread) {
  if (peakSpread < 25) return { desc: "very organized", color: "var(--color-accent-green)" };
  if (peakSpread < 35) return { desc: "organized", color: "var(--color-accent-green)" };
  if (peakSpread < 45) return { desc: "moderate", color: "var(--color-accent-orange)" };
  return { desc: "confused", color: "var(--color-accent-red)" };
}

/** Descriptor + colour for the all-frequency angular spread. */
function avgSpreadDescriptor(avgSpread) {
  if (avgSpread < 30) return { desc: "very clean", color: "var(--color-accent-green)" };
  if (avgSpread < 45) return { desc: "clean", color: "var(--color-accent-green)" };
  if (avgSpread < 60) return { desc: "mixed", color: "var(--color-accent-orange)" };
  return { desc: "messy", color: "var(--color-accent-red)" };
}

/**
 * Wave direction angular spread: labelled values, visual spread vectors, and
 * the collapsible explanation. Returns "" when the station reports no spread.
 */
export function buildSpreadSection(b, id) {
  const peakSpread = b.wave_direction_spread_peak;
  const avgSpread = b.wave_direction_spread_avg;
  if (peakSpread == null && avgSpread == null) return "";

  let html = `
              <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark);">
                🧭 Wave Direction Angular Spread
                <span class="spread-info-btn" style="cursor: pointer; font-size: 0.9em; margin-left: 0.3rem; color: var(--color-primary); user-select: none;" title="Click for explanation">ℹ️</span>
              </p>
            `;

  if (peakSpread != null) {
    const { desc, color } = peakSpreadDescriptor(peakSpread);
    html += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Spread:</b> ${peakSpread}° <span style="color: ${color}; font-weight: 600;">(${desc})</span> <span style="font-size: 0.85em; color: var(--color-text-muted);">— dominant swell</span></p>`;
  }

  if (avgSpread != null) {
    const { desc, color } = avgSpreadDescriptor(avgSpread);
    html += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Spread:</b> ${avgSpread}° <span style="color: ${color}; font-weight: 600;">(${desc})</span> <span style="font-size: 0.85em; color: var(--color-text-muted);">— all frequencies</span></p>`;
  }

  // Visual angular spread vectors
  const peakDir = b.wave_direction_peak;
  const avgDir = b.wave_direction_avg;

  if ((peakDir != null && peakSpread != null) || (avgDir != null && avgSpread != null)) {
    html += `<div style="margin-top: 0.75rem; padding: 0.75rem; background: var(--color-surface-alt); border-radius: 4px; border: 1px solid var(--color-border-light);">`;
    html += `<p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">Visual Direction & Spread</p>`;

    if (peakDir != null && peakSpread != null) {
      html += `
                  <div style="margin: 0.5rem 0;">
                    <span style="font-size: 0.85em; color: var(--color-text-muted); font-weight: 600;">Peak:</span>
                    ${createAngularSpreadVector(peakDir, peakSpread, 70)}
                    <span style="font-size: 0.75em; color: var(--color-text-muted); margin-left: 0.5rem;">${b.wave_direction_peak_cardinal ?? degreesToCardinal(peakDir)} ${Math.round(peakDir)}° ± ${Math.round(peakSpread / 2)}°</span>
                  </div>
                `;
    }

    if (avgDir != null && avgSpread != null) {
      html += `
                  <div style="margin: 0.5rem 0;">
                    <span style="font-size: 0.85em; color: var(--color-text-muted); font-weight: 600;">Average:</span>
                    ${createAngularSpreadVector(avgDir, avgSpread, 70)}
                    <span style="font-size: 0.75em; color: var(--color-text-muted); margin-left: 0.5rem;">${b.wave_direction_avg_cardinal ?? degreesToCardinal(avgDir)} ${Math.round(avgDir)}° ± ${Math.round(avgSpread / 2)}°</span>
                  </div>
                `;
    }

    html += `<p style="font-size: 0.75em; color: var(--color-text-light); margin-top: 0.5rem; margin-bottom: 0;">Arrows show wave travel direction. Sector shows angular spread.</p>`;
    html += `</div>`;
  }

  // Collapsible explanatory footnote (hidden by default)
  if (peakSpread != null && avgSpread != null) {
    html += `
                <div id="spread-info-${id}" style="display: none; font-size: 0.85em; color: var(--color-text-light); margin-top: 0.5rem; padding: 0.75rem; background: var(--color-callout-info-bg); border-left: 3px solid var(--color-primary); border-radius: 4px; line-height: 1.5;">
                  <strong style="color: var(--color-tagline-text);">Angular Spread</strong> measures how organized the waves are:<br>
                  <br>
                  <strong>Lower numbers</strong> = waves coming from one direction (clean swell)<br>
                  <strong>Higher numbers</strong> = waves from multiple directions (choppy/messy)<br>
                  <br>
                  • <strong>Peak:</strong> The main swell direction<br>
                  • <strong>Average:</strong> Overall surface (includes wind chop)<br>
                  <br>
                  <span style="font-size: 0.9em; color: var(--color-text-muted);">Beach conditions may differ from open-ocean buoy readings.</span>
                </div>
              `;
  }

  return html;
}

/**
 * EC buoys and other non-NOAA stations: sig is the headline (compact card),
 * so here we add average + one "high-end" reading. EC buoy families report the
 * high end differently (average-max / peak / plain max), so pick whichever
 * exists. Returns "" when the station reports none of them.
 */
export function buildEcWaveDetails(b, meta, id) {
  const heightPrecision = waveHeightPrecision(meta);

  let highHeight = null;
  let highHeightLabel = "";
  if (b.wave_height_max_avg != null) {
    highHeight = b.wave_height_max_avg;
    highHeightLabel = "Avg Max Height";
  } else if (b.wave_height_peak != null) {
    highHeight = b.wave_height_peak;
    highHeightLabel = "Peak Height";
  } else if (b.wave_height_max != null) {
    highHeight = b.wave_height_max;
    highHeightLabel = "Max Height";
  }

  let highPeriod = null;
  let highPeriodLabel = "";
  if (b.wave_period_peak != null) {
    highPeriod = b.wave_period_peak;
    highPeriodLabel = "Peak Period";
  } else if (b.wave_period_max_wave != null) {
    highPeriod = b.wave_period_max_wave;
    highPeriodLabel = "Period of Largest Wave";
  }

  const hasDetailData =
    b.wave_height_avg != null ||
    highHeight != null ||
    b.wave_period_avg != null ||
    highPeriod != null;

  if (!hasDetailData) return "";

  let html = `<p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">📊 Additional Metrics</p>`;

  // --- Heights (sig is shown on the compact card above) ---
  if (b.wave_height_avg != null) {
    html += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Height:</b> ${b.wave_height_avg.toFixed(heightPrecision)} m</p>`;
  }

  if (highHeight != null) {
    const sigHeight = b.wave_height_sig || 0;
    const ratio = sigHeight > 0 ? (highHeight / sigHeight).toFixed(1) : "";
    const ratioText = ratio
      ? ` <span style="color: var(--color-text-muted); font-size: 0.9em;">(${ratio}× sig)</span>`
      : "";
    html += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;${highHeightLabel}:</b> ${highHeight.toFixed(heightPrecision)} m${ratioText}</p>`;
  }

  // --- Periods (sig is shown on the compact card above) ---
  if (b.wave_period_avg != null) {
    html += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Period:</b> ${b.wave_period_avg.toFixed(1)} s</p>`;
  }

  if (highPeriod != null) {
    html += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;${highPeriodLabel}:</b> ${highPeriod.toFixed(1)} s</p>`;
  }

  html += buildSpreadSection(b, id);
  return html;
}

/**
 * Temperatures and pressure (all stations). Pile-station sensors report
 * excess precision, so their temps are rounded to one decimal.
 */
export function buildTempPressure(b, meta) {
  const isPile = isPileStation(meta);
  const seaTemp = b.sea_temp != null ? (isPile ? b.sea_temp.toFixed(1) : b.sea_temp) : "—";
  const airTemp = b.air_temp != null ? (isPile ? b.air_temp.toFixed(1) : b.air_temp) : "—";

  return `
        <p class="buoy-metric" style="margin-top: 0.75rem;"><b>🌡️ Sea:</b> ${seaTemp} °C | <b>Air:</b> ${airTemp} °C</p>
        <p class="buoy-metric"><b>⏱️ Pressure:</b> ${b.pressure ?? "—"} hPa</p>
      `;
}

/** The whole hidden-by-default details section. */
export function buildDetailsSection(b, id, meta, freshness) {
  let html = `<div id="card-details-${id}" style="display: none; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid var(--color-border);">`;
  html += buildStalenessCallout(freshness);
  html += isNoaaStation(meta) ? buildNoaaWaveDetails(b) : buildEcWaveDetails(b, meta, id);
  html += buildTempPressure(b, meta);
  html += `</div>`;
  return html;
}

/** "View Location" / "View Charts" buttons; charts is disabled with no data. */
export function buildNavLinks(b) {
  const hasChartData = b.wave_height_sig != null || b.wind_speed != null;
  const chartButtonDisabled = !hasChartData
    ? 'disabled style="opacity: 0.5; cursor: not-allowed;"'
    : "";

  return `
        <div class="buoy-nav-links" style="display: flex; gap: 0.5rem; margin-top: 0.75rem;">
          <button class="buoy-nav-link" data-action="map" style="
            flex: 1;
            padding: 0.5rem;
            background: #004b7c;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 600;
            transition: background 0.2s;
          ">
            📍 View Location
          </button>
          <button class="buoy-nav-link" data-action="charts" ${chartButtonDisabled} style="
            flex: 1;
            padding: 0.5rem;
            background: #004b7c;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 600;
            transition: background 0.2s;
          ">
            📊 View Charts
          </button>
        </div>
      `;
}

/**
 * Full inner HTML for one buoy card.
 *
 * @param {Object} b - Snapshot entry from latest_buoy_v2.json
 * @param {string} id - Station ID
 * @param {Object} meta - stations.json entry (undefined → EC-style defaults)
 * @param {Function} formatTimestamp - Formatter for the "Last Update" line
 * @returns {string}
 */
export function buildBuoyCardHTML(b, id, meta, formatTimestamp) {
  // No data in DB at all (e.g. buoy offline and records purged)
  if (b.no_data) return buildNoDataCard(b, id, meta);

  const freshness = freshnessState(b);

  let html = buildCardHeader(b, id, meta, freshness, formatTimestamp);
  html += buildCompactView(b, meta, freshness);
  html += buildToggleButtons();
  html += buildDetailsSection(b, id, meta, freshness);
  // History section is filled in lazily by toggleCardHistory()
  html += `<div id="card-history-${id}" style="display: none; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid var(--color-border);"></div>`;
  html += buildNavLinks(b);
  html += buildSourceLink(meta);
  return html;
}

/**
 * Wire up the card's buttons. Inline handlers are stripped by DOMPurify, and
 * the toggle callbacks live in main.js, so they're passed in.
 *
 * @param {HTMLElement} card
 * @param {string} id - Station ID, passed to every handler
 * @param {Object} handlers - {onDetails, onHistory, onSpreadInfo, onMap, onCharts}
 */
export function wireBuoyCardEvents(card, id, handlers) {
  const bindings = [
    [".toggle-details-btn", handlers.onDetails],
    [".toggle-history-btn", handlers.onHistory],
    [".spread-info-btn", handlers.onSpreadInfo],
    ['.buoy-nav-link[data-action="map"]', handlers.onMap],
    ['.buoy-nav-link[data-action="charts"]', handlers.onCharts],
  ];

  for (const [selector, handler] of bindings) {
    const el = card.querySelector(selector);
    if (el && handler) el.addEventListener("click", () => handler(id));
  }
}
