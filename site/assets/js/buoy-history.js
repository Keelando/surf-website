/* -----------------------------
   Buoy Card History Table (ES module)

   The 12-hour history table shown inside a buoy card, extracted from
   renderHistoryTable() in main.js (step 4 of
   docs/project/BUOY_CARD_REFACTOR.md). Pure builders over a station's
   timeseries object plus its stations.json metadata — never a station ID.
   main.js keeps the fetch and the toggle wiring.

   Which fields hold the displayed height/period is NOT decided here: it comes
   from waveHeightField()/wavePeriodFields() in shared/station-meta.js, the
   same priorities the compact card line reads.

   degreesToCardinal comes from the classic script chart-utils-v4.js loaded
   before the module graph, same as main.js.
   ----------------------------- */

import { formatTimeHM } from "./shared/format-time.js";
import {
  reportsSubHourly,
  usesDominantPeriod,
  usesSwellDisplay,
  waveHeightField,
  waveHeightPrecision,
  wavePeriodFields,
} from "./shared/station-meta.js";

const HISTORY_WINDOW_HOURS = 12;

/** A timeseries channel's samples, or [] when the station doesn't report it. */
function series(timeseries, field) {
  return timeseries?.[field]?.data || [];
}

/**
 * The channels the table reads, resolved for this station.
 *
 * `windWaveHeight` is empty except at stations that split the sea state into
 * swell and wind waves (the NOAA buoys' spectral feed); the table only gets
 * a Wind Wave column where it has something to show.
 *
 * @returns {{wind: Object, waveHeight: Array, windWaveHeight: Array, wavePeriods: Array, airTemp: Array, seaTemp: Array}}
 */
export function selectHistorySeries(timeseries, meta) {
  return {
    wind: {
      speed: series(timeseries, "wind_speed"),
      direction: series(timeseries, "wind_direction"),
      gust: series(timeseries, "wind_gust"),
    },
    waveHeight: series(timeseries, waveHeightField(meta)),
    windWaveHeight: series(timeseries, "wind_wave_height"),
    // Priority-ordered, same list the compact card line uses.
    wavePeriods: wavePeriodFields(meta).map(({ field }) => series(timeseries, field)),
    airTemp: series(timeseries, "air_temp"),
    seaTemp: series(timeseries, "sea_temp"),
  };
}

/**
 * Row timestamps, newest first, within the trailing 12-hour window.
 *
 * The axis comes from wave data when present, falling back to wind and then
 * temperature so the table still renders when a buoy's wave sensor is offline.
 * Sub-hourly reporters (the Crescent pile stations) are thinned to
 * on-the-hour rows so 12 hours fits the table.
 *
 * @param {Date} [now] - Injectable for tests; defaults to the current time.
 */
export function buildHistoryTimes(selected, meta, now = new Date()) {
  const { waveHeight, wind, seaTemp, airTemp } = selected;
  const timeSource = waveHeight.length
    ? waveHeight
    : wind.speed.length
      ? wind.speed
      : seaTemp.length
        ? seaTemp
        : airTemp;

  let times = timeSource.map((d) => d.time);

  if (reportsSubHourly(meta)) {
    times = times.filter((time) => new Date(time).getMinutes() === 0);
  }

  const cutoff = new Date(now.getTime() - HISTORY_WINDOW_HOURS * 60 * 60 * 1000);
  return times
    .filter((time) => new Date(time) >= cutoff)
    .sort()
    .reverse();
}

/** Value at `time` in a samples array, or undefined. */
function valueAt(samples, time) {
  return samples.find((d) => d.time === time)?.value;
}

/** Backup sensor that supplied the sample at `time`, or undefined (first choice). */
function sensorAt(samples, time) {
  return samples.find((d) => d.time === time)?.sensor;
}

/**
 * Stretches of a wave-height series that came from a backup sensor, for
 * shading on the chart: [{sensor, start, end}], each running from its first
 * backup point to the point after its last (so a lone point still spans).
 *
 * @param {Array<{time: string, sensor?: string}>} points - Time-ordered
 * @returns {Array<{sensor: string, start: string, end: string}>}
 */
export function backupSensorSpans(points) {
  const spans = [];
  points.forEach((point, i) => {
    const last = spans.at(-1);
    if (point.sensor && last?.sensor === point.sensor && last.open) {
      last.end = points[i + 1]?.time ?? point.time;
    } else if (point.sensor) {
      spans.push({
        sensor: point.sensor,
        start: point.time,
        end: points[i + 1]?.time ?? point.time,
        open: true,
      });
    } else if (last) {
      last.open = false;
    }
  });
  return spans.map(({ sensor, start, end }) => ({ sensor, start, end }));
}

/** Backup sensors named by any wave-height row shown, in order of appearance. */
function backupSensors(times, selected) {
  return [...new Set(times.map((t) => sensorAt(selected.waveHeight, t)).filter(Boolean))];
}

/** First non-null value at `time` across the priority-ordered channels. */
function firstValueAt(channels, time) {
  for (const samples of channels) {
    const v = valueAt(samples, time);
    if (v != null) return v;
  }
  return undefined;
}

/**
 * Time cell — "Mo-11<br/>08h10", or just "08h10" when the date is unchanged
 * from the previous row.
 *
 * Everything is pinned to America/Vancouver: getDay()/getHours() would use the
 * viewer's timezone and mislabel evening rows for non-Pacific visitors. Hour
 * and minute come from one formatTimeHM() call because engines only zero-pad
 * reliably when both fields are requested together.
 *
 * @returns {{html: string, date: string}} `date` feeds the next row's compare.
 */
export function formatHistoryTimeCell(dateObj, previousDate) {
  const dayOfWeek = dateObj
    .toLocaleString("en-US", { weekday: "short", timeZone: "America/Vancouver" })
    .slice(0, 2);
  const dayOfMonth = dateObj.toLocaleString("en-US", {
    day: "numeric",
    timeZone: "America/Vancouver",
  });
  const [hour, minute] = formatTimeHM(dateObj).split(":");

  const date = `${dayOfWeek}-${dayOfMonth}`;
  const clock = `${hour}h${minute !== "00" ? minute : ""}`;
  return { html: date !== previousDate ? `${date}<br/>${clock}` : clock, date };
}

/**
 * Wind cell — "WNW 10 G 15", or "—" with no reading.
 *
 * "G" rather than "gust": this column was the widest data in the table, and
 * the compact wind line on the card above already abbreviates it the same way.
 */
export function formatHistoryWind(speed, direction, gust) {
  if (speed == null) return "—";
  const cardinal = degreesToCardinal(direction);
  const cardinalStr = cardinal ? `${cardinal} ` : "";
  const gustStr = gust != null ? ` G ${Math.round(gust)}` : "";
  return `${cardinalStr}${Math.round(speed)}${gustStr}`;
}

/**
 * Header cell with the unit stacked underneath.
 *
 * Single-line headers were what forced the table wider than the card: four of
 * six columns were sized by their label rather than their data ("Wave Ht [m]"
 * needed 90px to show 20px-wide values), pushing Air off the right edge.
 */
function headerCell(label, unit) {
  return `<th>${label}<span class="history-unit">[${unit}]</span></th>`;
}

/** One decimal, or the em-dash placeholder. */
function fixedOrDash(value, decimals) {
  return value != null ? value.toFixed(decimals) : "—";
}

/** `<tr>` rows for the given timestamps. */
export function buildHistoryRows(times, selected, meta) {
  const heightDecimals = waveHeightPrecision(meta);
  const showWindWave = selected.windWaveHeight.length > 0;
  let previousDate = null;

  return times
    .map((time) => {
      const dateObj = new Date(time);
      const { html: timeCell, date } = formatHistoryTimeCell(dateObj, previousDate);
      previousDate = date;

      const wind = formatHistoryWind(
        valueAt(selected.wind.speed, time),
        valueAt(selected.wind.direction, time),
        valueAt(selected.wind.gust, time),
      );

      return `
      <tr>
        <td>${timeCell}</td>
        <td>${wind}</td>
        <td>${fixedOrDash(valueAt(selected.waveHeight, time), heightDecimals)}${sensorAt(selected.waveHeight, time) ? "*" : ""}</td>
        ${showWindWave ? `<td>${fixedOrDash(valueAt(selected.windWaveHeight, time), heightDecimals)}</td>` : ""}
        <td>${fixedOrDash(firstValueAt(selected.wavePeriods, time), 1)}</td>
        <td>${fixedOrDash(valueAt(selected.seaTemp, time), 1)}</td>
        <td>${fixedOrDash(valueAt(selected.airTemp, time), 1)}</td>
      </tr>
    `;
    })
    .join("");
}

/**
 * Explanatory note under the table for stations whose period column isn't a
 * plain significant period. Returns "" when no note applies.
 */
export function buildHistoryNote(meta) {
  if (usesSwellDisplay(meta)) {
    return `
      <div class="history-note">
        <strong>Note:</strong> Neah Bay's Wave Ht and Period are <strong>swell</strong> (long-period ocean waves from distant storms) rather than combined wave metrics. Wind Wave is the locally generated chop, typically much smaller here.
      </div>
    `;
  }
  if (usesDominantPeriod(meta)) {
    return `
      <div class="history-note">
        <strong>Note:</strong> Wave Ht is significant wave height, and Wind Wave the part of it raised by local wind; period is the <strong>dominant period</strong>, NOAA's term for the wave period carrying the most energy (equivalent to peak period).
      </div>
    `;
  }
  return "";
}

/**
 * Full history-table HTML for one station.
 *
 * @param {Object} timeseries - Station entry from buoy_timeseries_48h.json
 * @param {Object} meta - stations.json entry (undefined → EC-style defaults)
 * @param {Object} [options]
 * @param {Date} [options.now] - Injectable clock for tests
 * @returns {string}
 */
export function buildHistoryTableHTML(timeseries, meta, options = {}) {
  const { now = new Date() } = options;

  const selected = selectHistorySeries(timeseries, meta);
  const times = buildHistoryTimes(selected, meta, now);

  // Emitted hidden and revealed by trackScrollAffordance() once the table is
  // in the DOM — whether it overflows can only be measured after layout.
  const scrollHint = `<div class="history-scroll-hint" hidden>← Scroll table horizontally →</div>`;

  return `
    ${scrollHint}
    <div class="history-scroll">
      <table class="history-table">
        <thead>
          <tr>
            <th>Time</th>
            ${headerCell("Wind", "kn")}
            ${headerCell("Wave Ht", "m")}
            ${selected.windWaveHeight.length > 0 ? headerCell("Wind Wave", "m") : ""}
            ${headerCell("Period", "s")}
            ${headerCell("Sea", "°C")}
            ${headerCell("Air", "°C")}
          </tr>
        </thead>
        <tbody>
    ${buildHistoryRows(times, selected, meta)}
        </tbody>
      </table>
    </div>

    <div class="history-hide-row">
      <button class="hide-history-btn">
        ▲ Hide History
      </button>
    </div>
  ${buildBackupSensorNote(backupSensors(times, selected))}
  ${buildHistoryNote(meta)}`;
}

/**
 * Note for rows whose waves came from a backup sensor, e.g. the radar at
 * Crescent Beach Ocean while its Anderaa has no reading. "" when none did.
 */
export function buildBackupSensorNote(sensors) {
  if (sensors.length === 0) return "";
  return `
      <div class="history-note">
        <strong>*</strong> From the station's backup wave sensor (${sensors.join(", ")}): its main sensor had no reading. Sensors read slightly differently, so a step where the source changes may not be the sea.
      </div>
    `;
}
