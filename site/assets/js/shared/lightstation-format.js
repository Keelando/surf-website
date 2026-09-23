/**
 * Lightstation reading formatters (ES module).
 *
 * One wording for a lightkeeper's wind report, shared by the station cards
 * (lightstation-page.js) and both map popups (lightstation-map.js,
 * stations-map.js). The three used to build it inline as
 * `${dir || "N/A"} ${speed || "N/A"} kt`, which printed a 0 kt reading as
 * "N/A" and an empty one as "N/A N/A kt".
 */

/**
 * "SOUTHEAST 12 kt (gusting) (est)", "Calm", or null when the report states
 * no wind at all (e.g. "PINE ISLAND. VISIBILITY ZERO.").
 *
 * @param {Object} obs - Entry from latest_lightstation.json
 * @returns {string|null}
 */
export function formatLightstationWind(obs) {
  if (!obs) return null;
  if (obs.wind_calm) return "Calm";
  const hasSpeed = obs.wind_speed_kt !== null && obs.wind_speed_kt !== undefined;
  if (!hasSpeed && !obs.wind_direction) return null;
  const speed = hasSpeed ? `${Math.round(obs.wind_speed_kt)} kt` : "speed not given";
  const parts = [obs.wind_direction, speed].filter(Boolean).join(" ");
  return `${parts}${obs.wind_gusting ? " (gusting)" : ""}${obs.wind_estimated ? " (est)" : ""}`;
}
