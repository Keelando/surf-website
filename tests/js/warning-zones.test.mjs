import assert from "node:assert/strict";
import { test } from "node:test";
import {
  collectActiveWarnings,
  DEFAULT_BANNER_ZONES,
  getBannerZones,
  getWarningIcon,
  getWarningId,
  getWarningSeverityClass,
} from "../../site/assets/js/shared/warning-zones.js";

const SOG_SOUTH = "strait_of_georgia_south_of_nanaimo";
const SOG_NORTH = "strait_of_georgia_north_of_nanaimo";

/** Build a marine_forecast.json-shaped document. */
function doc(areas) {
  return { generated_utc: "2026-08-20T22:00:00Z", areas };
}

function zone(zoneName, warnings) {
  return { zone_name: zoneName, warnings };
}

const GALE = {
  type: "Gale warning",
  status: "IN EFFECT",
  issued_utc: "2026-08-20T17:30:00Z",
};
const STORM = {
  type: "Storm warning",
  status: "IN EFFECT",
  issued_utc: "2026-08-20T17:30:00Z",
};

test("home-water zones raise warnings", () => {
  const data = doc({
    strait_of_georgia: {
      area: "Strait of Georgia",
      locations: { [SOG_SOUTH]: zone("south of Nanaimo", [GALE]) },
    },
  });

  const warnings = collectActiveWarnings(data);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].zone_key, SOG_SOUTH);
  assert.equal(warnings[0].zone_name, "south of Nanaimo");
  assert.equal(warnings[0].area_name, "Strait of Georgia");
});

test("sub-storm warnings outside the banner set are carried but never interrupt", () => {
  // The regression this guards: widening the sr3 accept regex adds zones to
  // marine_forecast.json, and the banner is on every page. A WCVI South gale
  // must not pop a banner over the tide tables.
  const data = doc({
    strait_of_georgia: {
      area: "Strait of Georgia",
      locations: { [SOG_SOUTH]: zone("south of Nanaimo", []) },
    },
    west_coast_vancouver_island_south: {
      area: "West Coast Vancouver Island South",
      locations: {
        west_coast_vancouver_island_south: zone("WCVI South", [GALE]),
      },
    },
    juan_de_fuca_strait: {
      area: "Juan de Fuca Strait",
      locations: {
        juan_de_fuca_strait_east_entrance: zone("east entrance", [
          { ...GALE, type: "Strong wind warning" },
        ]),
      },
    },
  });

  assert.deepEqual(collectActiveWarnings(data), []);
});

test("a storm warning banners from any carried zone, selected or not", () => {
  // The severity floor. A reader who never found the picker still gets
  // interrupted by 48+ kt, wherever it is.
  const data = doc({
    west_coast_vancouver_island_south: {
      area: "West Coast Vancouver Island South",
      locations: {
        west_coast_vancouver_island_south: zone("WCVI South", [STORM]),
      },
    },
  });

  const warnings = collectActiveWarnings(data);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].zone_key, "west_coast_vancouver_island_south");
});

test("the storm floor survives a reader turning every zone off", () => {
  // This is what makes the zero-zone state safe to allow silently.
  const data = doc({
    juan_de_fuca_strait: {
      area: "Juan de Fuca Strait",
      locations: {
        juan_de_fuca_strait_east_entrance: zone("east entrance", [STORM, GALE]),
      },
    },
  });

  const types = collectActiveWarnings(data, []).map((w) => w.type);
  assert.deepEqual(types, ["Storm warning"]);
});

test("the storm floor still respects IN EFFECT", () => {
  const data = doc({
    west_coast_vancouver_island_south: {
      area: "WCVI South",
      locations: {
        west_coast_vancouver_island_south: zone("WCVI South", [
          { ...STORM, status: "ENDED" },
        ]),
      },
    },
  });

  assert.deepEqual(collectActiveWarnings(data, []), []);
});

test("only IN EFFECT warnings count", () => {
  const data = doc({
    strait_of_georgia: {
      area: "Strait of Georgia",
      locations: {
        [SOG_SOUTH]: zone("south of Nanaimo", [
          { ...GALE, status: "ENDED" },
          { ...STORM, status: "IN EFFECT" },
        ]),
      },
    },
  });

  const warnings = collectActiveWarnings(data);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].type, "Storm warning");
});

test("warnings sort most severe first across zones", () => {
  const data = doc({
    strait_of_georgia: {
      area: "Strait of Georgia",
      locations: {
        [SOG_SOUTH]: zone("south of Nanaimo", [GALE]),
        [SOG_NORTH]: zone("north of Nanaimo", [STORM]),
      },
    },
  });

  const types = collectActiveWarnings(data).map((w) => w.type);
  assert.deepEqual(types, ["Storm warning", "Gale warning"]);
});

test("an unrecognised warning type sorts last but is never dropped", () => {
  const odd = { type: "Waterspout warning", status: "IN EFFECT" };
  const data = doc({
    strait_of_georgia: {
      area: "Strait of Georgia",
      locations: {
        [SOG_SOUTH]: zone("south of Nanaimo", [odd, GALE]),
      },
    },
  });

  const types = collectActiveWarnings(data).map((w) => w.type);
  assert.deepEqual(types, ["Gale warning", "Waterspout warning"]);
});

test("zone_name falls back to the warning's own location", () => {
  const data = doc({
    strait_of_georgia: {
      area: "Strait of Georgia",
      locations: {
        [SOG_SOUTH]: {
          warnings: [{ ...GALE, location: "south of Nanaimo" }],
        },
      },
    },
  });

  assert.equal(collectActiveWarnings(data)[0].zone_name, "south of Nanaimo");
});

test("malformed documents yield no warnings rather than throwing", () => {
  assert.deepEqual(collectActiveWarnings(null), []);
  assert.deepEqual(collectActiveWarnings({}), []);
  assert.deepEqual(collectActiveWarnings(doc({})), []);
  assert.deepEqual(
    collectActiveWarnings(doc({ strait_of_georgia: { area: "SoG" } })),
    [],
  );
  assert.deepEqual(
    collectActiveWarnings(
      doc({
        strait_of_georgia: {
          area: "SoG",
          locations: { [SOG_SOUTH]: { warnings: "not an array" } },
        },
      }),
    ),
    [],
  );
});

test("the banner zone set is overridable, ready for the per-user opt-in", () => {
  const data = doc({
    juan_de_fuca_strait: {
      area: "Juan de Fuca Strait",
      locations: { juan_de_fuca_strait_east_entrance: zone("east", [GALE]) },
    },
  });

  assert.deepEqual(collectActiveWarnings(data), []);
  assert.equal(
    collectActiveWarnings(data, ["juan_de_fuca_strait_east_entrance"]).length,
    1,
  );
});

test("default banner zones are the two home waters", () => {
  assert.deepEqual(getBannerZones(), DEFAULT_BANNER_ZONES);
  assert.deepEqual(DEFAULT_BANNER_ZONES, [SOG_NORTH, SOG_SOUTH]);
});

test("a never-chosen selection resolves to the default", () => {
  assert.deepEqual(getBannerZones(null), DEFAULT_BANNER_ZONES);
  assert.deepEqual(getBannerZones(undefined), DEFAULT_BANNER_ZONES);
});

test("a stored selection replaces the default outright", () => {
  assert.deepEqual(getBannerZones(["howe_sound"]), ["howe_sound"]);
});

test("an empty stored selection is a real choice, not a fallback to the default", () => {
  assert.deepEqual(getBannerZones([]), []);
});

test("stored zones missing from the document are dropped from the effective set", () => {
  // An sr3 accept change or an EC rename can retire a zone out from under a
  // stored preference. Dropped on the way out — never rewritten in storage,
  // so a bulletin missing for one cycle does not erase the choice.
  assert.deepEqual(getBannerZones(["howe_sound", "gone_zone"], ["howe_sound", SOG_SOUTH]), [
    "howe_sound",
  ]);
});

test("the default is filtered against the document too", () => {
  assert.deepEqual(getBannerZones(null, [SOG_SOUTH]), [SOG_SOUTH]);
});

test("getBannerZones returns a copy, never the shared default array", () => {
  const resolved = getBannerZones();
  resolved.push("howe_sound");
  assert.deepEqual(DEFAULT_BANNER_ZONES, [SOG_NORTH, SOG_SOUTH]);
});

test("warning id includes issue time so a re-issue is not still dismissed", () => {
  const first = getWarningId({ zone_key: SOG_SOUTH, ...GALE });
  const reissued = getWarningId({
    zone_key: SOG_SOUTH,
    ...GALE,
    issued_utc: "2026-08-21T04:30:00Z",
  });

  assert.notEqual(first, reissued);
  assert.match(first, /^strait_of_georgia_south_of_nanaimo_Gale warning_/);
});

test("warning id tolerates a missing issue time", () => {
  assert.equal(
    getWarningId({ zone_key: SOG_SOUTH, type: "Gale warning" }),
    "strait_of_georgia_south_of_nanaimo_Gale warning_unknown",
  );
});

test("severity classes match warning types", () => {
  assert.equal(getWarningSeverityClass("Storm warning"), "warning-storm");
  assert.equal(getWarningSeverityClass("Gale warning"), "warning-gale");
  assert.equal(getWarningSeverityClass("Strong wind warning"), "warning-strong-wind");
  assert.equal(getWarningSeverityClass("Wind warning"), "warning-strong-wind");
  assert.equal(getWarningSeverityClass("Freezing spray"), "warning-default");
  assert.equal(getWarningSeverityClass(undefined), "warning-default");
});

test("icons match warning types", () => {
  assert.equal(getWarningIcon("Storm warning"), "⚠️");
  assert.equal(getWarningIcon("Gale warning"), "💨");
  assert.equal(getWarningIcon("Strong wind warning"), "🌬️");
  assert.equal(getWarningIcon("Anything else"), "⚠️");
});
