import assert from "node:assert/strict";
import { test } from "node:test";
import {
  DEFAULT_ZONE_KEY,
  listZones,
  orderZonesForDisplay,
  shortZoneLabel,
} from "../../site/assets/js/shared/marine-zones.js";

const SOG_SOUTH = "strait_of_georgia_south_of_nanaimo";
const SOG_NORTH = "strait_of_georgia_north_of_nanaimo";

/** A document with Juan de Fuca listed first, so home-area pinning is visible. */
function doc() {
  return {
    areas: {
      juan_de_fuca_strait: {
        area: "Juan de Fuca Strait",
        locations: {
          juan_de_fuca_strait_east_entrance: {
            zone_name: "Juan de Fuca Strait - east entrance",
          },
          juan_de_fuca_strait_west_entrance: {
            zone_name: "Juan de Fuca Strait - west entrance",
          },
        },
      },
      strait_of_georgia: {
        area: "Strait of Georgia",
        locations: {
          [SOG_NORTH]: { zone_name: "Strait of Georgia - north of Nanaimo" },
          [SOG_SOUTH]: { zone_name: "Strait of Georgia - south of Nanaimo" },
        },
      },
      howe_sound: {
        area: "Howe Sound",
        locations: { howe_sound: { zone_name: "Howe Sound" } },
      },
    },
  };
}

test("listZones flattens areas into a zone list", () => {
  const zones = listZones(doc());
  assert.equal(zones.length, 5);

  const sog = zones.find((z) => z.zoneKey === SOG_SOUTH);
  assert.equal(sog.zoneName, "Strait of Georgia - south of Nanaimo");
  assert.equal(sog.areaKey, "strait_of_georgia");
  assert.equal(sog.areaName, "Strait of Georgia");
});

test("listZones falls back to de-slugged keys when names are missing", () => {
  const zones = listZones({
    areas: { some_area: { locations: { some_zone: {} } } },
  });
  assert.equal(zones[0].zoneName, "some zone");
  assert.equal(zones[0].areaName, "some area");
});

test("listZones tolerates malformed documents", () => {
  assert.deepEqual(listZones(null), []);
  assert.deepEqual(listZones({}), []);
  assert.deepEqual(listZones({ areas: { a: {} } }), []);
});

test("shortZoneLabel drops the repeated area prefix", () => {
  assert.equal(
    shortZoneLabel("Juan de Fuca Strait - west entrance", "Juan de Fuca Strait"),
    "West entrance",
  );
});

test("shortZoneLabel leaves unfamiliar shapes alone", () => {
  assert.equal(shortZoneLabel("Howe Sound", "Howe Sound"), "Howe Sound");
  assert.equal(shortZoneLabel("Howe Sound", ""), "Howe Sound");
  assert.equal(shortZoneLabel("Haro Strait", "Strait of Georgia"), "Haro Strait");
});

test("orderZonesForDisplay pins the home area first, then document order", () => {
  const groups = orderZonesForDisplay(listZones(doc()));
  assert.deepEqual(
    groups.map((g) => g.areaName),
    ["Strait of Georgia", "Juan de Fuca Strait", "Howe Sound"],
  );
  assert.deepEqual(
    groups[0].zones.map((z) => z.zoneKey),
    [SOG_NORTH, SOG_SOUTH],
  );
});

test("orderZonesForDisplay keeps document order when home waters are absent", () => {
  const data = doc();
  delete data.areas.strait_of_georgia;
  const groups = orderZonesForDisplay(listZones(data));
  assert.deepEqual(
    groups.map((g) => g.areaName),
    ["Juan de Fuca Strait", "Howe Sound"],
  );
});

test("the default zone is the one Halibut Bank sits in", () => {
  assert.equal(DEFAULT_ZONE_KEY, SOG_SOUTH);
});
