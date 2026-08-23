import assert from "node:assert/strict";
import { test } from "node:test";
import {
  BANNER_ZONES_STORAGE_KEY,
  readBannerZones,
  writeBannerZones,
} from "../../site/assets/js/shared/warning-preferences.js";

/** Minimal Storage stand-in. */
function fakeStorage(initial = {}) {
  const map = new Map(Object.entries(initial));
  return {
    getItem: (key) => (map.has(key) ? map.get(key) : null),
    setItem: (key, value) => map.set(key, String(value)),
    map,
  };
}

/** Storage that throws on every access — private browsing, disabled site data. */
function hostileStorage() {
  return {
    getItem() {
      throw new Error("SecurityError");
    },
    setItem() {
      throw new Error("SecurityError");
    },
  };
}

test("an absent key reads as never chosen", () => {
  assert.equal(readBannerZones(fakeStorage()), null);
});

test("a stored list round-trips through write and read", () => {
  const storage = fakeStorage();
  assert.equal(writeBannerZones(storage, ["howe_sound", "haro_strait"]), true);
  assert.deepEqual(readBannerZones(storage), ["howe_sound", "haro_strait"]);
});

test("an empty list round-trips and stays distinguishable from absent", () => {
  const storage = fakeStorage();
  writeBannerZones(storage, []);
  assert.deepEqual(readBannerZones(storage), []);
  assert.notEqual(readBannerZones(storage), null);
});

test("corrupt JSON reads as never chosen rather than throwing", () => {
  const storage = fakeStorage({ [BANNER_ZONES_STORAGE_KEY]: "{not json" });
  assert.equal(readBannerZones(storage), null);
});

test("a stored value of the wrong shape reads as never chosen", () => {
  // A comma-joined string from some earlier build, or an object: neither is a
  // selection, and treating either as an empty one would silence the banner.
  assert.equal(readBannerZones(fakeStorage({ [BANNER_ZONES_STORAGE_KEY]: '"howe_sound"' })), null);
  assert.equal(readBannerZones(fakeStorage({ [BANNER_ZONES_STORAGE_KEY]: "{}" })), null);
  assert.equal(readBannerZones(fakeStorage({ [BANNER_ZONES_STORAGE_KEY]: "null" })), null);
});

test("non-string entries are dropped from an otherwise usable list", () => {
  const storage = fakeStorage({
    [BANNER_ZONES_STORAGE_KEY]: '["howe_sound", 3, null]',
  });
  assert.deepEqual(readBannerZones(storage), ["howe_sound"]);
});

test("storage that throws on read degrades to the default, silently", () => {
  assert.equal(readBannerZones(hostileStorage()), null);
});

test("storage that throws on write reports failure rather than throwing", () => {
  assert.equal(writeBannerZones(hostileStorage(), ["howe_sound"]), false);
});

test("a Set of zone keys is accepted and stored as an array", () => {
  const storage = fakeStorage();
  writeBannerZones(storage, new Set(["howe_sound"]));
  assert.equal(storage.map.get(BANNER_ZONES_STORAGE_KEY), '["howe_sound"]');
});
