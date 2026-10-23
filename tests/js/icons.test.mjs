import assert from "node:assert/strict";
import { test } from "node:test";

import { ICON_NAMES, icon } from "../../site/assets/js/shared/icons.js";

test("every icon is a decorative currentColor line drawing", () => {
  for (const name of ICON_NAMES) {
    const svg = icon(name);
    assert.match(svg, /^<svg class="icon"/, name);
    assert.match(svg, /aria-hidden="true"/, name);
    assert.match(svg, /stroke="currentColor"/, name);
  }
});

test("a modifier class is added beside the base class", () => {
  assert.match(icon("external", "icon--trailing"), /class="icon icon--trailing"/);
});

test("an unknown name renders nothing rather than a broken shape", () => {
  assert.equal(icon("nope"), "");
});
