import assert from "node:assert/strict";
import { test } from "node:test";

import { setSafeHTML } from "../../site/assets/js/shared/safe-html.js";

/** Minimal stand-in for an element: only innerHTML is ever touched. */
function el() {
  return { innerHTML: "untouched" };
}

/** Run `fn` with `globalThis.setSanitizedHTML` set to `impl`, then restore. */
function withSanitizer(impl, fn) {
  const had = Object.hasOwn(globalThis, "setSanitizedHTML");
  const prev = globalThis.setSanitizedHTML;
  if (impl === undefined) delete globalThis.setSanitizedHTML;
  else globalThis.setSanitizedHTML = impl;
  try {
    fn();
  } finally {
    if (had) globalThis.setSanitizedHTML = prev;
    else delete globalThis.setSanitizedHTML;
  }
}

test("delegates to the global sanitizer when one is published", () => {
  const calls = [];
  const target = el();
  withSanitizer(
    (element, html) => {
      calls.push([element, html]);
      element.innerHTML = "sanitized";
    },
    () => setSafeHTML(target, "<b>hi</b>"),
  );
  assert.deepEqual(calls, [[target, "<b>hi</b>"]]);
  assert.equal(target.innerHTML, "sanitized");
});

test("falls back to assigning the markup when DOMPurify never loaded", () => {
  // Deliberate: the historical behaviour of all thirteen copied versions was
  // to render rather than blank the panel. Every caller passes markup built
  // from this repo's own exports, so a missing sanitizer must not empty the
  // page. Anything rendering third-party text sanitizes at its call site.
  const target = el();
  withSanitizer(undefined, () => setSafeHTML(target, "<b>hi</b>"));
  assert.equal(target.innerHTML, "<b>hi</b>");
});

test("ignores a non-function global rather than throwing", () => {
  const target = el();
  withSanitizer("not a function", () => setSafeHTML(target, "<i>x</i>"));
  assert.equal(target.innerHTML, "<i>x</i>");
});

test("a null element is a no-op, so callers can skip the guard", () => {
  withSanitizer(
    () => assert.fail("sanitizer must not be called for a null element"),
    () => {
      setSafeHTML(null, "<b>hi</b>");
      setSafeHTML(undefined, "<b>hi</b>");
    },
  );
});
