/**
 * Sanitized innerHTML, in one place.
 *
 * This was copy-pasted into thirteen files and had already drifted into two
 * variants (a blank line apart, but drift is drift). Consolidated 2026-08-26.
 *
 * The indirection through the global is deliberate and not laziness:
 * `sanitize-html.js` is a classic script that runs before any module and
 * publishes `setSanitizedHTML`, which wraps DOMPurify. Modules cannot import
 * a classic script, and making DOMPurify an import would mean bundling it —
 * this site has no build step. So the global is the seam.
 *
 * The fallback matters for what it does NOT do: if `setSanitizedHTML` is
 * missing, this assigns the markup unsanitized rather than dropping it. That
 * is the historical behaviour of all thirteen copies and is preserved here
 * on purpose — every caller passes markup this repo built from its own JSON
 * exports, not from user input, so a page that renders is better than a page
 * that silently blanks. Anything that ever renders third-party text must
 * sanitize at its own call site instead of relying on this.
 *
 * @param {Element|null} element - Target; a null element is a no-op, which is
 *   what lets callers skip a `getElementById` guard on optional markup.
 * @param {string} html
 * @returns {void}
 */
export function setSafeHTML(element, html) {
  if (!element) return;

  // globalThis rather than a bare `window` so this is importable under node
  // for the unit tests; in a browser the two are the same object.
  const sanitized = globalThis.setSanitizedHTML;
  if (typeof sanitized === "function") {
    sanitized(element, html);
  } else {
    element.innerHTML = html;
  }
}
