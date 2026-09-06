(function (window) {
  function escapeHtml(html) {
    const div = document.createElement("div");
    div.textContent = html ?? "";
    return div.innerHTML;
  }

  // `target` is NOT in DOMPurify's default allow-list, so every link this site
  // renders through the sanitizer lost it silently: the buoy cards' "View
  // Source Data", the forecast zone "View source", the lightstation card link
  // — all of them announce an external destination with a ↗ and an aria-label
  // saying "opens in a new tab", and all of them were opening in the same tab.
  // Found 2026-09-06 while adding the lightstation card link (DOMPurify 3.0.9).
  //
  // Allowing `target` back is only safe with the reverse-tabnabbing guard, so
  // the hook below is not optional decoration: a `target="_blank"` link gives
  // the opened page a `window.opener` handle back to ours unless `rel` says
  // otherwise. The hook stamps `rel` on every such link rather than trusting
  // each call site to remember, which is the whole reason it lives here and
  // not in the callers.
  let hookInstalled = false;

  function installTargetRelHook() {
    if (hookInstalled || !window.DOMPurify || typeof window.DOMPurify.addHook !== "function") {
      return;
    }
    window.DOMPurify.addHook("afterSanitizeAttributes", (node) => {
      if (node.tagName === "A" && node.hasAttribute("target")) {
        node.setAttribute("rel", "noopener noreferrer");
      }
    });
    hookInstalled = true;
  }

  function sanitizeHtml(html, options = {}) {
    if (window.DOMPurify && typeof window.DOMPurify.sanitize === "function") {
      installTargetRelHook();
      // Caller-supplied ADD_ATTR wins, but still gets `target` — a call site
      // asking for one extra attribute is not asking to drop this one.
      const addAttr = options.ADD_ATTR ? [...new Set([...options.ADD_ATTR, "target"])] : ["target"];
      return window.DOMPurify.sanitize(html, { ...options, ADD_ATTR: addAttr });
    }

    console.warn("DOMPurify not available; falling back to plain-text rendering");
    return escapeHtml(html);
  }

  function setSanitizedHTML(element, html, options = {}) {
    if (!element) {
      return;
    }

    element.innerHTML = sanitizeHtml(html, options);
  }

  window.sanitizeHTML = sanitizeHtml;
  window.setSanitizedHTML = setSanitizedHTML;
})(window);
