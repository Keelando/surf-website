(function(window) {
  function escapeHtml(html) {
    const div = document.createElement('div');
    div.textContent = html ?? '';
    return div.innerHTML;
  }

  function sanitizeHtml(html, options = {}) {
    if (window.DOMPurify && typeof window.DOMPurify.sanitize === 'function') {
      return window.DOMPurify.sanitize(html, options);
    }

    console.warn('DOMPurify not available; falling back to plain-text rendering');
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
