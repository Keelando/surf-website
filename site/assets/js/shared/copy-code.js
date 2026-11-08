/**
 * Copy buttons for code on the page (user 2026-11-08, API page first).
 *
 * Every `pre > code` inside `root`, and any element marked `data-copy`, gets
 * a small "Copy" button that puts its text on the clipboard and says
 * "Copied" for two seconds. The block is wrapped so the button sits in its
 * corner without scrolling away with a long line: the <pre> scrolls
 * sideways, the wrapper doesn't.
 *
 * No clipboard (an insecure context, an old browser): no button, rather than
 * one that fails when pressed.
 */

const COPIED_MS = 2000;

/**
 * @param {HTMLElement} target - The element whose text is copied
 * @param {HTMLElement} host - Where the button goes (positioned)
 * @param {HTMLElement} status - Live region that announces the copy
 */
function addButton(target, host, status) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy-code-btn";
  button.textContent = "Copy";
  button.setAttribute("aria-label", "Copy to clipboard");

  let timer = null;
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(target.textContent.trim());
      button.textContent = "Copied";
      button.classList.add("is-copied");
      status.textContent = "Copied to clipboard";
    } catch {
      button.textContent = "Couldn't copy";
      status.textContent = "Couldn't copy to the clipboard";
    }
    clearTimeout(timer);
    timer = setTimeout(() => {
      button.textContent = "Copy";
      button.classList.remove("is-copied");
      status.textContent = "";
    }, COPIED_MS);
  });

  host.appendChild(button);
}

/**
 * Add copy buttons inside `root`.
 *
 * @param {ParentNode} [root=document]
 * @returns {void}
 */
export function addCopyButtons(root = document) {
  if (!root || !navigator.clipboard?.writeText) return;

  // One polite live region for the page, so a screen reader hears the result.
  let status = document.getElementById("copy-code-status");
  if (!status) {
    status = document.createElement("p");
    status.id = "copy-code-status";
    status.className = "visually-hidden";
    status.setAttribute("aria-live", "polite");
    document.body.appendChild(status);
  }

  for (const code of root.querySelectorAll("pre > code")) {
    const pre = code.parentElement;
    if (pre.parentElement?.classList.contains("copy-code")) continue;
    const wrap = document.createElement("div");
    wrap.className = "copy-code";
    pre.replaceWith(wrap);
    wrap.appendChild(pre);
    addButton(code, wrap, status);
  }

  for (const el of root.querySelectorAll("[data-copy]")) {
    if (el.querySelector(".copy-code-btn")) continue;
    el.classList.add("copy-code-inline");
    addButton(el.querySelector("code") ?? el, el, status);
  }
}
