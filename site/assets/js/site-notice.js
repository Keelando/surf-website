(async function () {
  const STORAGE_KEY = "dismissed_site_notice";

  async function loadNotice() {
    try {
      const res = await fetch(`/data/notice.json?t=${Date.now()}`);
      if (!res.ok) return null;
      const data = await res.json();
      if (!data.message || !data.expires) return null;
      return data;
    } catch {
      return null;
    }
  }

  function noticeId(notice) {
    return `${notice.expires}|${notice.message}`;
  }

  function isDismissed(id) {
    try {
      return localStorage.getItem(STORAGE_KEY) === id;
    } catch {
      return false;
    }
  }

  function dismiss(id, container) {
    try {
      localStorage.setItem(STORAGE_KEY, id);
    } catch {
      /* localStorage unavailable (private mode, quota, disabled) — dismissal is best-effort */
    }
    container.style.transition = "opacity 0.3s ease";
    container.style.opacity = "0";
    setTimeout(() => {
      container.style.display = "none";
    }, 300);
  }

  const notice = await loadNotice();
  if (!notice) return;

  if (Date.now() > new Date(notice.expires).getTime()) return;

  const id = noticeId(notice);
  if (isDismissed(id)) return;

  const container = document.getElementById("site-notice-container");
  if (!container) return;

  const div = document.createElement("div");
  div.className = "site-notice";

  const icon = document.createElement("span");
  icon.className = "site-notice-icon";
  icon.setAttribute("aria-hidden", "true");
  icon.textContent = "ℹ";

  const msg = document.createElement("span");
  msg.className = "site-notice-message";
  msg.textContent = notice.message;

  const btn = document.createElement("button");
  btn.className = "site-notice-dismiss";
  btn.setAttribute("aria-label", "Dismiss notice");
  btn.textContent = "×";
  btn.addEventListener("click", () => dismiss(id, container));

  div.appendChild(icon);
  div.appendChild(msg);
  div.appendChild(btn);
  container.appendChild(div);
})();
