(function () {
  const STORAGE_KEY = "theme-preference";
  const Theme = {
    LIGHT: "light",
    DARK: "dark",
  };

  const html = document.documentElement;

  // TRIAL ONLY, must not ship (docs/project/PAGE_BACKGROUND.md):
  // ?bg=gradient|chart|contours picks a page background on the dev preview.
  // Nothing is remembered; remove this with the options not chosen.
  try {
    const bg = new URLSearchParams(window.location.search).get("bg");
    if (bg === "gradient" || bg === "chart" || bg === "contours") {
      html.dataset.bg = bg;
    }
  } catch (error) {
    // No URL API: no trial background.
  }
  const THEME_COLORS = {
    light: "#eef4f8",
    dark: "#0d1b2a",
  };

  let appliedTheme = null;
  let appliedPreference = null;

  function safeGetPreference() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored === Theme.LIGHT || stored === Theme.DARK) {
        return stored;
      }
      return Theme.LIGHT;
    } catch (error) {
      return Theme.LIGHT;
    }
  }

  function safeSetPreference(preference) {
    try {
      localStorage.setItem(STORAGE_KEY, preference);
    } catch (error) {
      // Ignore storage errors (Safari private mode, etc.)
    }
  }

  function ensureThemeColorMeta() {
    let meta = document.querySelector('meta[name="theme-color"]');
    if (!meta) {
      meta = document.createElement("meta");
      meta.setAttribute("name", "theme-color");
      document.head.appendChild(meta);
    }
    return meta;
  }

  function updateMetaThemeColor(theme) {
    const meta = ensureThemeColorMeta();
    const color = THEME_COLORS[theme] || THEME_COLORS.light;
    meta.setAttribute("content", color);
  }

  function dispatchThemeChange(theme, preference) {
    if (typeof window.CustomEvent !== "function") return;
    window.dispatchEvent(
      new CustomEvent("themechange", {
        detail: { theme, preference },
      }),
    );
  }

  function applyTheme(theme, preference) {
    if (theme === appliedTheme && preference === appliedPreference) {
      return;
    }

    appliedTheme = theme;
    appliedPreference = preference;

    html.setAttribute("data-theme", theme);
    html.setAttribute("data-theme-preference", preference);
    updateMetaThemeColor(theme);
    dispatchThemeChange(theme, preference);
  }

  function setPreference(preference) {
    const normalized = preference === Theme.DARK ? Theme.DARK : Theme.LIGHT;
    safeSetPreference(normalized);
    applyTheme(normalized, normalized);
  }

  function cyclePreference() {
    const current = safeGetPreference();
    const next = current === Theme.DARK ? Theme.LIGHT : Theme.DARK;
    setPreference(next);
  }

  function init() {
    const preference = safeGetPreference();
    applyTheme(preference, preference);
  }

  init();

  window.ThemeManager = {
    getPreference: () => safeGetPreference(),
    setPreference,
    cycle: cyclePreference,
    getState: () => ({
      theme: appliedTheme || safeGetPreference(),
      preference: appliedPreference || safeGetPreference(),
    }),
  };
})();
