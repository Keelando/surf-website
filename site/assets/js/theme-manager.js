(function () {
  const DARK_MODE_ENABLED = false;
  const STORAGE_KEY = "theme-preference";
  const Theme = {
    SYSTEM: "system",
    LIGHT: "light",
    DARK: "dark",
  };

  const html = document.documentElement;
  const THEME_COLORS = {
    light: "#eef4f8",
    dark: "#0d1b2a",
  };

  let appliedTheme = null;
  let appliedPreference = null;

  function safeGetPreference() {
    if (!DARK_MODE_ENABLED) {
      return Theme.LIGHT;
    }
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored === Theme.LIGHT || stored === Theme.DARK) {
        return stored;
      }
      return Theme.SYSTEM;
    } catch (error) {
      return Theme.SYSTEM;
    }
  }

  function safeSetPreference(preference) {
    if (!DARK_MODE_ENABLED) {
      return;
    }
    try {
      if (preference === Theme.SYSTEM) {
        localStorage.removeItem(STORAGE_KEY);
      } else {
        localStorage.setItem(STORAGE_KEY, preference);
      }
    } catch (error) {
      // Ignore storage errors (Safari private mode, etc.)
    }
  }

  function getSystemTheme() {
    return window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: dark)").matches
      ? Theme.DARK
      : Theme.LIGHT;
  }

  function resolveTheme(preference) {
    if (!DARK_MODE_ENABLED) {
      return Theme.LIGHT;
    }
    if (preference === Theme.LIGHT) return Theme.LIGHT;
    if (preference === Theme.DARK) return Theme.DARK;
    return getSystemTheme();
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
    if (!DARK_MODE_ENABLED) {
      applyTheme(Theme.LIGHT, Theme.LIGHT);
      return;
    }
    const normalized =
      preference === Theme.LIGHT || preference === Theme.DARK
        ? preference
        : Theme.SYSTEM;
    safeSetPreference(normalized);
    applyTheme(resolveTheme(normalized), normalized);
  }

  function cyclePreference() {
    if (!DARK_MODE_ENABLED) {
      applyTheme(Theme.LIGHT, Theme.LIGHT);
      return;
    }
    const current = safeGetPreference();
    const next =
      current === Theme.SYSTEM
        ? Theme.LIGHT
        : current === Theme.LIGHT
          ? Theme.DARK
          : Theme.SYSTEM;
    setPreference(next);
  }

  function init() {
    const preference = DARK_MODE_ENABLED ? safeGetPreference() : Theme.LIGHT;
    applyTheme(resolveTheme(preference), preference);
  }

  init();

  const mediaQuery = window.matchMedia
    ? window.matchMedia("(prefers-color-scheme: dark)")
    : null;

  function handleSystemChange() {
    if (!DARK_MODE_ENABLED) return;
    if (safeGetPreference() === Theme.SYSTEM) {
      applyTheme(resolveTheme(Theme.SYSTEM), Theme.SYSTEM);
    }
  }

  if (mediaQuery) {
    if (typeof mediaQuery.addEventListener === "function") {
      mediaQuery.addEventListener("change", handleSystemChange);
    } else if (typeof mediaQuery.addListener === "function") {
      mediaQuery.addListener(handleSystemChange);
    }
  }

  window.ThemeManager = {
    getPreference: () => safeGetPreference(),
    setPreference,
    cycle: cyclePreference,
    getState: () => ({
      theme: appliedTheme || resolveTheme(safeGetPreference()),
      preference: appliedPreference || safeGetPreference(),
    }),
  };
})();
