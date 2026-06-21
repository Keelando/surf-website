// Navigation initialization (runs whenever the nav HTMX fragment is swapped in)
(function () {
  const LABELS = {
    system: "System",
    light: "Light",
    dark: "Dark",
  };

  function highlightActivePage() {
    const path = window.location.pathname;
    let activePage = "buoys";

    if (path === "/" || path === "/index.html") activePage = "buoys";
    else if (path.includes("tides")) activePage = "tides";
    else if (path.includes("winds")) activePage = "winds";
    else if (path.includes("lightstations")) activePage = "lightstations";
    else if (path.includes("webcams")) activePage = "webcams";
    else if (path.includes("forecasts")) activePage = "forecasts";
    else if (path.includes("storm_surge")) activePage = "storm_surge";

    let activeLink = null;
    document.querySelectorAll(".nav-link").forEach((link) => {
      if (link.dataset.page === activePage) {
        link.classList.add("active");
        activeLink = link;
      } else {
        link.classList.remove("active");
      }
    });
    return activeLink;
  }

  function initClock() {
    if (window._navClockStarted) return;
    window._navClockStarted = true;

    const updateClock = () => {
      const clocks = document.querySelectorAll(".nav-clock");
      if (!clocks.length) return;

      const now = new Date();
      const timeString = now.toLocaleTimeString("en-US", {
        timeZone: "America/Vancouver",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      });

      clocks.forEach((clock) => {
        clock.textContent = `PST ${timeString}`;
      });
    };

    updateClock();
    setInterval(updateClock, 1000);
  }

  function getThemeState() {
    if (window.ThemeManager && typeof window.ThemeManager.getState === "function") {
      return window.ThemeManager.getState();
    }

    const fallbackTheme = document.documentElement.getAttribute("data-theme") || "light";
    return { theme: fallbackTheme, preference: "system" };
  }

  function getNextPreference(current) {
    return current === "dark" ? "light" : "dark";
  }

  function updateToggle(button) {
    if (!button) return;
    const { theme, preference } = getThemeState();
    const nextPreference = getNextPreference(preference);

    const iconSpan = button.querySelector(".theme-toggle-icon");
    const textSpan = button.querySelector(".theme-toggle-text");

    if (iconSpan) {
      iconSpan.textContent = preference === "system" ? "⚙" : theme === "dark" ? "☾" : "☀";
    }

    if (textSpan) {
      textSpan.textContent = LABELS[preference] || preference;
    }

    const ariaPressed = preference === "dark" ? "true" : preference === "light" ? "false" : "mixed";
    button.setAttribute("aria-pressed", ariaPressed);
    button.setAttribute(
      "aria-label",
      `Theme: ${LABELS[preference] || preference}. Switch to ${LABELS[nextPreference]} mode`,
    );
  }

  function initThemeToggle() {
    const buttons = document.querySelectorAll(".theme-toggle");
    if (!buttons.length) return;

    buttons.forEach((button) => {
      if (!button.dataset.bound) {
        button.dataset.bound = "true";
        button.addEventListener("click", () => {
          if (window.ThemeManager && typeof window.ThemeManager.cycle === "function") {
            window.ThemeManager.cycle();
          }
        });
      }
      updateToggle(button);
    });

    if (!window._navThemeListenerAttached) {
      window._navThemeListenerAttached = true;
      window.addEventListener("themechange", () => {
        document.querySelectorAll(".theme-toggle").forEach(updateToggle);
      });
    }
  }

  function initHamburger() {
    const nav = document.querySelector(".main-nav");
    const button = document.querySelector(".nav-toggle");

    // Per-element wiring (re-runs safely after each HTMX nav swap).
    if (nav && button && !button.dataset.bound) {
      button.dataset.bound = "true";

      const setOpen = (open) => {
        nav.classList.toggle("nav-open", open);
        button.setAttribute("aria-expanded", open ? "true" : "false");
      };

      button.addEventListener("click", (event) => {
        event.stopPropagation();
        setOpen(!nav.classList.contains("nav-open"));
      });

      // Tapping a destination closes the drawer.
      nav.querySelectorAll(".nav-link").forEach((link) => {
        link.addEventListener("click", () => setOpen(false));
      });
    }

    // Document-level listeners bind once; they re-query the current nav so they
    // keep working across HTMX fragment swaps.
    if (!window._navHamburgerGlobal) {
      window._navHamburgerGlobal = true;

      const closeMenu = () => {
        const currentNav = document.querySelector(".main-nav");
        const currentButton = document.querySelector(".nav-toggle");
        if (currentNav && currentNav.classList.contains("nav-open")) {
          currentNav.classList.remove("nav-open");
          if (currentButton) currentButton.setAttribute("aria-expanded", "false");
        }
      };

      document.addEventListener("click", (event) => {
        const currentNav = document.querySelector(".main-nav");
        if (
          currentNav &&
          currentNav.classList.contains("nav-open") &&
          !currentNav.contains(event.target)
        ) {
          closeMenu();
        }
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") closeMenu();
      });
    }
  }

  function initNav() {
    highlightActivePage();
    initClock();
    initThemeToggle();
    initHamburger();
  }

  setTimeout(initNav, 0);
})();
