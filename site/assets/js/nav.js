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
        link.setAttribute("aria-current", "page");
        activeLink = link;
      } else {
        link.classList.remove("active");
        link.removeAttribute("aria-current");
      }
    });
    return activeLink;
  }

  // The same nav fragment is injected at the top and bottom of every page;
  // identical landmarks need distinguishing labels. Re-runs after each HTMX
  // swap, so whichever fragment arrives last still labels both.
  function labelNavLandmarks() {
    document.querySelectorAll("nav.main-nav").forEach((nav, index) => {
      nav.setAttribute("aria-label", index === 0 ? "Main navigation" : "Footer navigation");
      // The same fragment is injected top and bottom, so the copies are
      // otherwise indistinguishable. Tag the footer one: below 600px the
      // header nav is sticky and always on screen, which makes a second
      // hamburger down here redundant, and CSS needs a handle to hide it.
      nav.classList.toggle("main-nav-footer", index > 0);
    });
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

  // Close every open drawer on the page and sync its button's aria state.
  function closeAllNavs() {
    document.querySelectorAll(".main-nav.nav-open").forEach((nav) => {
      nav.classList.remove("nav-open");
      nav.querySelector(".nav-toggle")?.setAttribute("aria-expanded", "false");
    });
  }

  function initHamburger() {
    // The nav fragment is injected TWICE per page — once at the top, once
    // above the footer — so every lookup here has to be per-nav. A singular
    // querySelector wired only the first, which left the footer hamburger
    // drawing its three bars and doing nothing when tapped.
    document.querySelectorAll(".main-nav").forEach((nav) => {
      const button = nav.querySelector(".nav-toggle");

      // Per-element wiring (re-runs safely after each HTMX nav swap).
      if (!button || button.dataset.bound) return;
      button.dataset.bound = "true";

      const setOpen = (open) => {
        nav.classList.toggle("nav-open", open);
        button.setAttribute("aria-expanded", open ? "true" : "false");
      };

      button.addEventListener("click", (event) => {
        event.stopPropagation();
        const open = !nav.classList.contains("nav-open");
        // Only one drawer open at a time, or tapping the footer hamburger
        // would leave the header's drawer hanging open off-screen.
        closeAllNavs();
        setOpen(open);
      });

      // Tapping a destination closes the drawer.
      nav.querySelectorAll(".nav-link").forEach((link) => {
        link.addEventListener("click", () => setOpen(false));
      });
    });

    // Document-level listeners bind once; they re-query the current nav so they
    // keep working across HTMX fragment swaps.
    if (!window._navHamburgerGlobal) {
      window._navHamburgerGlobal = true;

      document.addEventListener("click", (event) => {
        // Close any open drawer the click landed outside of.
        document.querySelectorAll(".main-nav.nav-open").forEach((nav) => {
          if (!nav.contains(event.target)) {
            nav.classList.remove("nav-open");
            nav.querySelector(".nav-toggle")?.setAttribute("aria-expanded", "false");
          }
        });
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") closeAllNavs();
      });
    }
  }

  function initNav() {
    highlightActivePage();
    labelNavLandmarks();
    initClock();
    initThemeToggle();
    initHamburger();
  }

  setTimeout(initNav, 0);
})();
