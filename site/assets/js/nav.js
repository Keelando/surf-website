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

  // On the mobile single-row nav, make the horizontal overflow discoverable:
  // scroll the active tab into view (so the current page is never hidden off the
  // right edge) and toggle a fade hint while more content sits past an edge. The
  // scroller is .nav-links (not the sticky .main-nav) so the fade mask repaints
  // in step with vertical scroll instead of lagging behind the sticky bar.
  function initScrollHints(activeLink) {
    const scroller = document.querySelector(".nav-scroll");
    if (!scroller) return;

    const updateHints = () => {
      const scrollable = scroller.scrollWidth - scroller.clientWidth > 1;
      scroller.classList.toggle("can-scroll-left", scrollable && scroller.scrollLeft > 1);
      scroller.classList.toggle(
        "can-scroll-right",
        scrollable && scroller.scrollLeft < scroller.scrollWidth - scroller.clientWidth - 1,
      );
    };

    // Centre the active tab without scrolling the page (no scrollIntoView).
    if (activeLink && scroller.scrollWidth > scroller.clientWidth) {
      const linkRect = activeLink.getBoundingClientRect();
      const scrollerRect = scroller.getBoundingClientRect();
      const target =
        scroller.scrollLeft +
        (linkRect.left - scrollerRect.left) -
        (scroller.clientWidth - linkRect.width) / 2;
      scroller.scrollLeft = Math.max(0, target);
    }

    if (!scroller.dataset.scrollHintsBound) {
      scroller.dataset.scrollHintsBound = "true";
      scroller.addEventListener("scroll", updateHints, { passive: true });
      window.addEventListener("resize", updateHints);
    }
    updateHints();
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

  function initNav() {
    const activeLink = highlightActivePage();
    initScrollHints(activeLink);
    initClock();
    initThemeToggle();
  }

  setTimeout(initNav, 0);
})();
