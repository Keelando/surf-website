// Navigation initialization (runs whenever the nav HTMX fragment is swapped in)
(function () {
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
      // otherwise indistinguishable. Tag the footer one: below 900px the
      // header nav is sticky and always on screen, which makes a second
      // hamburger down here redundant, and CSS needs a handle to hide it.
      nav.classList.toggle("main-nav-footer", index > 0);
    });
  }

  function initClock() {
    if (window._navClockStarted) return;
    window._navClockStarted = true;

    // timeZoneName gives "PST" or "PDT" as the date falls; a hardcoded label
    // is wrong half the year. hourCycle h23, not hour12: false, which Chromium
    // renders as "24:00:00" at midnight.
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Vancouver",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
      timeZoneName: "short",
    });

    const updateClock = () => {
      const clocks = document.querySelectorAll(".nav-clock");
      if (!clocks.length) return;

      const parts = formatter.formatToParts(new Date());
      const part = (type) => parts.find((p) => p.type === type)?.value;
      const label = `${part("timeZoneName")} ${part("hour")}:${part("minute")}:${part("second")}`;

      clocks.forEach((clock) => {
        clock.textContent = label;
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

  // The icon is CSS's job (it follows data-theme); the label says what a
  // press does. No aria-pressed: a button whose label changes with its state
  // must not also announce a pressed state, or it reads as "Switch to dark
  // mode, pressed".
  function updateToggle(button) {
    if (!button) return;
    const { preference } = getThemeState();
    const next = getNextPreference(preference);
    button.removeAttribute("aria-pressed");
    button.setAttribute("aria-label", `Switch to ${next} mode`);
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

  // Keys that scroll the page, cancelled while the drawer holds the lock.
  const SCROLL_KEYS = new Set([" ", "PageDown", "PageUp", "End", "Home", "ArrowDown", "ArrowUp"]);

  // True while the scroll-cancelling listeners are attached.
  let scrollLocked = false;

  // Let a scroll gesture through if it started inside the drawer — the drawer
  // has its own overflow-y and must stay scrollable when its list is long.
  function insideDrawer(target) {
    const drawer = document.querySelector(".main-nav.nav-open .nav-scroll");
    return drawer !== null && target instanceof Node && drawer.contains(target);
  }

  function blockScrollEvent(event) {
    if (insideDrawer(event.target)) return;
    event.preventDefault();
  }

  function blockScrollKey(event) {
    if (!SCROLL_KEYS.has(event.key)) return;
    const target = event.target;
    // Never swallow a key headed for a form control or the drawer's own list.
    if (insideDrawer(target)) return;
    if (target instanceof Element && target.closest("input, select, textarea")) return;
    event.preventDefault();
  }

  /**
   * Lock or unlock page scrolling to match whether any drawer is open.
   *
   * Driven by the DOM rather than by each call site: three separate paths
   * clear `nav-open` (the toggle, the outside-click handler, Escape), and a
   * lock that each one had to remember to release is a lock that eventually
   * strands the page unscrollable.
   *
   * Implemented by cancelling scroll events rather than by CSS, because this
   * nav is `position: sticky` and every CSS recipe for a scroll lock breaks it
   * — see the note in nav-tide-styles-v4.css for the two that were measured
   * doing so. Cancelling events changes no layout, so the bar stays exactly
   * where it is stuck and the drawer stays on screen.
   *
   * `passive: false` is required: listeners for wheel and touchmove are
   * passive by default, and a passive listener cannot preventDefault.
   */
  function syncScrollLock() {
    const anyOpen = document.querySelector(".main-nav.nav-open") !== null;
    if (anyOpen === scrollLocked) return;
    scrollLocked = anyOpen;

    const method = anyOpen ? "addEventListener" : "removeEventListener";
    document[method]("wheel", blockScrollEvent, { passive: false });
    document[method]("touchmove", blockScrollEvent, { passive: false });
    document[method]("keydown", blockScrollKey);
  }

  // Close every open drawer on the page and sync its button's aria state.
  function closeAllNavs() {
    document.querySelectorAll(".main-nav.nav-open").forEach((nav) => {
      nav.classList.remove("nav-open");
      nav.querySelector(".nav-toggle")?.setAttribute("aria-expanded", "false");
    });
    syncScrollLock();
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
        syncScrollLock();
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
        syncScrollLock();
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") closeAllNavs();
      });
    }
  }

  // ---- Header wave ---------------------------------------------------------
  // The band along the bottom of the header is one sinusoid drawn from
  // Halibut Bank's latest report: wave height, against what is rough there
  // (shared/wave-scale.js), sets the amplitude; wave period the speed of the
  // drift. Decoration only; the numbers live in the station list.

  const WAVE_STATION = "4600146";
  const WAVE_HEIGHT_PX = 12;
  // One sinusoid, as on the brand board: wavelength and centreline in px.
  const WAVE_WAVELENGTH_PX = 200;
  const WAVE_CENTRE_PX = 6.5;
  // Long enough to cover the widest screen plus one wavelength of drift.
  const WAVE_LENGTH_PX = 4000;
  // What a calm sea looks like, and what the band shows when the buoy is
  // late, down or missing: no error state in the header. `amp` is the
  // amplitude factor (0.2–1.2) that shared/wave-scale.js would give.
  const CALM = { amp: 0.3, period: 4 };
  const SVG_NS = "http://www.w3.org/2000/svg";

  /**
   * A filled sine band: quadratic half-waves along the centreline, closed
   * along the bottom edge. `a` is the crest height in px.
   */
  function wavePath(wavelength, centre, a) {
    const half = wavelength / 2;
    let d = `M${-wavelength} ${centre} Q${-wavelength + half / 2} ${centre - 2 * a} ${-half} ${centre}`;
    for (let x = 0; x <= WAVE_LENGTH_PX; x += half) d += ` T${x} ${centre}`;
    return `${d} V${WAVE_HEIGHT_PX} H${-wavelength} Z`;
  }

  /** Amplitude and period to draw, or the calm default when the report is unusable. */
  function waveReading(entry, waveAmplitude) {
    if (!entry || entry.no_data || entry.out_of_service || entry.status !== "ok") return CALM;
    const height = entry.wave_height_sig;
    if (height == null || !Number.isFinite(height)) return CALM;
    const period = entry.wave_period_peak ?? entry.wave_period_avg ?? CALM.period;
    return {
      amp: waveAmplitude(height, WAVE_STATION),
      period: Number.isFinite(period) && period > 0 ? period : CALM.period,
    };
  }

  function drawWave(svg, reading) {
    const { amp } = reading;
    // At the top of the clamp the troughs just touch the band's bottom edge.
    const maxCrest = 5;
    // The band slides one wavelength per cycle, so the loop is seamless;
    // a longer swell period drifts slower.
    svg.style.setProperty("--wave-period", `${(reading.period * 3).toFixed(2)}s`);
    svg.style.setProperty("--wave-shift", `${-WAVE_WAVELENGTH_PX}px`);
    svg
      .querySelector(".nav-wave-layer path")
      .setAttribute("d", wavePath(WAVE_WAVELENGTH_PX, WAVE_CENTRE_PX, maxCrest * (amp / 1.2)));
  }

  function buildWave() {
    const svg = document.createElementNS(SVG_NS, "svg");
    svg.setAttribute("class", "nav-wave");
    svg.setAttribute("aria-hidden", "true");
    svg.setAttribute("focusable", "false");
    const layer = document.createElementNS(SVG_NS, "g");
    layer.setAttribute("class", "nav-wave-layer");
    layer.appendChild(document.createElementNS(SVG_NS, "path"));
    svg.appendChild(layer);
    drawWave(svg, CALM);
    return svg;
  }

  async function refreshWave() {
    const svg = document.querySelector(".main-nav:not(.main-nav-footer) .nav-wave");
    if (!svg) return;
    let reading = CALM;
    try {
      // nav.js is a classic script, so the shared scale comes in by dynamic
      // import (unversioned, like every module import: revalidated each load).
      const [{ waveAmplitude }, response] = await Promise.all([
        import("/assets/js/shared/wave-scale.js"),
        fetch(`/data/latest_buoy_v2.json?t=${Date.now()}`),
      ]);
      if (response.ok) reading = waveReading((await response.json())[WAVE_STATION], waveAmplitude);
    } catch {
      // Network trouble leaves the calm wave up; the next refresh tries again.
    }
    drawWave(svg, reading);
  }

  function initWave() {
    // Header copy only: the footer nav is the same fragment, and one moving
    // band per page is plenty.
    const nav = document.querySelector(".main-nav:not(.main-nav-footer)");
    if (!nav || nav.querySelector(".nav-wave")) return;
    nav.appendChild(buildWave());
    refreshWave();
    if (!window._navWaveTimer) {
      // The same five-minute rhythm as the home page's station refresh.
      window._navWaveTimer = setInterval(refreshWave, 5 * 60 * 1000);
    }
  }

  function initNav() {
    highlightActivePage();
    labelNavLandmarks();
    initClock();
    initThemeToggle();
    initHamburger();
    initWave();
  }

  setTimeout(initNav, 0);
})();
