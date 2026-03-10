// Nav initialization - runs when nav.html is loaded by HTMX
// This file replaces the inline script that was previously in nav.html
(function () {
  setTimeout(() => {
    const path = window.location.pathname;
    let activePage = "buoys";

    if (path === "/" || path === "/index.html") activePage = "buoys";
    else if (path.includes("tides")) activePage = "tides";
    else if (path.includes("winds")) activePage = "winds";
    else if (path.includes("lightstations")) activePage = "lightstations";
    else if (path.includes("webcams")) activePage = "webcams";
    else if (path.includes("forecasts")) activePage = "forecasts";
    else if (path.includes("storm_surge")) activePage = "storm_surge";

    document.querySelectorAll(".nav-link").forEach((link) => {
      if (link.dataset.page === activePage) {
        link.classList.add("active");
      }
    });

    // Start clock only once, even if nav is loaded multiple times
    if (!window._navClockStarted) {
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
  }, 0);
})();
