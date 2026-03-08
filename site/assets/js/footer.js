// Footer initialization - runs when footer.html is loaded by HTMX
// This file replaces the inline script that was previously in footer.html
(function() {
  fetch('/data/system_health.json')
    .then(res => res.json())
    .then(data => {
      const badge = document.getElementById('system-status-badge');
      if (!badge) return;
      const dot = badge.querySelector('.status-dot');
      const text = badge.querySelector('.status-text');

      const freshness = data.checks.data_freshness;
      const total = freshness.total_stations;
      const reporting = total - freshness.stale_count;
      const pct = Math.round((reporting / total) * 100);

      badge.classList.remove('status-ok', 'status-warning', 'status-error');
      badge.classList.add('status-' + data.overall_status);

      text.textContent = `${reporting}/${total} stations (${pct}%)`;

      const staleNames = freshness.stale_stations
        .filter(s => s.severity === 'error' || s.severity === 'warning')
        .map(s => s.name)
        .join(', ');

      badge.title = staleNames
        ? `System Status: ${data.overall_status.toUpperCase()}\nStale: ${staleNames}`
        : `System Status: ${data.overall_status.toUpperCase()}\nAll stations reporting normally`;
    })
    .catch(err => {
      const badge = document.getElementById('system-status-badge');
      if (badge) badge.querySelector('.status-text').textContent = 'Status unavailable';
      console.error('Failed to load system health:', err);
    });
})();
