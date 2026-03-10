# Contributing to halibutbank.ca

This is a personal project — contributions welcome, but please read these notes first.

> **This repository is public and this is the live production environment.**
> The code here runs [halibutbank.ca](https://halibutbank.ca) directly — there is no staging server.
> Do not commit secrets, credentials, internal paths, or security configuration details.

## Project structure

```
envcan_wave/
├── scripts/          # Backend: fetch, parse, export
├── site/             # Frontend: static HTML/CSS/JS
│   ├── assets/js/    # All JavaScript lives here
│   ├── components/   # HTMX-loaded HTML partials (nav, footer, etc.)
│   └── data/         # JSON outputs (auto-generated, do not edit)
├── config/           # stations.json and other config
└── docs/             # Extended documentation
```

See [docs/ARCHITECTURE_DETAILED.md](docs/ARCHITECTURE_DETAILED.md) for a full overview.

---

## Frontend rules

### No inline JavaScript in HTML

All JS must live in external files under `assets/js/`. Inline handlers and script blocks are not supported and will not work.

```html
<!-- Bad -->
<button onclick="doThing()">Click me</button>

<!-- Good — add an id, wire it up in the JS file -->
<button id="do-thing-btn">Click me</button>
```

```js
// In the appropriate assets/js/*.js file:
document.getElementById('do-thing-btn').addEventListener('click', doThing);

// Or use event delegation for dynamically created elements:
document.addEventListener('click', function(e) {
  if (e.target.closest('.my-class')) doThing();
});
```

The same applies to HTML component partials (`site/components/`). If a component needs JS, load it as an external script:

```html
<!-- Bad -->
<script>/* inline code */</script>

<!-- Good -->
<script src="/assets/js/my-component.js"></script>
```

### No createElement with inline handlers

When building DOM elements in JS, use `addEventListener` — don't set `onclick` as an HTML attribute or via `.innerHTML`:

```js
// Bad
el.innerHTML = '<button onclick="doThing()">Click</button>';

// Good
const btn = document.createElement('button');
btn.textContent = 'Click';
btn.addEventListener('click', doThing);
el.appendChild(btn);
```

### Cache-busting

After changing a JS or CSS file, bump its `?v=` query string in the HTML that loads it (e.g. `main.js?v=20260308` → `main.js?v=20260309`). This ensures browsers pick up the new version.

---

## Backend rules

### Python dependencies

If you add a new `import` to any script that runs via cron, **add it to `requirements.txt`** immediately. The venv syncs nightly, but cron jobs will fail silently until then.

```bash
# Install locally right away
.venv/bin/pip install some-package

# Add to requirements with pinned version
echo "some-package==1.2.3" >> requirements.txt
```

### XML parsing

Use `defusedxml` instead of the standard library `xml.etree.ElementTree` when parsing external data:

```python
# Bad
import xml.etree.ElementTree as ET

# Good
from defusedxml import ElementTree as ET
```

### Data conventions

- **Directions** are meteorological (FROM, not TO)
- **Storage** in km/h; **display** in knots
- **Timestamps** are Unix epoch in SQLite, ISO 8601 UTC in JSON
- **Freshness window** is 2 hours per field (each metric ages independently)
- **NOAA pressure** near 999 hPa is valid data, not missing

---

## Formatting & linting

- JavaScript lives under `site/assets/js`. Format with `npm run format:js` (Biome) and lint with `npm run lint:js`.
- Python formatting and linting run via Ruff: `ruff format . && ruff check .`.

---

## Deployment

After making changes:

```bash
# Reload Caddy if Caddyfile changed
sudo caddy reload --config /etc/caddy/Caddyfile

# Manually run a parser/exporter if you can't wait for cron
.venv/bin/python3 scripts/parse/buoy_to_influx_sqlite.py
.venv/bin/python3 scripts/export/sqlite_to_json.py
```

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the full setup and cron schedule.
See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) if something is broken.
