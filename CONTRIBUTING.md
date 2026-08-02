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

**`pyproject.toml` is the single source of truth for dependencies.**
`requirements-lock.txt` is derived from it — a pinned artifact recording the
exact versions this server runs. There is no `requirements.txt`.

If you add a new `import` to any script that runs via cron, add it to
`[project.dependencies]` immediately. Cron jobs fail silently otherwise.

```bash
# 1. Declare it in pyproject.toml (unpinned — the lock file does the pinning)
# 2. Install locally right away
.venv/bin/pip install some-package

# 3. Regenerate the lock (see the header of requirements-lock.txt)
```

Two kinds of dependency are easy to miss, because nothing imports them in a way
a grep for `import` will find:

- **Subprocess-invoked binaries** — e.g. `yt-dlp`, called from the venv `bin/`
  by `lib/webcam/youtube.py`. Still a real dependency; declare it.
- **Optional features of another package** — e.g. `amqp`, which `metpx-sr3`
  lists as an optional feature (`sr3 features`) and pip therefore will *not*
  install, even though every `config/sr3/*.conf` uses `broker amqps://`.

Never rely on a package arriving transitively. `Pillow` reached this venv only
because an unrelated tool happened to require it — one cleanup away from
silently breaking the webcam pipeline.

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

## Code quality

### Pre-commit hook

A git pre-commit hook runs automatically on every commit. It checks:

1. **ruff check** on staged `.py` files (lint errors block the commit)
2. **pytest** — the full test suite (209 tests, ~0.3s)
3. **eslint** on staged `.js` files

If any step fails, the commit is rejected. Fix the issue and try again.

The hook lives at `.git/hooks/pre-commit`. It is not tracked by git, so after a fresh clone you need to set it up:

```bash
cp docs/hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

### Formatting & linting tools

| Tool | Language | Command | Purpose |
|------|----------|---------|---------|
| [Ruff](https://docs.astral.sh/ruff/) | Python | `.venv/bin/ruff check .` | Linting |
| [Ruff](https://docs.astral.sh/ruff/) | Python | `.venv/bin/ruff format .` | Formatting |
| [Biome](https://biomejs.dev/) | JavaScript | `npm run format:js` | Formatting |
| [ESLint](https://eslint.org/) | JavaScript | `npm run lint:js` | Linting |

### Running tests

```bash
# Python tests only
npm run test:python
# or directly:
.venv/bin/pytest tests/ -v

# Full suite (Python + Playwright frontend tests)
npm run test
```

### Workflow

1. Make your changes
2. Format: `npm run format:js` (if JS changed), `.venv/bin/ruff format .` (if Python changed)
3. Commit — the pre-commit hook runs automatically
4. If the hook fails, fix the issue and commit again

---

## Deployment

After making changes:

```bash
# Reload Caddy if Caddyfile changed
sudo caddy reload --config /etc/caddy/Caddyfile

# Manually run a parser/exporter if you can't wait for cron
.venv/bin/python3 scripts/parse/buoy_to_sqlite.py
.venv/bin/python3 scripts/export/sqlite_to_json.py
```

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the full setup and cron schedule.
See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) if something is broken.
