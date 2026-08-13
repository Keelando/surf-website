"""
Credential loading from the environment, with `config/.env` as fallback.

Credentials must never live in a tracked file. They used to sit in
`config/crontab.txt` (tracked, public) as plain `VAR=value` lines, which is
how cron exported them to the fetch scripts. They now live in `config/.env`,
which is gitignored, and this module is the single place that reads it —
replacing one ad-hoc `.env` parser and three copies of `_require_env`.

`os.environ` still wins, so a value exported by systemd, a shell or a test
overrides the file without editing it.
"""

from __future__ import annotations

import os
from functools import lru_cache

from lib.config import PROJECT_ROOT

ENV_FILE = PROJECT_ROOT / "config" / ".env"


@lru_cache(maxsize=1)
def _file_values() -> dict[str, str]:
    """Parse `config/.env` — `KEY=value`, `#` comments, optional quotes."""
    values: dict[str, str] = {}
    if not ENV_FILE.is_file():
        return values
    for line in ENV_FILE.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values


def get_env(name: str, default: str | None = None) -> str | None:
    """Value from the process environment, else `config/.env`, else default."""
    value = os.environ.get(name)
    if value:
        return value
    return _file_values().get(name, default)


def require_env(name: str) -> str:
    """Same as `get_env`, but fail loudly rather than half-run without auth."""
    value = get_env(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Add it to {ENV_FILE} (gitignored) or export it "
            f"in the environment."
        )
    return value
