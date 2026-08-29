"""Tests for the public /api/v1 developer API.

The API surface is described in three hand-maintained places:

  1. the Caddy allowlist regexp + rewrites (docs/caddy-api-block.txt, a
     reference copy of the live /etc/caddy/Caddyfile),
  2. the machine-readable catalog (site/assets/api-catalog.json),
  3. the human docs table (site/api.html).

Nothing generates one from another, so they drift silently — a rewrite
without a matching allowlist entry 404s, and a documented endpoint that was
never wired up 404s too. These tests pin them to each other.

They also enforce the public field allowlist on the stations export, which
is served both at /data/stations.json and as /api/v1/stations.
"""

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CADDY_BLOCK = REPO_ROOT / "docs" / "caddy-api-block.txt"
CATALOG = REPO_ROOT / "site" / "assets" / "api-catalog.json"
API_HTML = REPO_ROOT / "site" / "api.html"
WAVE_FETCH = REPO_ROOT / "scripts" / "fetch" / "fetch_wave_forecast.py"
STATIONS_EXPORT = REPO_ROOT / "site" / "data" / "stations.json"

# A station id stands in for {station_id} when checking path templates.
SAMPLE_ID = "46087"


def _caddy_text():
    if not CADDY_BLOCK.exists():
        pytest.skip(f"{CADDY_BLOCK} not present")
    return CADDY_BLOCK.read_text()


def allowlist_regexp():
    """The compiled allowlist guard from the Caddy reference block."""
    m = re.search(r"not path_regexp allow (\S+)", _caddy_text())
    assert m, "allowlist regexp not found in caddy-api-block.txt"
    return re.compile(m.group(1))


def rewrite_paths():
    """Left-hand sides of the literal `rewrite <path> <file>` directives."""
    return {m.group(1) for m in re.finditer(r"^\s*rewrite (/\S*) (/\S+)$", _caddy_text(), re.M)}


def catalog():
    return json.loads(CATALOG.read_text())


def catalog_paths():
    return {e["path"] for e in catalog()["endpoints"]}


def api_html_paths():
    """Endpoint paths from the first column of the api.html endpoint table."""
    rows = re.findall(r"<td>(/[a-z0-9{}/_-]+)</td>", API_HTML.read_text())
    return set(rows)


def concrete(path):
    """Substitute a real station id into a documented path template."""
    return path.replace("{station_id}", SAMPLE_ID).replace("{id}", SAMPLE_ID)


def wave_station_ids():
    """Canonical wave-forecast station ids: the STATIONS dict in the fetcher."""
    block = re.search(r"^STATIONS = \{(.*?)^\}", WAVE_FETCH.read_text(), re.M | re.S)
    assert block, "STATIONS dict not found in fetch_wave_forecast.py"
    return set(re.findall(r'^\s*"([^"]+)":', block.group(1), re.M))


class TestSurfacesAgree:
    """The catalog, the docs page, and the Caddy config describe one API."""

    def test_catalog_and_api_html_list_the_same_endpoints(self):
        # The two pages use different placeholder spellings ({station_id} vs
        # {id}); compare the concrete paths they describe, not the templates.
        assert {concrete(p) for p in catalog_paths()} == {concrete(p) for p in api_html_paths()}

    def test_every_documented_endpoint_passes_the_allowlist(self):
        allow = allowlist_regexp()
        for path in sorted(catalog_paths()):
            assert allow.match(concrete(path)), f"{path} is documented but the Caddy allowlist would 404 it"

    def test_every_rewrite_passes_the_allowlist(self):
        """A rewrite without an allowlist entry is dead — the guard runs first."""
        allow = allowlist_regexp()
        for path in sorted(rewrite_paths()):
            assert allow.match(path), f"rewrite {path} is unreachable: it fails the allowlist guard"

    def test_every_rewrite_is_documented(self):
        """A wired-up endpoint nobody documented is invisible to consumers."""
        documented = {concrete(p) for p in catalog_paths()}
        # "/" is the catalog itself, published as the base URL rather than as
        # an entry in its own endpoint list.
        undocumented = rewrite_paths() - documented - {"/"}
        assert not undocumented, f"undocumented endpoints: {sorted(undocumented)}"


class TestWaveStationIds:
    """Station ids appear in the catalog and the allowlist; both must match."""

    def test_catalog_station_ids_match_the_fetcher(self):
        expected = wave_station_ids()
        listed = [e for e in catalog()["endpoints"] if "station_ids" in e]
        assert listed, "no endpoint in the catalog publishes station_ids"
        for endpoint in listed:
            assert set(endpoint["station_ids"]) == expected, (
                f"{endpoint['path']} station_ids are out of sync with " f"STATIONS in fetch_wave_forecast.py"
            )

    def test_allowlist_admits_exactly_the_known_stations(self):
        allow = allowlist_regexp()
        for sid in wave_station_ids():
            assert allow.match(f"/wave-forecast/{sid}")
            assert allow.match(f"/wave-forecast/verification/{sid}")

    def test_allowlist_rejects_unknown_stations(self):
        """Enumerating ids is what gives an unknown station a JSON 404 body."""
        allow = allowlist_regexp()
        for bogus in ("NOSUCH", "46087x", "0", "../etc"):
            assert not allow.match(f"/wave-forecast/{bogus}")
            assert not allow.match(f"/wave-forecast/verification/{bogus}")


class TestGuardRejectsInternals:
    """The allowlist is what stops the API exposing the whole site/ tree."""

    @pytest.mark.parametrize(
        "path",
        [
            "/data/system_health.json",
            "/data/latest_buoy_v2.json",
            "/data/stations.json",
            "/assets/api-catalog.json",
            "/index.html",
            "/stations/",
            "/STATIONS",
            "/nope-xyz",
        ],
    )
    def test_internal_paths_fail_the_allowlist(self, path):
        assert not allowlist_regexp().match(path)

    def test_system_health_is_not_aliased(self):
        """Operational telemetry stays off the API. See docs/PUBLIC_API.md.

        The prose comment in the block mentions it deliberately, so check the
        rewrite targets rather than the raw text.
        """
        targets = re.findall(r"^\s*rewrite \S+ (/\S+)$", _caddy_text(), re.M)
        assert not [t for t in targets if "system_health" in t]


class TestMethodGuard:
    """GET/HEAD/OPTIONS only, and the guard has to sit where it executes."""

    def test_method_guard_is_inside_the_route_block(self):
        """Caddy sorts `respond` after `route`, so a guard written outside the
        route never runs and every method falls through to file_server."""
        text = _caddy_text()
        route_at = text.index("\t\troute {")
        badmethod_at = text.index("@badmethod")
        preflight_at = text.index("@preflight")
        assert badmethod_at > route_at, "@badmethod guard is outside `route` — dead code"
        assert preflight_at > route_at, "@preflight guard is outside `route` — dead code"

    def test_only_safe_methods_are_advertised(self):
        assert 'Access-Control-Allow-Methods "GET, HEAD, OPTIONS"' in _caddy_text()


class TestCacheTiers:
    """Tier matchers must be disjoint; overlap silently serves stale data."""

    def test_med_and_slow_do_not_overlap(self):
        """/wave-forecast/verification/* and /storm-surge/observed are 10-minute
        feeds sitting under prefixes the slow tier would otherwise claim."""
        text = _caddy_text()
        med = re.search(r"@med path (.+)", text).group(1).split()
        assert "/wave-forecast/verification/*" in med
        assert "/storm-surge/observed" in med

        slow = re.search(r"@slow \{(.*?)\}", text, re.S).group(1)
        assert "not path /wave-forecast/verification/*" in slow
        assert "/storm-surge/*" not in slow, "/storm-surge/* would swallow /storm-surge/observed into the slow tier"

    def test_catalog_cache_seconds_match_the_served_tiers(self):
        """Every documented cache_seconds must be a tier that actually exists."""
        text = _caddy_text()
        tiers = {int(m) for m in re.findall(r"Cache-Control \"public, max-age=(\d+)", text)}
        for endpoint in catalog()["endpoints"]:
            assert endpoint["cache_seconds"] in tiers, (
                f"{endpoint['path']} documents cache_seconds=" f"{endpoint['cache_seconds']}, which no Caddy tier emits"
            )


class TestStationsExportIsFiltered:
    """site/data/stations.json is public twice over: /data/ and /api/v1/stations."""

    PRIVATE_FIELDS = {
        "channels",
        "fallback_channels",
        "flowworks_site_id",
        "url",
        "credentials",
        "auth",
        "rate_limit",
    }

    def test_no_private_fields_are_published(self):
        if not STATIONS_EXPORT.exists():
            pytest.skip("stations.json not exported yet")
        data = json.loads(STATIONS_EXPORT.read_text())

        found = []

        def walk(node, path=""):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in self.PRIVATE_FIELDS:
                        found.append(f"{path}.{key}")
                    walk(value, f"{path}.{key}")
            elif isinstance(node, list):
                for i, value in enumerate(node):
                    walk(value, f"{path}[{i}]")

        walk(data)
        assert not found, f"private fields leaked into the public export: {found}"

    def test_export_uses_an_allowlist_not_a_denylist(self):
        """A new registry field must stay private until named deliberately."""
        source = (REPO_ROOT / "scripts" / "export" / "export_stations_json.py").read_text()
        assert "PUBLIC_STATION_FIELDS" in source
        assert "filter_public_fields" in source
