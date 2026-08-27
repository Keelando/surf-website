"""Social/SEO metadata must point at things that exist.

og:image sat at /assets/img/social_preview.jpg for months while the real
asset directory was assets/images/ - a path that had never existed in the
repo's history. Nothing failed, because a 404 on og:image is invisible
locally: it only shows up as a missing preview card in someone else's Slack.
These tests make that class of typo fail here instead.
"""

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE = REPO_ROOT / "site"
BASE = "https://halibutbank.ca/"


def _tracked_pages():
    out = subprocess.run(
        ["git", "ls-files", "--", "site/*.html"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return [REPO_ROOT / p for p in out if Path(p).parent == Path("site")]


def _meta(html, key):
    m = re.search(rf'<meta (?:property|name)="{re.escape(key)}" content="([^"]*)"', html)
    return m.group(1) if m else None


def test_local_asset_urls_in_meta_resolve():
    """Any halibutbank.ca asset URL in a meta tag must exist on disk."""
    for page in _tracked_pages():
        html = page.read_text()
        for key in ("og:image", "twitter:image"):
            url = _meta(html, key)
            assert url, f"{page.name} has no {key}"
            assert url.startswith(BASE), f"{page.name} {key} is not absolute: {url}"
            asset = SITE / url.removeprefix(BASE)
            assert asset.is_file(), (
                f"{page.name} {key} points at {url}, but {asset} does not exist"
            )


def test_social_card_essentials_present():
    for page in _tracked_pages():
        html = page.read_text()
        for key in ("og:title", "og:description", "og:url", "twitter:card"):
            assert _meta(html, key), f"{page.name} is missing {key}"
        assert _meta(html, "twitter:card") == "summary_large_image"
        assert re.search(r'<link rel="canonical"', html), f"{page.name} has no canonical"


def test_every_page_has_an_h1_in_the_served_html():
    """Crawlers that do not run JS must still see the heading.

    These h1s used to arrive via htmx after load, so the served HTML had none.
    """
    for page in _tracked_pages():
        assert "<h1" in page.read_text(), (
            f"{page.name} has no <h1> in its static HTML - if it is injected at "
            "runtime, non-JS crawlers will never see it"
        )


def test_structured_data_is_valid_json():
    import json

    found = {}
    for page in _tracked_pages():
        for block in re.findall(
            r'<script type="application/ld\+json">(.*?)</script>',
            page.read_text(),
            re.S,
        ):
            data = json.loads(block)  # raises on malformed JSON-LD
            assert data.get("@context") == "https://schema.org"
            found[page.name] = data["@type"]
    assert found.get("index.html") == "WebSite"
    assert found.get("api.html") == "Dataset"
