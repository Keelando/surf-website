"""sitemap.xml must list every public page, and only real ones.

api.html shipped footer-linked and crawlable but unlisted, which is how this
drifts: a new page is added and the sitemap is not touched. The coverage test
below fails on that. EXCLUDED holds the deliberate omissions, each with a
reason, so leaving a page out stays a decision rather than an oversight.
"""

import datetime
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE = REPO_ROOT / "site"
SITEMAP = SITE / "sitemap.xml"
NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
BASE = "https://halibutbank.ca/"

# Pages deliberately absent from the sitemap.
EXCLUDED = {
    # Untracked local ops dashboard, linked from nothing.
    "analytics.html",
}


def _tracked_pages():
    """Top-level tracked pages only.

    Note the pathspec: git globs match across directories, so a bare
    "site/*.html" would also sweep in site/components/*.html, which are HTML
    fragments injected by nav.js/footer.js, not pages. Hence the parent check.
    """
    out = subprocess.run(
        ["git", "ls-files", "--", "site/*.html"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return {Path(p).name for p in out if Path(p).parent == Path("site")}


def _sitemap_locs():
    root = ET.parse(SITEMAP).getroot()
    return [u.findtext("sm:loc", namespaces=NS) for u in root]


def test_every_tracked_page_is_listed():
    listed = {loc.removeprefix(BASE) or "index.html" for loc in _sitemap_locs()}
    missing = _tracked_pages() - listed - EXCLUDED
    assert not missing, (
        f"Pages missing from site/sitemap.xml: {sorted(missing)}. "
        "Add them, or add to EXCLUDED here with a reason."
    )


def test_no_listed_page_is_missing_or_noindex():
    for loc in _sitemap_locs():
        assert loc.startswith(BASE), f"{loc} is not on {BASE}"
        name = loc.removeprefix(BASE) or "index.html"
        page = SITE / name
        assert page.is_file(), f"sitemap lists {loc}, but {page} does not exist"
        # A page cannot both ask to be indexed and tell robots not to.
        assert "noindex" not in page.read_text().lower(), (
            f"{name} is in the sitemap but carries a noindex robots directive"
        )


def test_lastmod_dates_are_valid_and_not_in_the_future():
    today = datetime.date.today()
    root = ET.parse(SITEMAP).getroot()
    for url in root:
        loc = url.findtext("sm:loc", namespaces=NS)
        lastmod = url.findtext("sm:lastmod", namespaces=NS)
        assert lastmod, f"{loc} has no <lastmod>"
        parsed = datetime.date.fromisoformat(lastmod)
        assert parsed <= today, f"{loc} has a future lastmod ({lastmod})"
