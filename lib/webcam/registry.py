"""One owner per field for webcam configuration.

A camera used to be described in four places, and on 2026-09-06 they disagreed
by up to 24 km of position, a wrong cadence and three of six names. The fix is
not a better test — it is a shape that cannot drift:

- ``config/stations.json`` ["webcams"] — tracked, public, already exported to
  ``/data/stations.json``. This is *what a camera is*: ``name``,
  ``short_name``, ``location``, ``lat``, ``lon``, ``source``,
  ``update_frequency_minutes``, ``stream_delay_minutes``, ``daylight_only``,
  ``daylight_margin_minutes``, ``page_url``.
- ``config/webcams.json`` — gitignored, and the only reason two files exist at
  all. This is *how a camera is fetched*: the URLs, referers, user agents,
  crop, archive and website directories, dedupe and cron offsets. It carries
  permission-restricted endpoints and must stay out of the public repo.

``load_webcams()`` merges the two and is the only way any consumer should read
either. Identity fields left behind in the private file are ignored with a
warning rather than honoured, so a stale copy cannot quietly win.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional

from lib.config import PROJECT_ROOT
from lib.stations import get_all_webcams

WEBCAM_CONFIG_PATH = PROJECT_ROOT / "config" / "webcams.json"

# Fields that now live in config/stations.json. If the private file still has
# them, the registry wins and we say so — that is the whole point of the split.
REGISTRY_OWNED_FIELDS = frozenset(
    {
        "name",
        "short_name",
        "location",
        "lat",
        "lon",
        "source",
        "source_text",
        "interval_minutes",
        "stream_delay_minutes",
        "check_daylight",
        "daylight_margin_minutes",
    }
)

logger = logging.getLogger(__name__)


class WebcamConfigMissing(FileNotFoundError):
    """The private half of the config is absent.

    A fresh clone has ``config/stations.json`` but not ``config/webcams.json``,
    so every loader has to fail clearly here rather than proceed with a
    half-configured camera that has a name and a position but nowhere to fetch
    from.
    """


def load_webcams(
    config_path: Path = WEBCAM_CONFIG_PATH, require_private: bool = True
) -> Dict[str, Dict]:
    """Return ``{cam_id: merged config}`` for every camera in both files.

    Identity comes from the tracked registry, fetch mechanics from the private
    file. ``archive_dir`` and ``website_dir`` come back as resolved ``Path``s
    (``website_dir`` is written relative to the repo root). Keys beginning
    with ``_`` (``_comment``, ``_permission_note``, ``_schedule_note``) are
    meta and are dropped from both sides.

    Args:
        config_path: location of the private file, for tests.
        require_private: raise :class:`WebcamConfigMissing` when the private
            file is absent. Pass ``False`` from consumers that only need
            identity (and can cope with an empty result), such as monitoring.
    """
    registry = {
        cam_id: cam for cam_id, cam in get_all_webcams().items() if not cam_id.startswith("_")
    }

    if not config_path.exists():
        if require_private:
            raise WebcamConfigMissing(
                f"Webcam config not found at {config_path}. "
                f"Copy config/webcams.example.json to config/webcams.json and edit."
            )
        logger.warning("webcam config not found at %s; fetch settings unavailable", config_path)
        return {}

    private = {
        cam_id: {k: v for k, v in cfg.items() if not k.startswith("_")}
        for cam_id, cfg in json.loads(config_path.read_text()).items()
        if not cam_id.startswith("_")
    }

    unknown = sorted(set(private) - set(registry))
    if unknown:
        raise KeyError(
            f"{config_path.name} describes cameras missing from config/stations.json: {unknown}. "
            f"Add them to the tracked registry — that is where a camera's identity lives."
        )

    webcams = {}
    for cam_id, ident in registry.items():
        mechanics = private.get(cam_id)
        if mechanics is None:
            # In the registry but not the private file: nothing can fetch it.
            # Skip rather than hand back a camera with no source.
            logger.warning(
                "%s is in config/stations.json but has no entry in %s; skipping",
                cam_id,
                config_path.name,
            )
            continue

        stale = sorted(REGISTRY_OWNED_FIELDS & set(mechanics))
        if stale:
            logger.warning(
                "%s: %s in %s %s ignored — config/stations.json owns %s",
                cam_id,
                ", ".join(stale),
                config_path.name,
                "are" if len(stale) > 1 else "is",
                "those fields" if len(stale) > 1 else "that field",
            )
            mechanics = {k: v for k, v in mechanics.items() if k not in REGISTRY_OWNED_FIELDS}

        webcams[cam_id] = {
            "id": cam_id,
            "name": ident["name"],
            "short_name": ident.get("short_name"),
            "location": ident.get("location"),
            "lat": ident.get("lat"),
            "lon": ident.get("lon"),
            "source_text": ident.get("source"),
            "interval_minutes": ident.get("update_frequency_minutes"),
            "stream_delay_minutes": ident.get("stream_delay_minutes"),
            "check_daylight": ident.get("daylight_only", False),
            "daylight_margin_minutes": ident.get("daylight_margin_minutes", 30),
            **mechanics,
        }
        # Resolved once, here, so no consumer has to know that archive_dir is
        # absolute and website_dir is written relative to the repo.
        if "archive_dir" in webcams[cam_id]:
            webcams[cam_id]["archive_dir"] = Path(webcams[cam_id]["archive_dir"])
        if "website_dir" in webcams[cam_id]:
            website_dir = Path(webcams[cam_id]["website_dir"])
            webcams[cam_id]["website_dir"] = (
                website_dir if website_dir.is_absolute() else PROJECT_ROOT / website_dir
            )
    return webcams


def get_webcam(cam_id: str, config_path: Path = WEBCAM_CONFIG_PATH) -> Optional[Dict]:
    """Merged config for one camera, or ``None`` if it is not configured."""
    return load_webcams(config_path).get(cam_id)
