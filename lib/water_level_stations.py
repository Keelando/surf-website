"""Central registry for stations consumed by water_level_export.py.

Each entry pairs a tide-prediction/observation station (key into
lib.stations.STATIONS.tides) with the storm-surge forecast source it should
use and the JSON output key under which its observed surge is published.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class WaterLevelStation:
    tide_key: str
    surge_source: Optional[str]
    observed_key: Optional[str]
    is_surrey: bool = False


WATER_LEVEL_STATIONS = (
    WaterLevelStation("point_atkinson", "Point_Atkinson", "Point_Atkinson"),
    WaterLevelStation("campbell_river", "Campbell_River", "Campbell_River"),
    WaterLevelStation("crescent_pile", "Crescent_Beach_Channel", "Crescent_Beach_Channel"),
    WaterLevelStation("tofino", "Tofino", "Tofino"),
    # Port Renfrew uses Neah Bay surge data (~30 km away); observed surge not exported
    # because there is no surge-station-aligned hindcast key for it.
    WaterLevelStation("port_renfrew", "Neah_Bay", None),
    # Surrey geodetic stations: forecast uses the Crescent_Beach_Channel surge feed
    # (channel for both; the ocean station is ~300 m away), observed publishes under
    # its own JSON key to keep them distinguishable on the hindcast plot.
    WaterLevelStation(
        "crescent_channel_ocean",
        "Crescent_Beach_Channel",
        "Crescent_Channel_Ocean",
        is_surrey=True,
    ),
    WaterLevelStation(
        "crescent_beach_ocean",
        "Crescent_Beach_Channel",
        "Crescent_Beach_Ocean",
        is_surrey=True,
    ),
)
