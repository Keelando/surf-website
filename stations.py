#!/usr/bin/env python3
"""
Unified station metadata loader for Salish Sea marine monitoring.

Usage:
    from stations import STATIONS, get_buoy, get_tide_station, get_all_buoys
    
    # Get specific station
    halibut = get_buoy("4600146")
    print(f"{halibut['name']} is at {halibut['lat']}, {halibut['lon']}")
    
    # Get all buoys for processing
    for buoy_id, metadata in get_all_buoys().items():
        print(f"Processing {metadata['name']}...")
    
    # Get tide stations with observations available
    obs_stations = get_tide_stations(has_observations=True)
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

# Default path to stations.json (use PROJECT_ROOT from config)
from config import PROJECT_ROOT
STATIONS_FILE = PROJECT_ROOT / "config" / "stations.json"

class StationRegistry:
    """Central registry for all marine monitoring stations."""
    
    def __init__(self, stations_file: Path = STATIONS_FILE):
        self.stations_file = stations_file
        self._data = None
        self._load()
    
    def _load(self):
        """Load stations from JSON file."""
        if not self.stations_file.exists():
            raise FileNotFoundError(
                f"Stations file not found: {self.stations_file}\n"
                f"Please ensure stations.json is in the correct location."
            )
        
        with open(self.stations_file, 'r') as f:
            self._data = json.load(f)
    
    @property
    def buoys(self) -> Dict:
        """Get all buoy stations."""
        return self._data.get('buoys', {})
    
    @property
    def tides(self) -> Dict:
        """Get all tide stations."""
        return self._data.get('tides', {})

    @property
    def wind(self) -> Dict:
        """Get all wind stations."""
        return self._data.get('wind', {})
    
    @property
    def metadata(self) -> Dict:
        """Get file metadata."""
        return self._data.get('_metadata', {})
    
    def get_buoy(self, buoy_id: str) -> Optional[Dict]:
        """Get metadata for a specific buoy by ID."""
        return self.buoys.get(buoy_id)
    
    def get_tide_station(self, station_key: str) -> Optional[Dict]:
        """Get metadata for a specific tide station by key."""
        return self.tides.get(station_key)
    
    def get_tide_station_by_id(self, station_id: str) -> Optional[Dict]:
        """Get metadata for a tide station by DFO ID."""
        for station_key, data in self.tides.items():
            if data.get('id') == station_id:
                return data
        return None

    def get_wind_station(self, station_id: str) -> Optional[Dict]:
        """Get metadata for a specific wind station by ID."""
        return self.wind.get(station_id)
    
    def get_buoys_by_source(self, source: str) -> Dict:
        """Get all buoys from a specific source (e.g., 'Environment Canada', 'NOAA NDBC')."""
        return {
            bid: data for bid, data in self.buoys.items() 
            if data.get('source') == source
        }
    
    def get_tide_stations(
        self, 
        has_observations: bool = None,
        has_predictions: bool = None,
        station_type: str = None
    ) -> Dict:
        """
        Get tide stations filtered by available data types.
        
        Args:
            has_observations: If True, only return stations with 'wlo' series
            has_predictions: If True, only return stations with 'wlp' series
            station_type: Filter by type ('PERMANENT' or 'TEMPORARY')
        """
        filtered = {}
        
        for key, data in self.tides.items():
            # Apply type filter
            if station_type and data.get('type') != station_type:
                continue
            
            series = data.get('series', [])
            
            # Apply observation filter
            if has_observations is not None:
                has_obs = 'wlo' in series
                if has_observations != has_obs:
                    continue
            
            # Apply prediction filter
            if has_predictions is not None:
                has_pred = 'wlp' in series
                if has_predictions != has_pred:
                    continue
            
            filtered[key] = data
        
        return filtered
    
    def get_all_coordinates(self) -> List[Dict]:
        """Get coordinates for all stations (useful for mapping)."""
        coords = []
        
        for bid, data in self.buoys.items():
            coords.append({
                'type': 'buoy',
                'id': bid,
                'name': data['name'],
                'lat': data['lat'],
                'lon': data['lon']
            })
        
        for key, data in self.tides.items():
            coords.append({
                'type': 'tide',
                'id': key,
                'name': data['name'],
                'lat': data['lat'],
                'lon': data['lon']
            })
        
        return coords
    
    def validate_buoy_id(self, buoy_id: str) -> bool:
        """Check if a buoy ID exists."""
        return buoy_id in self.buoys
    
    def validate_tide_station(self, station_key: str) -> bool:
        """Check if a tide station key exists."""
        return station_key in self.tides


# Global instance for easy imports
STATIONS = StationRegistry()

# Convenience functions
def get_buoy(buoy_id: str) -> Optional[Dict]:
    """Get buoy metadata by ID."""
    return STATIONS.get_buoy(buoy_id)

def get_tide_station(station_key: str) -> Optional[Dict]:
    """Get tide station metadata by key."""
    return STATIONS.get_tide_station(station_key)

def get_all_buoys() -> Dict:
    """Get all buoy stations."""
    return STATIONS.buoys

def get_all_tides() -> Dict:
    """Get all tide stations."""
    return STATIONS.tides

def get_tide_stations(has_observations: bool = None) -> Dict:
    """Get tide stations, optionally filtered by observation availability."""
    return STATIONS.get_tide_stations(has_observations=has_observations)

def get_all_wind() -> Dict:
    """Get all wind stations."""
    return STATIONS.wind

def get_wind_station(station_id: str) -> Optional[Dict]:
    """Get wind station metadata by ID."""
    return STATIONS.get_wind_station(station_id)


# Example usage and testing
if __name__ == "__main__":
    print("🌊 Salish Sea Station Registry")
    print("=" * 60)
    
    # Test buoy access
    print(f"\n📊 Total Buoys: {len(STATIONS.buoys)}")
    for buoy_id, data in STATIONS.buoys.items():
        print(f"  • {data['name']} ({buoy_id}) - {data['source']}")
    
    # Test tide station access
    print(f"\n🌊 Total Tide Stations: {len(STATIONS.tides)}")
    obs_stations = STATIONS.get_tide_stations(has_observations=True)
    print(f"  • With observations: {len(obs_stations)}")
    for key, data in obs_stations.items():
        print(f"    - {data['name']} ({data['code']})")
    
    # Test specific lookups
    print(f"\n🔍 Test Lookups:")
    halibut = get_buoy("4600146")
    if halibut:
        print(f"  ✅ Halibut Bank: {halibut['lat']}, {halibut['lon']}")
    
    point_atk = get_tide_station("point_atkinson")
    if point_atk:
        print(f"  ✅ Point Atkinson: {point_atk['name']} (DFO ID: {point_atk['id']})")
    
    # Show Environment Canada buoys
    envcan = STATIONS.get_buoys_by_source("Environment Canada")
    print(f"\n🇨🇦 Environment Canada Buoys: {len(envcan)}")
    for bid, data in envcan.items():
        print(f"  • {data['name']} ({bid})")
