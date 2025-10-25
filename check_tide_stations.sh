#!/bin/bash
# Check DFO IWLS tide stations with coordinates and series info

STATIONS=(
  5cebf1de3d0f4a073c4bb94c  # Point Atkinson
  5cebf1e43d0f4a073c4bc404  # Kitsilano
  5cebf1de3d0f4a073c4bb935  # Tsawwassen
  5cebf1de3d0f4a073c4bb933  # White Rock
  5dd3064fe0fdc4b9b4be69d7  # Crescent Beach
  5cebf1de3d0f4a073c4bba2d  # Nanaimo
  5cebf1e13d0f4a073c4bbf93  # New Westminster
  5cebf1de3d0f4a073c4bb996  # Campbell River
)

for id in "${STATIONS[@]}"; do
  echo
  echo "===== $id ====="
  curl -s "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/$id" \
    | jq '{code, officialName, type, operating, latitude, longitude, timeSeries: [.timeSeries[].code]}'
  sleep 4
done
