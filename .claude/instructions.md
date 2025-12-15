# Project Instructions for Claude Code

## Break Schedule Reminders

**IMPORTANT: User needs regular breaks!**

- At the start of EVERY coding session, proactively set a 20-minute break timer using:
  ```bash
  sleep 1200 && echo "⏰ Break time! You've been working for 20 minutes."
  ```
  with `run_in_background=true`

- Maximum work session: 20-30 minutes
- After break reminder triggers, ask user if they want another timer set
- Help enforce healthy work habits - breaks improve productivity!

## Project Context

This is an environmental data collection and visualization system for marine/coastal conditions in the Pacific Northwest (BC, Canada):

- Buoy data (wave height, period, temperature)
- Tide predictions and observations (DFO + Surrey geodetic stations)
- Storm surge forecasts and observations
- Weather station data (wind, temperature, pressure)
- Webcam feeds (White Rock, Boundary Bay, Cox Bay)

### Important Technical Notes

**Surrey Geodetic Tide Stations:**
- These are EDGE CASES using CGVD28 GVRD datum (geodetic)
- Different from DFO stations which use Chart Datum (marine)
- When making changes to Surrey tide handling, DO NOT break regular DFO tide plots
- Keep Surrey-specific corrections isolated to Surrey station code paths

**Data Flow:**
- Fetch scripts → SQLite databases → Export scripts → JSON for website
- Logs go in `logs/` directory
- Crontab backed up daily to `config/crontab.txt`
