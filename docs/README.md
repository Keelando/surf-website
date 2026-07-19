# Documentation Index

Welcome to the Salish Sea Marine Monitoring System documentation.

## Project Documentation

High-level project information and planning:

- **[CLAUDE.md](../CLAUDE.md)** - Instructions for Claude Code (AI assistant context; repo root)
- **[TODO.md](../TODO.md)** - The single project todo list (maintenance backlog + feature backlog; repo root)
- **[WORKLOG.md](project/WORKLOG.md)** - Completed-work history
- **[NEXT_SESSION.md](project/NEXT_SESSION.md)** - Next session work plan
- **[BACKEND_AUDIT_2025-12-06.md](project/BACKEND_AUDIT_2025-12-06.md)** - Backend audit report (Dec 2025)
- **[BACKEND_TEST_RESULTS.md](../archive/docs/BACKEND_TEST_RESULTS.md)** - Testing results (Nov 2025, archived)

**Historical documentation:** Outdated refactoring plans and completed migration docs have been archived to `../archive/docs/` (2025-12-06)

## Deployment & Operations

Setup, configuration, and operational guides:

- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Server deployment and cron setup
- **[../config/crontab.txt](../config/crontab.txt)** - Complete production cron schedule
- **[WEBCAM_PIPELINE.md](WEBCAM_PIPELINE.md)** - Webcam capture, storage, and display system
- **[STORM_SURGE_SETUP.md](STORM_SURGE_SETUP.md)** - Storm surge forecast setup (GDSPS/GeoMet)
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Debugging guide and common issues
- **[BACKUP_STATUS.md](BACKUP_STATUS.md)** - Backup configuration and status
- **[SR3_MANAGEMENT.md](SR3_MANAGEMENT.md)** - Sarracenia (sr3) feed management

## Architecture & Commands

Technical reference and database information:

- **[ARCHITECTURE_DETAILED.md](ARCHITECTURE_DETAILED.md)** - Complete system architecture and schemas
- **[COMMANDS.md](COMMANDS.md)** - Command examples and database queries
- **[GDSPS_AND_WAVE_EFFECTS.md](GDSPS_AND_WAVE_EFFECTS.md)** - Storm surge and wave modeling details
- **[BUOY_DATA_GUIDE.md](BUOY_DATA_GUIDE.md)** - Buoy data sources and processing
- **[BUOY_PARAMETERS.md](BUOY_PARAMETERS.md)** - Wave and meteorological parameter definitions
- **[WEBSITE_PARAMETERS.md](WEBSITE_PARAMETERS.md)** - Frontend parameter reference
- **[PARAMETERS_QUICK_REFERENCE.md](PARAMETERS_QUICK_REFERENCE.md)** - Quick parameter lookup
- **[PR_AUTOMATION_GUIDE.md](PR_AUTOMATION_GUIDE.md)** - GitHub PR automation guide

## Integrations

External system integration documentation:

- **[SURREY_INTEGRATION_GUIDE.md](integrations/SURREY_INTEGRATION_GUIDE.md)** - Surrey FlowWorks wave data integration
- **[SURREY_DEPLOYMENT.md](integrations/SURREY_DEPLOYMENT.md)** - Surrey deployment steps
- **[SURREY_FRONTEND_GUIDE.md](integrations/SURREY_FRONTEND_GUIDE.md)** - Surrey frontend implementation
- **[JERICHO_PLANNING.md](integrations/JERICHO_PLANNING.md)** - Jericho Sailing Centre wind data integration

## Frontend

Website and UI documentation:

- **[MARINE_FORECAST_FRONTEND_SUMMARY.md](frontend/MARINE_FORECAST_FRONTEND_SUMMARY.md)** - Marine forecast UI implementation
- **Frontend directory:** `site/` — HTML/CSS/JS for halibutbank.ca (merged into this repo 2026-03-04)

## Feature Planning

Planned and in-progress features:

- **[TODO_WAVE_DIRECTION_VECTOR_VISUALIZATION.md](TODO_WAVE_DIRECTION_VECTOR_VISUALIZATION.md)** - Wave direction visualization feature
- **[FEATURE_BUOY_POSITION_DRIFT.md](FEATURE_BUOY_POSITION_DRIFT.md)** - Buoy position tracking and drift detection
- **[FEATURE_ENVIRONMENTAL_ASTRONOMICAL.md](FEATURE_ENVIRONMENTAL_ASTRONOMICAL.md)** - Sunrise/sunset, moon phase data
- **[FEATURE_COXBAY_WEBCAM.md](FEATURE_COXBAY_WEBCAM.md)** - Cox Bay webcam integration planning

## Quick Links

**For new developers:**
1. Start with [CLAUDE.md](project/CLAUDE.md) for system overview
2. Read [ARCHITECTURE_DETAILED.md](ARCHITECTURE_DETAILED.md) for technical details
3. Check [COMMANDS.md](COMMANDS.md) for common operations
4. See [DEPLOYMENT.md](DEPLOYMENT.md) for server setup

**For troubleshooting:**
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
- [COMMANDS.md](COMMANDS.md) - Database queries section

**For adding features:**
- [CLAUDE.md](project/CLAUDE.md) - Design principles and conventions
- [ARCHITECTURE_DETAILED.md](ARCHITECTURE_DETAILED.md) - System architecture
- Station metadata: `../config/stations.json`

---

**Live Site:** [halibutbank.ca](https://halibutbank.ca)

## System Overview

The Salish Sea Marine Monitoring System now includes:

- **9 Wave Buoys** - Environment Canada, NOAA, and NOAA C-MAN land stations
- **13 Wind Stations** - Environment Canada, US airports (KBLI, KORS), and JSCA Jericho
- **12 Tide Stations** - DFO IWLS with observations, predictions, and high/low events
- **23 Lightstations** - DFO manual weather reports (every 3 hours)
- **5 Webcams** - White Rock Pier, White Rock East Beach, Cox Bay, Mud Bay HD, Ambleside (Hollyburn Sailing Club)
- **Storm Surge Forecasts** - GeoMet GDSPS with combined water level modeling
- **Marine Forecasts** - Environment Canada zone forecasts and warnings

**Last updated:** December 2025
