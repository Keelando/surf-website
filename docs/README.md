# Documentation Index

Welcome to the Salish Sea Marine Monitoring System documentation.

## Project Documentation

High-level project information and planning:

- **[CLAUDE.md](project/CLAUDE.md)** - Instructions for Claude Code (AI assistant context)
- **[TODO.md](project/TODO.md)** - Project todo list and upcoming work

**Historical documentation:** Outdated refactoring plans and completed migration docs have been archived to `../archive/docs/` (2025-12-06)

## Deployment & Operations

Setup, configuration, and operational guides:

- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Server deployment and cron setup
- **[../config/crontab.txt](../config/crontab.txt)** - Complete production cron schedule
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
- **Front-end repository:** [surf-website-front-end](https://github.com/Keelando/surf-website-front-end) - HTML/CSS/JS for halibutbank.ca

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

- **8 Wave Buoys** - Environment Canada, NOAA, and Surrey FlowWorks
- **11 Wind Stations** - Environment Canada, JSCA Jericho, White Rock Pier
- **12 Tide Stations** - DFO IWLS with observations, predictions, and high/low events
- **10 Lightstations** - DFO manual weather reports (hourly)
- **2 Webcams** - White Rock Pier and Boundary Bay (30-day archive)
- **Storm Surge Forecasts** - GeoMet GDSPS with combined water level modeling
- **Marine Forecasts** - Environment Canada zone forecasts and warnings

**Last updated:** December 2025
