# Documentation Archive Plan - 2025-12-06

## Outdated Documentation to Archive

### envcan_wave Repo

#### Completed Migration/Phase Docs (5 files)
✅ **Move to archive/docs/completed_migrations/**

1. `PHASE_3_CLEANUP.md` - Phase 3 logging migration (COMPLETED per LOGGING_MIGRATION_STATUS.md)
2. `PHASE_3_QUICKSTART.md` - Phase 3 quickstart (migration complete)
3. `LOGGING_MIGRATION_STATUS.md` - Status doc showing 100% complete
4. `LOGGING.md` - Logging guide (superseded by actual implementation in logging_config.py)

**Reason**: Migrations are 100% complete, these are historical planning/status docs

#### Abandoned/Superseded Refactoring Plans (6 files)
✅ **Move to archive/docs/refactoring_plans/**

5. `docs/project/PHASE0_READY_TO_EXECUTE.md` - Shows "NOT READY - CRITICAL BUGS" from 2025-01-13, appears abandoned
6. `docs/project/REFACTORING_PLAN.md` - Large refactoring plan (32KB, Nov 12)
7. `docs/project/REFACTORING_EXECUTION_GUIDE.md` - Execution guide (36KB, Nov 12)
8. `docs/project/CODE_REFACTORING_OPPORTUNITIES.md` - Opportunities doc (23KB, Nov 12)
9. `docs/project/HYBRID_REFACTORING_STRATEGY.md` - Strategy doc (12KB, Nov 12)
10. `docs/project/DIRECTORY_CLEANUP_PLAN.md` - Cleanup plan (16KB, Nov 13)
11. `docs/project/PRACTICAL_REFACTORING_PLAN.md` - Practical plan (18KB, Nov 13)

**Reason**: Multiple overlapping refactoring plans from same timeframe, likely superseded. If refactoring is ongoing, keep only the ACTIVE plan in docs/project/TODO.md

### site Repo

#### Historical Analysis/Planning Docs (6 files)
✅ **Move to archive/docs/historical_analysis/**

1. `docs/VERSION_CLEANUP_PLAN.md` - Version unification plan from Nov 14 (likely completed or obsolete)
2. `docs/FRONTEND_ANALYSIS_REPORT.md` - Analysis report from Nov 14 (24KB)
3. `docs/FRONTEND_ANALYSIS_SUMMARY.txt` - Summary text file
4. `docs/ANALYSIS_INDEX.md` - Index of analyses from Nov 14
5. `docs/FRAMEWORK_DISCUSSION.md` - Framework evaluation (historical)
6. `docs/READ_ME_FIRST.txt` - Possibly outdated intro text

**Reason**: Analysis/planning docs from Nov 14, likely superseded by current implementation

## Documentation to Keep (Active/Reference)

### envcan_wave - Keep Active
- ✅ `README.md` - Main project readme
- ✅ `BACKEND_AUDIT_2025-12-06.md` - Recent audit (today!)
- ✅ `BACKEND_TEST_RESULTS.md` - Test results (Nov 22)
- ✅ `docs/CLAUDE.md` - Active Claude Code guide
- ✅ `docs/ARCHITECTURE_DETAILED.md` - Architecture reference
- ✅ `docs/BUOY_DATA_GUIDE.md` - Data guide
- ✅ `docs/BUOY_PARAMETERS.md` - NEW parameter reference (today!)
- ✅ `docs/COMMANDS.md` - Command reference
- ✅ `docs/DEPLOYMENT.md` - Deployment guide
- ✅ `docs/TROUBLESHOOTING.md` - Troubleshooting guide
- ✅ `docs/STORM_SURGE_SETUP.md` - Storm surge guide
- ✅ `docs/project/TODO.md` - Current project todos
- ✅ `docs/project/CLAUDE.md` - Project guidance
- ✅ All `docs/integrations/*.md` - Integration guides
- ✅ All `docs/frontend/*.md` - Frontend integration docs
- ✅ All `tests/*.md` - Testing documentation

### site - Keep Active
- ✅ `README.md` - Main readme
- ✅ `UI_IMPROVEMENT_PLAN.md` - Recent UI plan (Nov 29)
- ✅ `docs/CLAUDE.md` - Claude Code guide (Nov 20)
- ✅ `docs/DEVELOPMENT_GUIDELINES.md` - Dev guidelines (Nov 23)
- ✅ `docs/REFACTORING_EXAMPLES.md` - Recent examples (Nov 29)
- ✅ `docs/HINDCAST_METHODOLOGY.md` - Methodology (Nov 23)
- ✅ `docs/FRONTEND_CHANGELOG.md` - Changelog (active)
- ✅ `docs/WARNING_BANNER_UPGRADE_SUMMARY.md` - Recent upgrade (Nov 5)
- ✅ `docs/BROWSER_STATE_EXPLAINED.md` - State management docs
- ✅ `docs/STATE_QUICK_REFERENCE.md` - Quick reference
- ✅ `docs/BACKEND_CONNECTION.md` - Backend connection docs
- ✅ `docs/CACHE_BUSTING.md` - Cache busting (Nov 15)
- ✅ `docs/FUTURE_ENHANCEMENTS.md` - Future plans
- ✅ `docs/DISMISSIBLE_WARNINGS_SUMMARY.md` - Feature summary
- ✅ `docs/MOBILE_UX_IMPROVEMENTS.md` - UX improvements

## Archive Structure

```
envcan_wave/
├── archive/
│   └── docs/
│       ├── completed_migrations/
│       │   ├── PHASE_3_CLEANUP.md
│       │   ├── PHASE_3_QUICKSTART.md
│       │   ├── LOGGING_MIGRATION_STATUS.md
│       │   └── LOGGING.md
│       └── refactoring_plans/
│           ├── PHASE0_READY_TO_EXECUTE.md
│           ├── REFACTORING_PLAN.md
│           ├── REFACTORING_EXECUTION_GUIDE.md
│           ├── CODE_REFACTORING_OPPORTUNITIES.md
│           ├── HYBRID_REFACTORING_STRATEGY.md
│           ├── DIRECTORY_CLEANUP_PLAN.md
│           └── PRACTICAL_REFACTORING_PLAN.md

site/
└── archive/
    └── docs/
        └── historical_analysis/
            ├── VERSION_CLEANUP_PLAN.md
            ├── FRONTEND_ANALYSIS_REPORT.md
            ├── FRONTEND_ANALYSIS_SUMMARY.txt
            ├── ANALYSIS_INDEX.md
            ├── FRAMEWORK_DISCUSSION.md
            └── READ_ME_FIRST.txt
```

## Total Files to Archive

- **envcan_wave**: 11 files (4 migration + 7 refactoring)
- **site**: 6 files (analysis/planning)
- **Total**: 17 files

## Benefits

1. **Cleaner documentation structure** - Active docs easier to find
2. **Preserve history** - Old docs archived, not deleted
3. **Reduced confusion** - No mixing of completed/abandoned plans with active docs
4. **Better onboarding** - New contributors see only relevant docs
