# Data Pipeline Refactoring Plan

**Created:** 2025-11-10
**Status:** Assessment & Planning Phase
**Goal:** Reorganize project structure for sustainable growth and maintainability

---

## Executive Summary

The marine weather monitoring system has grown organically from 6 scripts to **17+ Python scripts** with multiple data sources, export formats, and integration points. The root directory has become cluttered (33 files), making it difficult to:

- Locate related functionality quickly
- Understand the pipeline at a glance
- Add new data sources without creating more clutter
- Separate production code from experiments/migrations
- Maintain consistent configuration management

**Recommendation:** Implement a phased refactoring using standard Python project structure with clear separation of concerns.

---

## Current State Assessment

### Directory Structure (As-Is)

```
surf-website/
├── [ROOT - 17 Python scripts, 8 MD files, 33 total files]
│   ├── buoy_to_influx_sqlite.py          # EC buoy XML parser
│   ├── fetch_noaa_buoy.py                 # NOAA buoy fetcher
│   ├── tide_to_sqlite.py                  # DFO tide fetcher
│   ├── fetch_storm_surge.py               # GDSPS storm surge fetcher
│   ├── parse_marine_forecast.py           # Marine forecast parser
│   ├── sqlite_to_json.py                  # Latest snapshot exporter
│   ├── export_24hr_timeseries.py          # Timeseries exporter
│   ├── export_tide_json.py                # Tide JSON exporter
│   ├── export_hindcast_json.py            # Hindcast exporter
│   ├── export_combined_water_level.py     # Combined water level exporter
│   ├── influx_to_mqtt.py                  # MQTT publisher
│   ├── calculate_storm_surge_observed.py  # Observed surge calculator
│   ├── stations.py                        # Station registry module
│   ├── validate_stations.py               # Station validator
│   ├── fetch_surrey_wave_v2.py            # Surrey integration (experimental?)
│   ├── update_exports_for_surrey.py       # Surrey export updater
│   ├── compare_surrey_dfo_water_levels.py # Surrey comparison tool
│   ├── CLAUDE.md                          # Project instructions (large)
│   ├── README.md
│   ├── TODO.md
│   ├── SURREY_*.md (3 files)              # Surrey-specific docs
│   ├── FIREFOX_ARROW_BUG.md
│   ├── MARINE_FORECAST_FRONTEND_SUMMARY.md
│   ├── cron.txt                           # Cron schedule reference
│   ├── marine_forecast.conf               # sr3 config (should be in ~/.config/)
│   ├── stations.json                      # Master station registry
│   ├── requirements.txt
│   ├── surrey wave integration.zip        # Orphaned artifact
│   ├── deploy_surrey_integration.sh
│   └── CLAUDE.md.backup-2025-11-05        # Backup file
│
├── docs/ (7 markdown files)
│   ├── ARCHITECTURE_DETAILED.md
│   ├── COMMANDS.md
│   ├── DEPLOYMENT.md
│   ├── TROUBLESHOOTING.md
│   ├── STORM_SURGE_SETUP.md
│   ├── GDSPS_AND_WAVE_EFFECTS.md
│   └── PR_AUTOMATION_GUIDE.md
│
├── tests/ (well-organized)
│   ├── fixtures/
│   │   ├── dfo_iwls/*.json
│   │   └── storm_surge/*.json
│   ├── databases/
│   ├── create_test_*.py (2 scripts)
│   ├── diagnose_hindcast_duplicates.py
│   ├── validate_hindcast_timestamps.py
│   ├── setup_offline_test.sh
│   ├── run_tide_test_workflow.sh
│   └── *.md (3 documentation files)
│
├── stations_migration/ (orphaned)
│   ├── Old copy of stations.py
│   ├── validate_stations.py
│   ├── migration_guide.py
│   ├── deploy_stations.sh
│   └── *.md (4 documentation files)
│
├── examples/ (underutilized)
│   ├── buoy_data_example.json
│   ├── storm_surge_example.json
│   └── README.md
│
└── scripts/ (almost empty)
    └── generate_pr_description.sh
```

### Pain Points Identified

#### 1. **Root Directory Clutter (Critical)**
- **Impact:** Hard to navigate, overwhelming for new contributors
- **Severity:** HIGH
- 17 Python scripts with no clear grouping
- Mixed concerns (ingestion, processing, export, utilities)
- Impossible to understand pipeline flow at a glance

#### 2. **Mixed Production vs Experimental Code (High)**
- **Impact:** Confusion about what's active vs deprecated
- **Severity:** HIGH
- Surrey integration files (3 scripts + 3 docs) - unclear if production or POC
- `stations_migration/` - completed migration, should be archived
- `.backup` files and `.zip` artifacts in root
- No clear "contrib" or "experimental" area

#### 3. **Configuration Management (Medium)**
- **Impact:** Hard to locate and update configs
- **Severity:** MEDIUM
- `marine_forecast.conf` in root (should be with other sr3 configs or in config/)
- `cron.txt` in root (deployment-specific, should be in docs/deployment/)
- `stations.json` in root (data file, could be in config/ or data/)

#### 4. **Documentation Fragmentation (Medium)**
- **Impact:** Hard to maintain consistency, find information
- **Severity:** MEDIUM
- 8 MD files in root (README, CLAUDE, TODO, 3 Surrey docs, Firefox bug, Marine forecast)
- 7 MD files in docs/
- 3 MD files in tests/
- 4 MD files in stations_migration/
- **Total: 22 markdown files** across 4 directories

#### 5. **Lack of Module Structure (Medium)**
- **Impact:** Code reuse difficult, imports messy
- **Severity:** MEDIUM
- No `src/` or package structure
- All scripts standalone (good for cron, bad for code sharing)
- Utilities like `stations.py` mixed with pipeline scripts

#### 6. **No Clear Functional Grouping (High)**
- **Impact:** Hard to understand pipeline stages
- **Severity:** HIGH
- Data ingestion scripts scattered (buoy_to_influx, fetch_noaa, tide_to_sqlite, fetch_storm_surge)
- Export scripts scattered (sqlite_to_json, export_24hr, export_tide, export_hindcast, export_combined)
- Processing scripts mixed in (calculate_storm_surge_observed, parse_marine_forecast)
- Integration scripts unclear (influx_to_mqtt, Surrey scripts)

#### 7. **Test Organization (Low)**
- **Impact:** Minimal, tests are well-organized
- **Severity:** LOW
- Tests directory is actually good!
- Diagnostic scripts in tests/ could potentially move to tools/

---

## Proposed Structure (To-Be)

### Design Principles

1. **Separation of Concerns:** Group by functional responsibility
2. **Discoverability:** Clear naming, logical hierarchy
3. **Scalability:** Easy to add new data sources/exporters
4. **Backward Compatibility:** Preserve cron job paths (or provide migration script)
5. **Standard Python Layout:** Follow community best practices (src/ layout)

### Recommended Directory Structure

```
surf-website/
│
├── README.md                      # Project overview
├── CHANGELOG.md                   # Version history
├── requirements.txt               # Python dependencies
├── pyproject.toml                 # (Future) Package metadata
├── .gitignore
│
├── src/                           # NEW: All production pipeline code
│   │
│   ├── core/                      # Shared utilities and modules
│   │   ├── __init__.py
│   │   ├── stations.py            # Station registry (moved from root)
│   │   ├── database.py            # (Future) DB connection helpers
│   │   ├── logging_config.py      # (Future) Centralized logging
│   │   └── constants.py           # (Future) Shared constants
│   │
│   ├── ingestion/                 # Data fetching scripts
│   │   ├── __init__.py
│   │   ├── buoy_ec.py             # (renamed) buoy_to_influx_sqlite.py
│   │   ├── buoy_noaa.py           # (renamed) fetch_noaa_buoy.py
│   │   ├── tide_dfo.py            # (renamed) tide_to_sqlite.py
│   │   ├── storm_surge_gdsps.py   # (renamed) fetch_storm_surge.py
│   │   └── marine_forecast_ec.py  # (renamed) parse_marine_forecast.py
│   │
│   ├── processing/                # Data processing/calculation scripts
│   │   ├── __init__.py
│   │   └── storm_surge_observed.py # (renamed) calculate_storm_surge_observed.py
│   │
│   ├── export/                    # JSON/file exporters
│   │   ├── __init__.py
│   │   ├── buoy_latest.py         # (renamed) sqlite_to_json.py
│   │   ├── buoy_timeseries.py     # (renamed) export_24hr_timeseries.py
│   │   ├── tide.py                # (renamed) export_tide_json.py
│   │   ├── hindcast.py            # (renamed) export_hindcast_json.py
│   │   └── combined_water_level.py # (renamed) export_combined_water_level.py
│   │
│   ├── integration/               # External system integrations
│   │   ├── __init__.py
│   │   ├── mqtt_publisher.py      # (renamed) influx_to_mqtt.py
│   │   └── surrey/                # Surrey-specific integration
│   │       ├── __init__.py
│   │       ├── fetch_wave.py      # (renamed) fetch_surrey_wave_v2.py
│   │       ├── update_exports.py  # (renamed) update_exports_for_surrey.py
│   │       └── compare_water_levels.py # (renamed) compare_surrey_dfo_water_levels.py
│   │
│   └── validation/                # Data validation tools
│       ├── __init__.py
│       └── stations.py            # (renamed) validate_stations.py
│
├── config/                        # NEW: Configuration files
│   ├── stations.json              # (moved from root)
│   ├── sr3/                       # sr3 configs (or symlink to ~/.config/sr3/)
│   │   └── marine_forecast.conf   # (moved from root)
│   └── logging.yaml               # (Future) Logging configuration
│
├── docs/                          # ALL documentation consolidated here
│   ├── README.md                  # Documentation index
│   ├── project/                   # Project-level docs
│   │   ├── CLAUDE.md              # (moved from root)
│   │   ├── TODO.md                # (moved from root)
│   │   └── CHANGELOG.md           # (link to root)
│   ├── architecture/
│   │   ├── ARCHITECTURE_DETAILED.md
│   │   ├── DATA_FLOW.md           # (Future) Visual pipeline diagram
│   │   └── GDSPS_AND_WAVE_EFFECTS.md
│   ├── deployment/
│   │   ├── DEPLOYMENT.md
│   │   ├── cron.txt               # (moved from root)
│   │   ├── MIGRATION_GUIDE.md     # (NEW) How to migrate to new structure
│   │   └── STORM_SURGE_SETUP.md
│   ├── operations/
│   │   ├── COMMANDS.md
│   │   ├── TROUBLESHOOTING.md
│   │   └── MONITORING.md          # (Future) Observability guide
│   ├── integrations/
│   │   ├── SURREY_INTEGRATION_GUIDE.md # (moved from root)
│   │   ├── SURREY_DEPLOYMENT.md   # (moved from root)
│   │   ├── SURREY_FRONTEND_GUIDE.md # (moved from root)
│   │   └── MQTT_HOMEASSISTANT.md  # (Future)
│   ├── frontend/
│   │   ├── MARINE_FORECAST_FRONTEND_SUMMARY.md # (moved from root)
│   │   └── FIREFOX_ARROW_BUG.md   # (moved from root)
│   └── development/
│       └── PR_AUTOMATION_GUIDE.md
│
├── tests/                         # Keep existing structure (good!)
│   ├── unit/                      # (Future) Unit tests
│   ├── integration/               # (Future) Integration tests
│   ├── fixtures/                  # Test data (keep as-is)
│   ├── databases/                 # Test databases (keep as-is)
│   ├── create_test_*.py           # Test setup scripts
│   ├── diagnose_*.py              # Diagnostic tools
│   └── *.sh                       # Test automation scripts
│
├── tools/                         # NEW: Developer/operator utilities
│   ├── migration/                 # One-time migration scripts
│   │   ├── migrate_to_new_structure.sh  # (NEW) Automated migration
│   │   └── archive/               # Completed migrations
│   │       └── stations_migration_2025-11/ # (moved from root)
│   ├── diagnostics/               # Operational diagnostic tools
│   │   └── README.md              # What tools exist and when to use them
│   └── deployment/
│       └── deploy_surrey_integration.sh # (moved from root)
│
├── scripts/                       # Cron-compatible entry points (symlinks or wrappers)
│   ├── ingest_buoy_ec.sh          # Wrapper for src/ingestion/buoy_ec.py
│   ├── ingest_buoy_noaa.sh        # Wrapper for src/ingestion/buoy_noaa.py
│   ├── export_buoy_latest.sh      # Wrapper for src/export/buoy_latest.py
│   └── ...                        # (One script per cron job)
│
├── examples/                      # Keep and enhance
│   ├── README.md
│   ├── data/                      # Sample data files
│   │   ├── buoy_data_example.json
│   │   └── storm_surge_example.json
│   └── notebooks/                 # (Future) Jupyter notebooks for exploration
│
└── archive/                       # NEW: Deprecated/obsolete code
    ├── README.md                  # What's here and why
    ├── surrey_wave_integration.zip # (moved from root)
    └── backups/
        └── CLAUDE.md.backup-2025-11-05 # (moved from root)
```

---

## Migration Strategy

### Phase 1: Planning & Preparation (Week 1)
**Goal:** Validate plan, prepare tooling, no code changes

**Tasks:**
1. ✅ Review and approve this refactoring plan
2. Create comprehensive test suite to verify functionality post-migration
   - Test all cron jobs execute successfully
   - Verify JSON exports match expected schema
   - Check database writes work correctly
3. Create migration script (`tools/migration/migrate_to_new_structure.sh`)
   - Automated file moves with git history preservation
   - Generate symlinks for backward compatibility
   - Validation checks
4. Document rollback procedure
5. Create feature branch for refactoring

**Deliverables:**
- [ ] Approved refactoring plan
- [ ] Migration automation script
- [ ] Test suite covering critical paths
- [ ] Rollback documentation

---

### Phase 2: Low-Risk Moves (Week 2)
**Goal:** Relocate non-code files, no functional changes

**Tasks:**
1. Create new directory structure (empty folders)
2. Move documentation files:
   - Root MD files → docs/
   - Organize into subdirectories (project/, deployment/, integrations/, etc.)
   - Update all cross-references and links
3. Move configuration files:
   - `stations.json` → config/
   - `marine_forecast.conf` → config/sr3/
   - `cron.txt` → docs/deployment/
4. Move artifacts to archive:
   - `surrey wave integration.zip` → archive/
   - `CLAUDE.md.backup-*` → archive/backups/
   - `stations_migration/` → tools/migration/archive/
5. Update .gitignore if needed
6. Test: Verify no broken documentation links

**Deliverables:**
- [ ] Cleaner root directory (no MD files except README)
- [ ] Organized docs/ structure
- [ ] Updated documentation cross-references
- [ ] Git commit: "docs: reorganize documentation structure"

**Risk:** LOW (no code execution changes)

---

### Phase 3: Create Module Structure (Week 3)
**Goal:** Establish src/ layout, move shared modules

**Tasks:**
1. Create src/ directory with subdirectories:
   - core/, ingestion/, processing/, export/, integration/, validation/
2. Move and rename shared modules first:
   - `stations.py` → src/core/stations.py
   - `validate_stations.py` → src/validation/stations.py
3. Update import statements in all scripts that use stations.py
4. Add `__init__.py` files to all packages
5. Update PYTHONPATH or install as editable package (`pip install -e .`)
6. Test: Run all scripts, verify imports work

**Deliverables:**
- [ ] src/ directory structure created
- [ ] stations.py successfully moved
- [ ] All imports updated and tested
- [ ] Git commit: "refactor: create src/ module structure"

**Risk:** MEDIUM (changes imports, but isolated to stations module)

---

### Phase 4: Move Ingestion Scripts (Week 4)
**Goal:** Relocate data fetching scripts with minimal disruption

**Tasks:**
1. Move ingestion scripts one at a time:
   - `buoy_to_influx_sqlite.py` → src/ingestion/buoy_ec.py
   - `fetch_noaa_buoy.py` → src/ingestion/buoy_noaa.py
   - `tide_to_sqlite.py` → src/ingestion/tide_dfo.py
   - `fetch_storm_surge.py` → src/ingestion/storm_surge_gdsps.py
   - `parse_marine_forecast.py` → src/ingestion/marine_forecast_ec.py
2. For each script:
   - Update internal imports (if any)
   - Create wrapper script in root (for backward compat) OR update cron.txt
   - Test manually before moving to next script
3. Update cron.txt with new paths (if not using wrappers)
4. Test: Run cron jobs in test environment

**Deliverables:**
- [ ] All ingestion scripts moved to src/ingestion/
- [ ] Cron jobs updated (or wrappers created)
- [ ] Tested in non-production environment
- [ ] Git commit: "refactor: move ingestion scripts to src/"

**Risk:** HIGH (affects production data collection)
**Mitigation:** Test each script individually, maintain backward compat wrappers

---

### Phase 5: Move Export Scripts (Week 5)
**Goal:** Relocate export scripts, update cron jobs

**Tasks:**
1. Move export scripts one at a time:
   - `sqlite_to_json.py` → src/export/buoy_latest.py
   - `export_24hr_timeseries.py` → src/export/buoy_timeseries.py
   - `export_tide_json.py` → src/export/tide.py
   - `export_hindcast_json.py` → src/export/hindcast.py
   - `export_combined_water_level.py` → src/export/combined_water_level.py
2. Update cron.txt or wrapper scripts
3. Test: Verify JSON exports match previous output (byte-for-byte if possible)

**Deliverables:**
- [ ] All export scripts moved to src/export/
- [ ] JSON output validated (no regressions)
- [ ] Git commit: "refactor: move export scripts to src/"

**Risk:** MEDIUM (affects website data, but easy to verify)

---

### Phase 6: Move Integration & Processing Scripts (Week 6)
**Goal:** Relocate remaining pipeline scripts

**Tasks:**
1. Move processing scripts:
   - `calculate_storm_surge_observed.py` → src/processing/storm_surge_observed.py
2. Move integration scripts:
   - `influx_to_mqtt.py` → src/integration/mqtt_publisher.py
3. Move Surrey integration scripts:
   - Create src/integration/surrey/
   - Move fetch_surrey_wave_v2.py, update_exports_for_surrey.py, compare_surrey_dfo_water_levels.py
4. Update cron.txt or wrappers
5. Test: Verify MQTT messages, Surrey integration still works

**Deliverables:**
- [ ] All processing/integration scripts moved
- [ ] Surrey integration isolated in subdirectory
- [ ] Git commit: "refactor: move processing and integration scripts"

**Risk:** MEDIUM (MQTT integration, Surrey may be experimental)

---

### Phase 7: Cleanup & Optimization (Week 7)
**Goal:** Remove wrappers, finalize structure, document

**Tasks:**
1. If using backward-compat wrappers, remove them after cron migration
2. Clean up root directory:
   - Verify only README.md, requirements.txt, .gitignore remain
   - Move any remaining orphaned files
3. Create scripts/ directory with clean cron entry points (if desired)
4. Update all documentation to reflect new structure:
   - CLAUDE.md (paths, examples)
   - COMMANDS.md (command examples)
   - DEPLOYMENT.md (cron jobs)
5. Create MIGRATION_GUIDE.md documenting what changed
6. Final testing: Run full pipeline end-to-end
7. Update TODO.md with completed refactoring

**Deliverables:**
- [ ] Clean root directory (<10 files)
- [ ] All documentation updated
- [ ] MIGRATION_GUIDE.md created
- [ ] Git commit: "refactor: finalize directory structure cleanup"

**Risk:** LOW (cleanup phase)

---

### Phase 8: Production Deployment (Week 8)
**Goal:** Deploy to production, monitor, validate

**Tasks:**
1. Create production deployment plan:
   - Maintenance window (if needed)
   - Rollback trigger criteria
   - Communication plan
2. Backup production databases and configs
3. Deploy refactored code:
   - Update crontab with new paths
   - Restart sr3 subscriptions (if configs changed)
   - Monitor logs for first 24 hours
4. Validate:
   - All cron jobs execute successfully
   - JSON exports continue updating
   - MQTT messages publishing
   - Website displays data correctly
5. Archive old codebase (if separate deployment)
6. Merge feature branch to main

**Deliverables:**
- [ ] Production deployment successful
- [ ] 24-hour stability validation
- [ ] Rollback plan ready (but not needed)
- [ ] Git tag: v2.0.0-refactored

**Risk:** MEDIUM (production deployment always carries risk)
**Mitigation:** Thorough testing in phases 1-7, rollback plan ready

---

## Benefits of Proposed Structure

### Immediate Benefits
1. **Discoverability:** New contributors can understand pipeline in 30 seconds
2. **Reduced Cognitive Load:** Root directory has <10 files instead of 33
3. **Clear Separation:** Ingestion vs export vs processing obvious at a glance
4. **Better Documentation:** All docs centralized with clear organization

### Long-Term Benefits
1. **Scalability:** Adding new data source = create file in src/ingestion/
2. **Code Reuse:** Shared utilities in src/core/ encourage DRY principles
3. **Testing:** Easier to write unit tests for modular code
4. **Onboarding:** CLAUDE.md can reference clear structure instead of flat list
5. **Professionalization:** Standard Python layout → easier to package, distribute

### Maintenance Benefits
1. **Reduced Clutter:** Deprecated code moves to archive/ instead of staying in root
2. **Clear Config Management:** All configs in config/, not scattered
3. **Easier Debugging:** Log what module failed, navigate directly to src/ingestion/
4. **Better Git History:** Meaningful directory structure in commits

---

## Alternative Approaches Considered

### Option A: Minimal Refactoring (Rejected)
**Approach:** Just move docs, leave scripts in root
**Pros:** Low effort, low risk
**Cons:** Doesn't solve core problem (17 scripts in root), not sustainable
**Decision:** Rejected - doesn't address growth concerns

### Option B: Full Python Package with setup.py (Deferred)
**Approach:** Convert to installable package, use entry points for scripts
**Pros:** Most professional, enables pip install, clear imports
**Cons:** Higher complexity, requires PYTHONPATH changes, bigger migration
**Decision:** Deferred to future phase - structure now supports this later

### Option C: Monorepo with Separate Packages (Overkill)
**Approach:** Separate packages for ingestion, export, processing
**Pros:** Maximum modularity
**Cons:** Over-engineering for current scale, deployment complexity
**Decision:** Rejected - not justified for current project size

---

## Risk Assessment

### High-Risk Areas
1. **Cron job path changes** - Production data collection depends on these
   - Mitigation: Test each change individually, maintain symlinks initially
2. **Import path changes** - Python imports can be fragile
   - Mitigation: Comprehensive test suite, incremental migration
3. **Configuration file paths** - Hard-coded paths in scripts
   - Mitigation: Audit all path references, use config/ directory consistently

### Medium-Risk Areas
1. **MQTT integration** - External system dependency
   - Mitigation: Test in isolated environment, verify messages post-migration
2. **Surrey integration** - Unclear if production or experimental
   - Mitigation: Clarify with stakeholders before moving

### Low-Risk Areas
1. **Documentation moves** - No code execution
2. **Archive moves** - Not referenced by active code
3. **Test directory** - Already well-organized

---

## Success Criteria

### Technical Criteria
- [ ] All cron jobs execute without errors
- [ ] JSON exports byte-identical to pre-refactor (or documented differences)
- [ ] MQTT messages publishing successfully
- [ ] Database writes functioning correctly
- [ ] Website displays data with no regressions
- [ ] Zero new exceptions in logs (first 72 hours)

### Organizational Criteria
- [ ] Root directory has <10 files
- [ ] All documentation in docs/ with clear hierarchy
- [ ] New contributor can locate ingestion scripts in <60 seconds
- [ ] CLAUDE.md references clear structure
- [ ] All migration decisions documented in MIGRATION_GUIDE.md

### Sustainability Criteria
- [ ] Adding new data source takes <30 minutes (file in src/ingestion/, update cron)
- [ ] Code reuse pattern established (src/core/ utilities)
- [ ] Test coverage increased (easier to test modular code)
- [ ] Deprecated code has clear home (archive/)

---

## Post-Refactoring Roadmap

### Immediate Follow-Ups (Month 2)
1. Add unit tests for critical modules
2. Create integration test suite
3. Set up pre-commit hooks (linting, type checking)
4. Implement centralized logging (src/core/logging_config.py)

### Medium-Term Improvements (Months 3-6)
1. Convert to installable package (setup.py or pyproject.toml)
2. Add type hints throughout codebase (mypy validation)
3. Create developer documentation (CONTRIBUTING.md)
4. Implement automated deployment (CI/CD)
5. Add monitoring/alerting for pipeline failures

### Long-Term Vision (Year 2)
1. Extract shared components to separate library (reusable across projects)
2. Add web UI for pipeline monitoring
3. Containerize pipeline (Docker/Docker Compose)
4. Implement plugin architecture for data sources
5. Consider migrating from cron to orchestrator (Apache Airflow, Luigi, Prefect)

---

## Open Questions for Discussion

### 1. Surrey Integration Status
**Question:** Is Surrey integration production, experimental, or deprecated?
**Impact:** Determines if it stays in src/integration/ or moves to archive/
**Decision needed by:** Phase 6 (Week 6)

### 2. Backward Compatibility Requirements
**Question:** Can we update cron.txt directly, or must we maintain old paths indefinitely?
**Impact:** Whether we create wrapper scripts or just update cron jobs
**Decision needed by:** Phase 4 (Week 4)

### 3. PYTHONPATH Management
**Question:** Should we install as editable package (`pip install -e .`) or manage PYTHONPATH manually?
**Impact:** How imports work, developer setup complexity
**Decision needed by:** Phase 3 (Week 3)

### 4. sr3 Configuration Location
**Question:** Keep sr3 configs in ~/.config/sr3/ or move to project config/?
**Impact:** Where sr3 looks for configs, deployment procedure
**Decision needed by:** Phase 2 (Week 2)

### 5. Test Database Location
**Question:** Keep in tests/databases/ or move to dedicated location outside repo?
**Impact:** Git ignore patterns, test setup scripts
**Decision needed by:** Phase 7 (Week 7)

---

## Timeline Summary

| Phase | Duration | Risk | Can Start |
|-------|----------|------|-----------|
| 1. Planning | 1 week | Low | Immediately |
| 2. Documentation | 1 week | Low | After Phase 1 |
| 3. Module Structure | 1 week | Medium | After Phase 2 |
| 4. Ingestion Scripts | 1 week | High | After Phase 3 |
| 5. Export Scripts | 1 week | Medium | After Phase 4 |
| 6. Integration/Processing | 1 week | Medium | After Phase 5 |
| 7. Cleanup | 1 week | Low | After Phase 6 |
| 8. Production Deploy | 1 week | Medium | After Phase 7 |

**Total Duration:** 8 weeks (2 months)
**Critical Path:** Phases 4-6 (high-risk code moves)
**Can parallelize:** Documentation updates (Phase 7) can start during earlier phases

---

## Approval & Sign-Off

**Prepared by:** Claude Code
**Date:** 2025-11-10
**Status:** Awaiting approval

**Stakeholder Sign-Off:**
- [ ] Project Owner - Approved for execution
- [ ] Technical Lead - Architecture reviewed
- [ ] Operations - Deployment plan acceptable

**Next Steps:**
1. Review this plan and provide feedback
2. Make go/no-go decision
3. If approved, create GitHub Project board for tracking
4. Begin Phase 1 (Planning & Preparation)

---

## Appendix A: File Move Mapping

Complete mapping of old → new locations (for migration script):

### Python Scripts
```
ROOT → src/ingestion/
  buoy_to_influx_sqlite.py       → buoy_ec.py
  fetch_noaa_buoy.py              → buoy_noaa.py
  tide_to_sqlite.py               → tide_dfo.py
  fetch_storm_surge.py            → storm_surge_gdsps.py
  parse_marine_forecast.py        → marine_forecast_ec.py

ROOT → src/processing/
  calculate_storm_surge_observed.py → storm_surge_observed.py

ROOT → src/export/
  sqlite_to_json.py               → buoy_latest.py
  export_24hr_timeseries.py       → buoy_timeseries.py
  export_tide_json.py             → tide.py
  export_hindcast_json.py         → hindcast.py
  export_combined_water_level.py  → combined_water_level.py

ROOT → src/integration/
  influx_to_mqtt.py               → mqtt_publisher.py

ROOT → src/integration/surrey/
  fetch_surrey_wave_v2.py         → fetch_wave.py
  update_exports_for_surrey.py    → update_exports.py
  compare_surrey_dfo_water_levels.py → compare_water_levels.py

ROOT → src/core/
  stations.py                     → stations.py (no rename)

ROOT → src/validation/
  validate_stations.py            → stations.py
```

### Documentation
```
ROOT → docs/project/
  CLAUDE.md
  TODO.md

ROOT → docs/frontend/
  MARINE_FORECAST_FRONTEND_SUMMARY.md
  FIREFOX_ARROW_BUG.md

ROOT → docs/integrations/
  SURREY_INTEGRATION_GUIDE.md
  SURREY_DEPLOYMENT.md
  SURREY_FRONTEND_GUIDE.md

ROOT → docs/deployment/
  cron.txt

stations_migration/ → tools/migration/archive/stations_migration_2025-11/
```

### Configuration
```
ROOT → config/
  stations.json

ROOT → config/sr3/
  marine_forecast.conf
```

### Archives
```
ROOT → archive/
  surrey wave integration.zip

ROOT → archive/backups/
  CLAUDE.md.backup-2025-11-05
```

---

## Appendix B: Migration Script Pseudocode

```bash
#!/bin/bash
# tools/migration/migrate_to_new_structure.sh

set -e  # Exit on error

echo "Starting migration to new directory structure..."

# 1. Create new directories
mkdir -p src/{core,ingestion,processing,export,integration/{,surrey},validation}
mkdir -p config/sr3
mkdir -p docs/{project,architecture,deployment,operations,integrations,frontend,development}
mkdir -p tools/{migration/archive,diagnostics,deployment}
mkdir -p archive/backups

# 2. Move Python scripts with git mv (preserves history)
git mv buoy_to_influx_sqlite.py src/ingestion/buoy_ec.py
git mv fetch_noaa_buoy.py src/ingestion/buoy_noaa.py
# ... (repeat for all scripts)

# 3. Move documentation
git mv CLAUDE.md docs/project/
git mv TODO.md docs/project/
# ... (repeat for all docs)

# 4. Move configs
git mv stations.json config/
git mv marine_forecast.conf config/sr3/
git mv cron.txt docs/deployment/

# 5. Move archives
git mv "surrey wave integration.zip" archive/
git mv CLAUDE.md.backup-2025-11-05 archive/backups/

# 6. Move stations_migration
git mv stations_migration tools/migration/archive/stations_migration_2025-11

# 7. Create __init__.py files
find src -type d -exec touch {}/__init__.py \;

# 8. Update imports (manual step, too complex for script)
echo "MANUAL STEP REQUIRED: Update import statements in moved files"
echo "  - Update stations.py imports to use src.core.stations"
echo "  - Add src/ to PYTHONPATH or install with pip install -e ."

# 9. Validation
echo "Validating migration..."
# Check that old files don't exist in root
if [ -f "buoy_to_influx_sqlite.py" ]; then
  echo "ERROR: Old files still in root"
  exit 1
fi

# Check that new structure exists
if [ ! -d "src/ingestion" ]; then
  echo "ERROR: New directory structure not created"
  exit 1
fi

echo "Migration complete! Next steps:"
echo "1. Review moved files with: git status"
echo "2. Update import statements (see migration guide)"
echo "3. Test all scripts manually"
echo "4. Update cron.txt with new paths"
echo "5. Commit changes: git commit -m 'refactor: reorganize directory structure'"
```

---

**End of Refactoring Plan**
