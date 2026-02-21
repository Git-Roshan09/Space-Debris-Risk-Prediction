# Directory Restructuring Summary

## Overview
Successfully restructured the Space Debris Risk Prediction project to follow big data best practices and industry standards.

## What Changed

### New Directory Structure
```
Space-Debris-Risk-Prediction/
├── config/                  [NEW] All configuration files
│   ├── airflow/            Airflow DAGs (moved from dags/)
│   ├── docker/             Docker configs (reserved)
│   └── hadoop/             Hadoop/HDFS configs (moved from hadoop-config/)
│
├── data/                    [NEW] Data storage with clear separation
│   ├── raw/                Raw input data (from Output/)
│   ├── processed/          Processed TLE histories (from Output/)
│   ├── external/           External dependencies (from Output/)
│   └── archive/            Historical/backup data
│
├── deployment/              [NEW] Infrastructure as Code
│   ├── docker-compose*.yml Moved from root
│   └── Dockerfile          Moved from root
│
├── pipelines/               [NEW] All data pipeline code
│   ├── ingestion/          Kafka producers, API (from src/kafka, src/demo/api)
│   ├── processing/         Spark jobs (from src/kafka)
│   └── orchestration/      Future: workflow orchestration
│
├── scripts/                 [NEW] Operational scripts
│   ├── setup/              Infrastructure setup (from script/)
│   ├── operations/         Day-to-day operations (from src/kafka/*.sh)
│   └── testing/            Test automation
│
├── notebooks/               Jupyter notebooks (kept as-is)
├── docs/                    [NEW] All documentation
└── logs/                    [NEW] Application logs
```

### File Migrations

#### Configuration Files
- `hadoop-config/*` → `config/hadoop/`
- `dags/*.py` → `config/airflow/`
- `docker-compose*.yml` → `deployment/`
- `Dockerfile` → `deployment/`

#### Data Files
- `Output/satellites_and_objects_catalog.csv` → `data/raw/`
- `Output/space_debris_catalog.csv` → `data/raw/`
- `Output/TLE_History/` → `data/processed/TLE_History/`
- `Output/TLE_History_Satellites/` → `data/processed/TLE_History_Satellites/`
- `Output/de421.bsp` → `data/external/`

#### Pipeline Code
- `src/kafka/*.py` → `pipelines/ingestion/`
- `src/demo/api/` → `pipelines/ingestion/api/`
- `browse_sgp4_data.py` → `pipelines/processing/`

#### Scripts
- `script/*` → `scripts/setup/`
- `src/kafka/*.sh` → `scripts/operations/`

#### Documentation
- `readme` → `docs/README_OLD.md`
- `PPT_OUTLINE.md` → `docs/`
- `src/kafka/*.md` → `docs/`

### Updated Files
The following files were updated with new paths:

1. **scripts/setup/start-containers.sh**
   - Updated to reference `deployment/docker-compose.yml`

2. **scripts/operations/start_streaming.sh**
   - Updated path to API: `pipelines/ingestion/api/`

3. **deployment/docker-compose.yml**
   - Volume mounts updated:
     - `./src/` → `../pipelines/`
     - `./Output/` → `../data/`
     - `./hadoop-config/` → `../config/hadoop/`
     - `./src/kafka/` → `../pipelines/ingestion/`

### New Documentation
Created comprehensive documentation:
- `README.md` - Main project README with new structure
- `data/README.md` - Data directory guide
- `scripts/README.md` - Scripts usage guide

## Benefits of New Structure

### 1. Clear Separation of Concerns
- **config/**: All configuration in one place
- **data/**: Clean data lifecycle (raw → processed)
- **pipelines/**: All data processing logic
- **deployment/**: Infrastructure as code
- **scripts/**: Operational tooling

### 2. Big Data Best Practices
- Follows industry-standard project layout
- Easier to scale and add new components
- Clear data lineage (raw → processed → archive)
- Separation of code, config, and data

### 3. Better Maintainability
- Easier onboarding for new team members
- Logical organization reduces cognitive load
- Facilitates CI/CD integration
- Clearer deployment boundaries

### 4. Production-Ready
- Config separated from code
- Data directories prepared for volume mounts
- Clear operational vs. development boundaries
- Ready for containerized deployment

## Next Steps

### Immediate Actions Required
1. **Update Python imports** in pipeline scripts if they reference old paths
2. **Update Airflow DAG paths** in Airflow configuration
3. **Verify Docker volume mounts** work correctly with new structure
4. **Update any hardcoded paths** in your code

### Testing Checklist
- [ ] Start containers: `./scripts/setup/start-containers.sh`
- [ ] Verify Hadoop config loads correctly
- [ ] Test Kafka producer with new paths
- [ ] Test Spark job with new volume mounts
- [ ] Verify Airflow DAGs are discovered
- [ ] Check data directories are accessible

### Recommended Additions
- Add `.gitignore` entries for `data/`, `logs/`, `.temp/`
- Create `data/.gitkeep`, `logs/.gitkeep` files
- Add environment variable configuration (`.env.example`)
- Create `deployment/README.md` for infrastructure docs
- Add `pipelines/README.md` for pipeline documentation

## Migration Commands

If you need to revert or reference original locations:
```bash
# Original locations still in git history
git log --follow -- <file_path>

# To restore a file from before restructuring:
git checkout <commit_hash> -- <file_path>
```

## Contact

For questions about the new structure, refer to:
- Main README: `README.md`
- Data directory: `data/README.md`
- Scripts guide: `scripts/README.md`
- Original readme: `docs/README_OLD.md`
