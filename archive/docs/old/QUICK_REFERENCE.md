# Quick Reference - New Directory Structure

## Common Tasks

### Starting the Infrastructure
```bash
# From project root
./scripts/setup/start-containers.sh
```

### Running the Pipeline

**Terminal 1: Start TLE API**
```bash
uv run pipelines/ingestion/api/tle_stream_api.py
```

**Terminal 2: Start Kafka Producer**
```bash
uv run pipelines/ingestion/tle_api_to_kafka_producer.py --limit 100
```

**Terminal 3: Start Spark Streaming**
```bash
docker exec -u root spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/pipelines/processing/spark_sgp4_to_hdfs.py \
  --kafka broker:29092
```

### Accessing Services
- **Spark Master UI**: http://localhost:9090
- **HDFS NameNode UI**: http://localhost:9870
- **Kafka Control Center**: http://localhost:9021
- **Airflow Webserver**: http://localhost:8080
- **Jupyter Notebook**: http://localhost:8888

## Directory Quick Reference

| Directory | Purpose | Key Files |
|-----------|---------|-----------|
| `config/airflow/` | Airflow DAGs | `airflow_dag_api_to_kafka.py` |
| `config/hadoop/` | Hadoop config | `core-site.xml`, `hdfs-site.xml` |
| `deployment/` | Docker setup | `docker-compose.yml` |
| `pipelines/ingestion/` | Data ingestion | `tle_api_to_kafka_producer.py` |
| `pipelines/processing/` | Data processing | `spark_sgp4_to_hdfs.py` |
| `data/raw/` | Raw data | Satellite catalogs |
| `data/processed/` | Processed data | TLE histories |
| `scripts/setup/` | Setup scripts | `start-containers.sh` |
| `scripts/operations/` | Ops scripts | `start_streaming.sh` |
| `notebooks/` | Jupyter notebooks | Analysis notebooks |
| `docs/` | Documentation | README files |

## Path Changes Summary

### Old → New Paths

**Code:**
- `src/kafka/*.py` → `pipelines/ingestion/*.py`
- `src/demo/api/` → `pipelines/ingestion/api/`
- `browse_sgp4_data.py` → `pipelines/processing/browse_sgp4_data.py`

**Config:**
- `dags/*.py` → `config/airflow/*.py`
- `hadoop-config/` → `config/hadoop/`
- `docker-compose.yml` → `deployment/docker-compose.yml`

**Data:**
- `Output/` → `data/raw/` or `data/processed/`

**Scripts:**
- `script/` → `scripts/setup/`
- `*.sh` from src/kafka → `scripts/operations/`

## Important Notes

1. **Docker Volume Mounts Changed**
   - Old: `./src/` → New: `../pipelines/`
   - Old: `./Output/` → New: `../data/`
   - Old: `./hadoop-config/` → New: `../config/hadoop/`

2. **Python Path Updates Needed**
   - Update imports if they reference `src.kafka` or similar
   - Use relative imports within pipelines

3. **Airflow DAG Discovery**
   - Ensure Airflow `dags_folder` points to `config/airflow/`

4. **Data Directories**
   - `data/raw/` - Immutable source data
   - `data/processed/` - Can be regenerated
   - `data/external/` - Version controlled dependencies
   - `data/archive/` - Long-term storage

## Testing After Restructure

```bash
# 1. Start containers
./scripts/setup/start-containers.sh

# 2. Check all services are up
docker ps

# 3. Verify HDFS
./scripts/setup/hdfs-cli.sh
# Inside HDFS CLI: hdfs dfs -ls /

# 4. Test Kafka
docker exec -it broker kafka-topics --list --bootstrap-server localhost:9092

# 5. Check Spark
curl http://localhost:9090

# 6. Run end-to-end test
./scripts/operations/test_e2e_pipeline.sh
```

## Troubleshooting

**Issue: Docker volume mount not working**
- Ensure you're running docker-compose from `deployment/` directory OR
- Use absolute paths in docker-compose.yml

**Issue: Python module not found**
- Check your PYTHONPATH includes project root
- Use relative imports within pipeline modules

**Issue: Airflow DAG not showing**
- Verify `AIRFLOW_HOME` or `dags_folder` setting
- Check `config/airflow/` directory permissions

**Issue: Data files not found**
- Update hardcoded paths to use `data/` instead of `Output/`
- Use environment variables for data paths

## Next Steps

1. Update any hardcoded paths in your Python code
2. Test the complete pipeline end-to-end
3. Update CI/CD pipelines if applicable
4. Add more comprehensive tests
5. Document any custom configurations

For detailed information, see:
- [Main README](../README.md)
- [Restructuring Summary](RESTRUCTURING_SUMMARY.md)
- [Original README](README_OLD.md)
