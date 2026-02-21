# Collision Prediction System - Documentation

## Overview

The Collision Prediction System is an advanced module for predicting potential satellite collisions using SGP4 orbital propagation. It processes historical satellite tracking data, predicts future positions, and identifies potential collision risks.

## Architecture

```
┌─────────────────┐
│  SGP4 Vectors   │
│     (HDFS)      │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Collision Prediction Engine    │
│  (Spark Job)                    │
│  - Read SGP4 data               │
│  - Propagate positions          │
│  - Detect collisions            │
│  - Classify risk levels         │
└────────┬────────────────────────┘
         │
         ├─────────────────┬──────────────────┐
         ▼                 ▼                  ▼
┌────────────────┐  ┌─────────────┐  ┌──────────────┐
│ HDFS Storage   │  │   Kafka     │  │  Dashboard   │
│ (Predictions)  │  │  (Alerts)   │  │  (Viz Web)   │
└────────────────┘  └─────────────┘  └──────────────┘
```

## Components

### 1. Collision Prediction Spark Job
**File:** `pipelines/processing/spark_collision_prediction.py`

**Features:**
- Reads latest SGP4 vector data from HDFS
- Propagates satellite positions for next N days (configurable)
- Compares all satellite pairs at each time step
- Detects potential collisions based on distance threshold
- Classifies risk levels (HIGH, MEDIUM, LOW)
- Saves predictions to HDFS
- Publishes high-risk alerts to Kafka

**Configuration (from .env):**
- `PREDICTION_DAYS`: Number of days to predict ahead (default: 7)
- `COLLISION_THRESHOLD_KM`: Distance threshold for collision detection (default: 10 km)
- `TIME_WINDOW_DAYS`: Window for considering data fresh (default: 7 days)
- `SGP4_PROPAGATION_STEP_HOURS`: Time step between predictions (default: 6 hours)

### 2. Airflow DAG
**File:** `pipelines/ingestion/airflow_dag_collision_prediction.py`

**Schedule:** Every 6 hours

**Tasks:**
1. Check HDFS data availability
2. Verify Kafka connection
3. Submit Spark collision prediction job
4. Verify output was written
5. Log summary statistics

### 3. Dashboard API
**File:** `dashboard_api.py`

**Endpoints:**
- `GET /api/health` - Health check
- `GET /api/collisions` - Get collision predictions (limit parameter)
- `GET /api/collisions/stats` - Get statistical summary
- `GET /api/collisions/high-risk` - Get high-risk alerts only
- `GET /api/collisions/timeline` - Get collision timeline (days parameter)
- `GET /api/satellites/tracking` - Get satellite tracking data
- `GET /api/satellites/pairs` - Get frequently colliding pairs
- `GET /api/config` - Get system configuration

**Port:** 5001 (configurable via `DASHBOARD_PORT`)

### 4. Web Dashboard
**Location:** `dashboard/`

**Files:**
- `index.html` - Main dashboard page
- `styles.css` - Styling and responsive design
- `dashboard.js` - Data fetching and visualization logic

**Features:**
- Real-time collision monitoring
- Risk level distribution charts
- Collision timeline visualization
- High-risk alert table
- Satellite pair frequency analysis
- Distance statistics
- Auto-refresh (configurable interval)

**Port:** 8080 (served via nginx)

## Installation & Setup

### 1. Prerequisites
Ensure the base system is running:
```bash
docker-compose up -d namenode datanode spark-master kafka zookeeper
```

### 2. Start Dashboard Services
```bash
docker-compose up -d dashboard-api dashboard-web
```

### 3. Verify Services
```bash
# Check API health
curl http://localhost:5001/api/health

# Check dashboard
curl http://localhost:8080
```

### 4. Trigger Collision Prediction
Via Airflow UI (http://localhost:8091):
- Navigate to DAGs
- Find `collision_prediction_pipeline`
- Enable and trigger the DAG

Or via command line:
```bash
docker exec airflow-scheduler airflow dags trigger collision_prediction_pipeline
```

## Configuration

All configuration is managed through `.env` file:

```env
# Collision Prediction Settings
PREDICTION_DAYS=7
COLLISION_THRESHOLD_KM=10.0
TIME_WINDOW_DAYS=7

# SGP4 Propagation Settings
SGP4_PROPAGATION_STEP_HOURS=6
MAX_PROPAGATION_DAYS=30

# HDFS Paths
HDFS_SGP4_VECTORS_PATH=hdfs://namenode:9000/space-debris/sgp4_vectors
HDFS_COLLISION_PREDICTIONS_PATH=hdfs://namenode:9000/space-debris/collision_predictions

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS=kafka:9093
KAFKA_COLLISION_TOPIC=space_debris_collisions

# Dashboard Settings
DASHBOARD_PORT=5001
DASHBOARD_UPDATE_INTERVAL_SECONDS=30

# Alerting Thresholds
HIGH_RISK_THRESHOLD_KM=5.0
MEDIUM_RISK_THRESHOLD_KM=10.0
LOW_RISK_THRESHOLD_KM=50.0
```

## Usage

### Access Dashboard
Open browser to: `http://localhost:8080`

The dashboard will automatically:
- Connect to the API
- Load latest collision predictions
- Refresh every 30 seconds (configurable)
- Display real-time statistics

### Query API Directly

**Get collision statistics:**
```bash
curl http://localhost:5001/api/collisions/stats | jq
```

**Get high-risk collisions:**
```bash
curl http://localhost:5001/api/collisions/high-risk | jq
```

**Get collision timeline:**
```bash
curl "http://localhost:5001/api/collisions/timeline?days=7" | jq
```

### Monitor Kafka Alerts
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic space_debris_collisions \
  --from-beginning
```

### Check HDFS Output
```bash
docker exec spark-master hdfs dfs -ls /space-debris/collision_predictions/
```

## Data Flow

1. **Input:** SGP4 vectors from HDFS (generated by `spark_sgp4_to_hdfs.py`)
2. **Processing:** 
   - Filter data within time window (7 days default)
   - Get latest position for each satellite
   - Propagate positions for next N days using SGP4
   - Compare all satellite pairs at each time step
   - Calculate distances between satellites
3. **Output:**
   - Save all predictions to HDFS (partitioned by risk_level)
   - Publish HIGH/MEDIUM risk alerts to Kafka
4. **Visualization:**
   - Dashboard API queries HDFS for latest data
   - Web dashboard displays real-time statistics and charts

## Risk Classification

- **HIGH RISK:** Distance < 5 km
- **MEDIUM RISK:** Distance < 10 km
- **LOW RISK:** Distance < 50 km

## Performance Considerations

- **Prediction Complexity:** O(n²) where n = number of satellites
- **Optimization:** Uses Spark's distributed processing
- **Memory:** Configure executor memory based on satellite count
- **Storage:** Parquet format with partitioning by risk_level

## Troubleshooting

### No Collisions Detected
- Check if SGP4 data exists: `hdfs dfs -ls /space-debris/sgp4_vectors/`
- Verify time window: older data might be filtered out
- Check collision threshold: might be too strict

### Dashboard Not Loading
- Verify API is running: `curl http://localhost:5001/api/health`
- Check HDFS connectivity from dashboard-api container
- Review logs: `docker logs dashboard-api`

### API Errors
- Ensure Spark session can connect to HDFS
- Verify parquet files exist in collision predictions path
- Check environment variables are properly set

### High Memory Usage
- Reduce `PREDICTION_DAYS` (fewer time steps)
- Increase `SGP4_PROPAGATION_STEP_HOURS` (coarser granularity)
- Limit satellite count in processing

## Future Enhancements

1. **Machine Learning Integration**
   - Predict collision probability based on historical patterns
   - Anomaly detection for unusual orbital behavior

2. **Advanced Visualization**
   - 3D orbital path visualization
   - Interactive satellite selection
   - Historical trend analysis

3. **Alert System**
   - Email/SMS notifications for high-risk collisions
   - Integration with external monitoring systems
   - Automated collision avoidance recommendations

4. **Performance Optimization**
   - Spatial indexing for faster collision detection
   - Incremental processing (only new data)
   - Caching layer (Redis) for frequently accessed data

## API Reference

See full API documentation in the dashboard API code or access interactive docs at:
- Swagger UI: `http://localhost:5001/docs` (if enabled)

## License

Part of the Space Debris Risk Prediction System
