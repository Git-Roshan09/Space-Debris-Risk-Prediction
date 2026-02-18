# Space Debris Risk Prediction

Technical implementation of space debris tracking and collision prediction pipeline using optimized TLE data classification.

## Implementation Status

**Data Pipeline:** Operational  
**API Integration:** Optimized TLE API (65M+ objects)  
**Processing:** SAT-SAT and SAT-DEB collision detection  
**Storage:** HDFS + PostgreSQL  

## Technical Components

### Data Sources
- **Optimized TLE API:** Server-sent events streaming, classified satellite/debris objects
- **Dataset:** 2004-2024 TLE data with country/launch metadata  
- **Classification:** Automatic satellite vs debris categorization

### Processing Pipeline
```
TLE API → Kafka → Spark SGP4 → HDFS
             ↓
        Collision Detection → PostgreSQL → Dashboard
```

### Services
- **API:** `src/apis/optimized_tle_api.py:5000` - Classified TLE data serving
- **Ingestion:** Airflow DAG streaming to Kafka topic `space_debris_tle`
- **Processing:** Spark SGP4 orbital calculations + collision prediction  
- **Storage:** HDFS vectors, PostgreSQL collision alerts
- **Dashboard:** Flask API + web interface

### Message Format
```json
{
  "norad_id": 16761,
  "object_name": "COSMOS 1751", 
  "classification": "SATELLITE|DEBRIS",
  "tle_line1": "1 16761U...",
  "tle_line2": "2 16761 074.0158...",
  "metadata": {
    "country": "CIS",
    "launch": "1986-06-06",
    "object_type": "PAYLOAD",
    "rcs_size": "MEDIUM"
  }
}
```

## Quick Start
```bash
# Start system
docker-compose up -d

# Verify services
docker ps | grep -E "(kafka|spark|airflow|hdfs|postgres)"

# Check API
curl http://localhost:5000/api/health

# Monitor processing
docker exec -it spark-master bash
# Check HDFS: http://localhost:9870
# Check Airflow: http://localhost:8088
```

## Key Files

- `src/apis/optimized_tle_api.py` - TLE data API with classification
- `pipelines/ingestion/dag_tle_ingestion_only.py` - Kafka ingestion DAG  
- `pipelines/processing/spark_sgp4_streaming.py` - SGP4 calculations
- `pipelines/processing/spark_collision_prediction.py` - Collision detection
- `src/apis/dashboard_api.py` - Dashboard backend

## Pending

- Spark streaming field mapping: `satellite_id` → `norad_id`
- Dashboard integration with new API format

## Documentation

See `docs/` directory for detailed technical documentation.

