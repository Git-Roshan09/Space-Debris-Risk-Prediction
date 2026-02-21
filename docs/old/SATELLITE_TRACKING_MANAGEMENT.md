# Satellite Tracking Management

## Overview

This document describes how the Space Debris Risk Prediction system manages satellite tracking, including when to stop tracking satellites that are no longer relevant.

## Current Tracking Status

- **Total Satellites in Dataset**: ~1,100 satellites
- **Data Source**: TLE History files in `data/processed/TLE_History/`
- **Tracking Identifier**: NORAD Catalog ID

## Automatic Stop Tracking Conditions

The system implements **three automatic stop conditions** to filter out satellites that should no longer be tracked:

### 1. **SGP4 Propagation Errors**
**Condition**: `sgp4_error_code != 0`

**Description**: 
- SGP4 (Simplified General Perturbations) is the algorithm used to propagate satellite positions
- Error codes 1-6 indicate various propagation failures
- Satellites with persistent errors cannot have their orbits accurately predicted

**Action**: Satellites with SGP4 errors are immediately excluded from tracking

**Reason**: Invalid propagation makes collision prediction impossible

---

### 2. **Low Altitude (De-orbit Threshold)**
**Condition**: `altitude_km < 150.0 km`

**Description**:
- Satellites below 150 km altitude experience significant atmospheric drag
- Objects at this altitude will re-enter Earth's atmosphere within days
- These satellites pose minimal long-term collision risk

**Default Threshold**: 150 km (configurable via `MIN_ALTITUDE_KM`)

**Action**: Satellites below threshold are marked as `STOPPED_LOW_ALTITUDE`

**Reason**: Imminent atmospheric re-entry makes continued tracking unnecessary

---

### 3. **Stale TLE Data**
**Condition**: `tle_age_days > 30 days`

**Description**:
- Two-Line Elements (TLE) are ephemeris data that describe satellite orbits
- TLEs become less accurate over time due to perturbations
- Data older than 30 days produces unreliable predictions

**Default Threshold**: 30 days (configurable via `MAX_TLE_AGE_DAYS`)

**Action**: Satellites with old TLEs are marked as `STOPPED_STALE_TLE`

**Reason**: Outdated orbital elements compromise prediction accuracy

---

## Configuration

### Docker Environment Variables

Edit [docker-compose.yml](../docker-compose.yml):

```yaml
spark-sgp4-streaming:
  environment:
    - MIN_ALTITUDE_KM=150.0      # Minimum altitude threshold
    - MAX_TLE_AGE_DAYS=30        # Maximum TLE age
```

### Command Line Arguments

When running the Spark job directly:

```bash
spark-submit /opt/spark-apps/processing/spark_sgp4_to_hdfs.py \
  --min-altitude 150.0 \
  --max-tle-age 30
```

## Tracking Status Values

| Status | Description |
|--------|-------------|
| `ACTIVE` | Satellite is actively tracked |
| `STOPPED_SGP4_ERROR` | SGP4 propagation failed |
| `STOPPED_LOW_ALTITUDE` | Below de-orbit threshold |
| `STOPPED_STALE_TLE` | TLE data too old |

## Stopped Satellite Logging

All stopped satellites are logged to HDFS for analysis:

**HDFS Path**: `hdfs://namenode:9000/space-debris/stopped_tracking`

**Format**: Parquet (partitioned by `tracking_status`)

**Fields**:
- `satellite_id`: NORAD Catalog ID
- `epoch_time`: TLE epoch timestamp
- `altitude_km`: Calculated altitude
- `sgp4_error_code`: SGP4 error code
- `tle_age_days`: Age of TLE data
- `tracking_status`: Reason for stopping
- `processing_time`: When satellite was filtered

## Analysis Queries

### Count Stopped Satellites by Reason

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Stopped Analysis").getOrCreate()
stopped_df = spark.read.parquet("hdfs://namenode:9000/space-debris/stopped_tracking")

stopped_df.groupBy("tracking_status").count().show()
```

### Find Recently Stopped Satellites

```python
from pyspark.sql.functions import col, current_timestamp, datediff

stopped_df.filter(
    datediff(current_timestamp(), col("processing_time")) < 7
).select("satellite_id", "tracking_status", "altitude_km", "processing_time").show()
```

## Pipeline Flow

```
┌─────────────────┐
│  Kafka Topic    │
│ space_debris_tle│
└────────┬────────┘
         │
         v
┌─────────────────┐
│ Parse TLE Data  │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Compute SGP4   │
│    Vectors      │
└────────┬────────┘
         │
         v
┌─────────────────────────────────┐
│   Apply Tracking Filters        │
│                                  │
│  1. SGP4 Error Code = 0?         │
│  2. Altitude >= 150 km?          │
│  3. TLE Age <= 30 days?          │
└────────┬────────────┬────────────┘
         │            │
         v            v
    ┌────────┐  ┌──────────────┐
    │ ACTIVE │  │   STOPPED    │
    │Tracking│  │   Tracking   │
    └────┬───┘  └──────┬───────┘
         │             │
         v             v
   ┌──────────┐  ┌──────────────┐
   │  HDFS    │  │ HDFS Stopped │
   │ Vectors  │  │     Log      │
   └──────────┘  └──────────────┘
```

## Future Enhancements

### Potential Additional Stop Conditions

1. **DECAY Date from Catalog**
   - Check if satellite has official decay date
   - Automatically stop if re-entry has occurred

2. **Collision Already Occurred**
   - Stop tracking satellites that have fragmented
   - Requires collision event database

3. **Mission End Date**
   - Stop tracking decommissioned satellites
   - Requires satellite mission database

4. **Geolocation Restrictions**
   - Filter satellites by orbit region
   - Example: Only track LEO satellites

## Monitoring

### Active Satellite Count

Check console output during streaming:

```
=== Applying Tracking Stop Conditions ===
Filter 1: SGP4 Error Code = 0 (valid propagation)
Filter 2: Altitude >= 150.0 km (above de-orbit threshold)
Filter 3: TLE Age <= 30 days (data freshness)
```

### HDFS Metrics

```bash
# Check stopped satellites directory
hdfs dfs -ls /space-debris/stopped_tracking

# Count files by partition
hdfs dfs -du -h /space-debris/stopped_tracking/*
```

## References

- [SGP4 Algorithm Documentation](https://en.wikipedia.org/wiki/Simplified_perturbations_models)
- [TLE Format Specification](https://en.wikipedia.org/wiki/Two-line_element_set)
- [Spark Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
