# Simplified Streaming Pipeline: TLE → SGP4 → HDFS

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│              INGESTION LAYER                             │
│  SpaceTrack API → Demo Flask API → Kafka Producer       │
│                   (port 5000)      (Python)              │
└────────────────────┬─────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│              MESSAGE QUEUE                               │
│         Kafka Topic: space_debris_tle                    │
│              (Distributed Log)                           │
└────────────────────┬─────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│           PROCESSING LAYER (Spark)                       │
│                                                          │
│  1. Read TLE from Kafka (streaming)                     │
│  2. Parse JSON → TLE elements                           │
│  3. SGP4 Propagation:                                   │
│     - Position vectors (X, Y, Z) in km                  │
│     - Velocity vectors (Vx, Vy, Vz) in km/s            │
│     - Altitude calculation                              │
│     - Velocity magnitude                                │
│  4. Error handling (filter failed propagations)         │
└────────────────────┬─────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│          STORAGE LAYER (HDFS)                            │
│                                                          │
│  Path: /space-debris/sgp4_vectors/                      │
│  Format: Parquet (columnar, compressed)                 │
│  Partitioning: By epoch_time (date-based)               │
│                                                          │
│  Schema:                                                 │
│  - satellite_id (string)                                │
│  - epoch_time (timestamp)                               │
│  - position_x, _y, _z (double) - TEME coordinates       │
│  - velocity_x, _y, _z (double) - km/s                   │
│  - altitude_km (double)                                 │
│  - velocity_magnitude_kms (double)                      │
│  - orbital elements (inclination, RAAN, etc.)           │
│  - processing_time (timestamp)                          │
└──────────────────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│         FUTURE: TIME SERIES ANALYSIS                     │
│  - Trajectory prediction                                │
│  - Collision probability modeling                       │
│  - Orbit determination refinement                       │
│  - Machine learning on historical vectors               │
└──────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Prerequisites

Ensure all services are running:
```bash
cd /home/bharath/Documents/BigData/project/data/Space-Debris-Risk-Prediction
docker-compose up -d
```

Verify services:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### 2. Start TLE Streaming API

```bash
# Terminal 1
uv run src/demo/api/tle_stream_api.py
```

### 3. Stream TLE Data to Kafka

```bash
# Terminal 2 - Send some test data
uv run src/kafka/tle_api_to_kafka_producer.py --limit 100 --acceleration 5000

# Or enable Airflow DAG for continuous streaming
# Go to http://localhost:8080 → Toggle ON: tle_api_to_kafka_streaming
```

### 4. Start Spark Streaming to HDFS

```bash
# Terminal 3
docker exec -it spark-master bash

# Inside container, install sgp4
pip install sgp4

# Run the streaming job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/bitnami/spark/work-dir/src/kafka/spark_sgp4_to_hdfs.py \
  --kafka broker:29092 \
  --hdfs-path /space-debris/sgp4_vectors
```

**Note**: You'll need to mount the source code into Spark container (see docker-compose setup below)

## HDFS Output Structure

```
/space-debris/sgp4_vectors/
├── epoch_time=2024-01-14/
│   ├── part-00000-xyz.parquet
│   ├── part-00001-xyz.parquet
│   └── ...
├── epoch_time=2024-01-15/
│   └── ...
└── _spark_metadata/
```

## Querying HDFS Data

### Option 1: PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnalyzeVectors").getOrCreate()

# Read all SGP4 vectors
df = spark.read.parquet("/space-debris/sgp4_vectors")

# Show schema
df.printSchema()

# Basic statistics
df.describe(['altitude_km', 'velocity_magnitude_kms']).show()

# Time series query - satellites between specific dates
df.filter(
    (df.epoch_time >= '2024-01-01') & 
    (df.epoch_time <= '2024-01-31')
).groupBy('satellite_id').count().show()

# Low Earth Orbit satellites (altitude < 2000 km)
leo_satellites = df.filter(df.altitude_km < 2000)
leo_satellites.count()

# High velocity objects (potential collision risk)
high_velocity = df.filter(df.velocity_magnitude_kms > 10.0)
high_velocity.select('satellite_id', 'altitude_km', 'velocity_magnitude_kms').show()
```

### Option 2: HDFS Command Line

```bash
# List all parquet files
docker exec spark-master hdfs dfs -ls /space-debris/sgp4_vectors/

# Check specific partition
docker exec spark-master hdfs dfs -ls /space-debris/sgp4_vectors/epoch_time=2024-01-14/

# Get file sizes
docker exec spark-master hdfs dfs -du -h /space-debris/sgp4_vectors/

# Copy data to local filesystem
docker exec spark-master hdfs dfs -get /space-debris/sgp4_vectors/ /tmp/vectors_backup/
```

## Docker Compose Configuration

Add this to your `docker-compose.yml` to mount source code into Spark:

```yaml
spark-master:
  image: apache/spark:latest
  container_name: spark-master
  command: bin/spark-class org.apache.spark.deploy.master.Master
  ports:
    - "9090:8080"
    - "7077:7077"
  volumes:
    - ./src:/opt/bitnami/spark/work-dir/src  # Mount source code
  networks:
    - confluent

spark-worker:
  image: apache/spark:latest
  container_name: spark-worker
  command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
  depends_on:
    - spark-master
  environment:
    SPARK_WORKER_CORES: 2
    SPARK_WORKER_MEMORY: 2g
  volumes:
    - ./src:/opt/bitnami/spark/work-dir/src  # Mount source code
  networks:
    - confluent
```

## Monitoring

### 1. Spark UI
- **URL**: http://localhost:9090
- **View**: Active jobs, stages, executors, streaming statistics

### 2. Kafka Monitoring
```bash
# Check topic messages
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic space_debris_tle \
  --from-beginning \
  --max-messages 5

# Check consumer lag
docker exec broker kafka-consumer-groups \
  --bootstrap-server broker:29092 \
  --describe \
  --group spark-kafka-source
```

### 3. Streaming Metrics
Watch the console output in Terminal 3 to see:
- Records processed per batch
- Processing time per batch
- SGP4 calculations per second

## Data Schema

### Input (from Kafka)
```json
{
  "satellite_id": "25544",
  "epoch": "2024-01-14T12:30:45.123456+00:00",
  "tle_line1": "1 25544U 98067A   ...",
  "tle_line2": "2 25544  51.6416 ...",
  "inclination": 51.6416,
  "raan": 123.4567,
  "eccentricity": "0.0001234",
  ...
}
```

### Output (in HDFS Parquet)
| Column | Type | Description |
|--------|------|-------------|
| satellite_id | string | NORAD catalog number |
| epoch_time | timestamp | TLE epoch |
| position_x, _y, _z | double | Position in TEME frame (km) |
| velocity_x, _y, _z | double | Velocity in TEME frame (km/s) |
| altitude_km | double | Altitude above Earth surface |
| velocity_magnitude_kms | double | Total velocity magnitude |
| inclination | double | Orbital inclination (degrees) |
| raan | double | Right Ascension of Ascending Node |
| eccentricity | string | Orbital eccentricity |
| processing_time | timestamp | When Spark processed this record |

## Performance Tuning

### Increase Throughput
```bash
# More Spark executors
spark-submit \
  --num-executors 3 \
  --executor-cores 2 \
  --executor-memory 2g \
  ...
```

### Optimize Parquet Writes
```python
# In the code, add compression
.option("compression", "snappy") \
.option("parquet.block.size", 134217728) \  # 128 MB blocks
```

### Kafka Consumer Tuning
```python
# In the code, add these options
.option("maxOffsetsPerTrigger", 1000) \  # Limit records per batch
.option("minPartitions", 3) \  # Distribute across partitions
```

## Next Steps (Future Analysis)

Once vectors are in HDFS, you can:

1. **Time Series Analysis**
   - Track orbital decay over time
   - Identify anomalous maneuvers
   - Predict future positions

2. **Collision Detection**
   - Calculate close approach distances
   - Identify conjunction events
   - Estimate collision probabilities

3. **Machine Learning**
   - Train models on historical vectors
   - Predict orbital lifetime
   - Classify satellite behavior

4. **Visualization**
   - Plot 3D trajectories
   - Heatmaps of object density
   - Time-lapse animations

## Troubleshooting

### Spark can't connect to Kafka
```bash
# Check network
docker exec spark-master ping broker

# Use docker network name in Kafka servers
--kafka broker:29092  # Inside Docker network
```

### SGP4 errors
Check console output for `sgp4_error_code`:
- 0: Success
- 1: Mean eccentricity out of range
- 2: Mean motion out of range
- 3: Perturbed eccentricity out of range
- 6: Orbit decayed

### HDFS permission denied
```bash
# Create directory with proper permissions
docker exec spark-master hdfs dfs -mkdir -p /space-debris
docker exec spark-master hdfs dfs -chmod 777 /space-debris
```
