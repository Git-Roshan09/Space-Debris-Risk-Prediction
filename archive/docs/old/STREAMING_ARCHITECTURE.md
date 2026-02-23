# Space Debris Streaming Architecture

## Data Flow

```
External TLE API (Demo)
         ↓
      Kafka Topic
    (space_debris_tle)
         ↓
   Spark Structured Streaming
         ↓
    ┌────┴────┐
    ↓         ↓
SGP4      Risk Analysis
Vectors      ↓
    ↓    ┌───┴───┐
    ↓    ↓       ↓
  HDFS  Cassandra  Console
(batch)  (real-time) (monitoring)
```

## Architecture Benefits

### 1. **Real-time Processing**
- TLE data → SGP4 vectors → Risk scores in one streaming pipeline
- Sub-second latency from ingestion to risk assessment

### 2. **Dual Storage Strategy**

**HDFS (Hadoop Distributed File System)**
- **Purpose**: Historical data for batch ML training
- **Format**: Parquet (columnar, compressed)
- **Data**: SGP4 position/velocity vectors
- **Use Cases**:
  - Train ML models for collision prediction
  - Historical trajectory analysis
  - Data science exploration

**Cassandra (NoSQL Database)**
- **Purpose**: Real-time queries and dashboards
- **Format**: Table with indexes
- **Data**: Latest risk scores per satellite
- **Use Cases**:
  - Real-time alerts for HIGH risk objects
  - REST API to query current risk levels
  - Dashboard showing top 10 highest risk satellites

### 3. **Scalability**
- Kafka handles high-throughput ingestion (millions of TLEs)
- Spark processes in parallel across worker nodes
- HDFS scales horizontally for storage
- Cassandra provides fast reads with replication

## Setup Instructions

### 1. Start All Services
```bash
cd /home/bharath/Documents/BigData/project/data/Space-Debris-Risk-Prediction
docker-compose up -d
```

### 2. Create Cassandra Schema
```bash
chmod +x src/kafka/setup_cassandra.sh
./src/kafka/setup_cassandra.sh
```

### 3. Verify HDFS Access
```bash
# Check HDFS is accessible
docker exec spark-master hdfs dfs -ls /
```

### 4. Add sgp4 to Dependencies
```bash
# Add to pyproject.toml
uv add sgp4
uv sync
```

### 5. Start TLE Streaming API
```bash
# Terminal 1
uv run src/demo/api/tle_stream_api.py
```

### 6. Start Kafka Producer
```bash
# Terminal 2 (optional - or use Airflow DAG)
uv run src/kafka/tle_api_to_kafka_producer.py --limit 1000 --acceleration 1000
```

### 7. Start Spark Streaming Job
```bash
# Terminal 3
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  /opt/bitnami/spark/src/kafka/spark_streaming_sgp4_risk.py \
  --kafka broker:29092 \
  --hdfs hdfs://localhost:9000/space-debris \
  --cassandra cassandra
```

## Querying Results

### Query Cassandra for High Risk Objects
```bash
docker exec cassandra cqlsh -e "
USE space_debris;

-- Get top 10 highest risk satellites
SELECT satellite_id, risk_score, risk_level, altitude_km, velocity_kms 
FROM risk_scores 
WHERE risk_level = 'HIGH' 
LIMIT 10 
ALLOW FILTERING;
"
```

### Query HDFS for Historical Vectors
```bash
# List parquet files
docker exec spark-master hdfs dfs -ls /space-debris/sgp4_vectors/

# Read with Spark
docker exec spark-master spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

scala> val df = spark.read.parquet("hdfs://localhost:9000/space-debris/sgp4_vectors")
scala> df.show()
scala> df.filter("altitude_km < 2000").count()
```

## Monitoring

### 1. Spark UI
- http://localhost:9090 - Spark Master
- View streaming jobs, stages, and executors

### 2. Kafka Control Center
- http://localhost:9021 - Confluent Control Center
- Monitor topics, consumer lag, throughput

### 3. Airflow
- http://localhost:8080 - Airflow UI (admin/admin)
- Schedule and monitor data ingestion DAGs

### 4. Console Output
```bash
# Watch Spark streaming output
docker logs -f spark-master
```

## Data Lineage

```
TLE from SpaceTrack API
  ↓ (historical, batch download)
CSV Files (Output/TLE_History/)
  ↓ (streaming at 100x-1000x speed)
Demo Flask API (http://localhost:5000)
  ↓ (HTTP stream)
Kafka Producer (tle_api_to_kafka_producer.py)
  ↓ (JSON messages)
Kafka Topic (space_debris_tle)
  ↓ (distributed log)
Spark Streaming (spark_streaming_sgp4_risk.py)
  ↓ (SGP4 propagation + risk calculation)
  ├→ HDFS/sgp4_vectors/*.parquet (for ML)
  └→ Cassandra/space_debris.risk_scores (for queries)
```

## Performance Tuning

### Kafka
- Increase partitions for `space_debris_tle` topic (default: 3)
- Tune `batch.size` and `linger.ms` in producer

### Spark
- Increase executor memory: `--executor-memory 2G`
- Add more workers: `docker-compose scale spark-worker=3`
- Tune micro-batch interval: `.trigger(processingTime='10 seconds')`

### Cassandra
- Create indexes on frequently queried columns
- Tune compaction strategy for time-series data
- Add more nodes for replication

## Next Steps

1. **Create Dashboard**: Grafana + Cassandra for real-time visualization
2. **ML Pipeline**: Train collision prediction model on HDFS data
3. **Alerting**: Set up PagerDuty/Slack alerts for HIGH risk objects
4. **API Layer**: Flask REST API on top of Cassandra for external queries
