# Space Debris Risk Prediction - Kafka Streaming Pipeline

Real-time streaming pipeline for space debris risk prediction using Apache Kafka, Apache Airflow, and Apache Spark.

## 🚀 Overview

This pipeline consumes TLE (Two-Line Element) data from a streaming API, publishes to Kafka, and performs real-time risk analysis using Spark Structured Streaming.

**Why Use a Demo API?**
Real space tracking APIs (like Space-Track.org) update TLE data infrequently—sometimes days or months between updates. Our demo API streams historical TLE data at accelerated speeds (100x-1000x), making it ideal for:
- ✅ Development and testing
- ✅ Real-time processing demonstrations
- ✅ Training and educational purposes
- ✅ System validation and debugging

### Architecture

```
TLE Streaming API (Demo) → Kafka Producer → Kafka Topic → Spark Streaming → Risk Analysis → Outputs
     (Flask)                  (Python)      (space_debris_tle)   (PySpark)      (Console/DB)
```

### Components

1. **TLE Streaming API** (`../demo/api/tle_stream_api.py`)
   - Streams historical TLE data from `/Output/TLE_History/`
   - Accelerated playback (default 100x real-time)
   - Chronologically ordered by epoch timestamp

2. **Kafka Producer** (`tle_api_to_kafka_producer.py`)
   - Consumes from TLE streaming API
   - Parses and enriches TLE data with orbital elements
   - Publishes to Kafka topic with satellite_id as partition key

3. **Kafka Topic**: `space_debris_tle`
   - Stores enriched TLE messages with orbital parameters
   - Partitioned by satellite_id for parallel processing

4. **Spark Consumer** (`space_debris_spark_consumer.py`)
   - Real-time risk scoring based on inclination, altitude, eccentricity
   - 5-minute windowed aggregations
   - Classifies objects as HIGH/MEDIUM/LOW risk

5. **Automation** (`airflow_dag_api_to_kafka.py`)
   - Airflow DAG for scheduled streaming
   - Runs hourly, streams 1000 records per run

### Files in This Directory

- **`tle_api_to_kafka_producer.py`**: Standalone Kafka producer (main entry point)
- **`airflow_dag_api_to_kafka.py`**: Airflow DAG for automated streaming
- **`space_debris_spark_consumer.py`**: Spark streaming consumer for risk analysis
- **`start_streaming.sh`**: Quick start bash script
- **`README.md`**: This documentation

## 📋 Prerequisites

- Docker and Docker Compose
- Apache Airflow
- Apache Kafka
- Apache Spark
- Python 3.8+

### Python Dependencies

```bash
pip install kafka-python pyspark apache-airflow
```

## 🔧 Setup

### 1. Start Kafka and Dependencies

Make sure your `docker-compose.yml` includes Kafka, Zookeeper, and Spark:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      
  broker:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Start services:
```bash
docker-compose up -d
```

### 2. Create Kafka Topics

```bash
# Create TLE topic
docker exec -it <kafka-container> kafka-topics --create \
  --bootstrap-server broker:29092 \
  --topic space_debris_tle \
  --partitions 3 \
  --replication-factor 1

# Create catalog topic
docker exec -it <kafka-container> kafka-topics --create \
  --bootstrap-server broker:29092 \
  --topic space_debris_catalog \
  --partitions 1 \
  --replication-factor 1
```

### 3. Start TLE Streaming API

Start the demo TLE streaming API:

```bash
cd ../demo/api
python tle_stream_api.py
```

The API will be available at `http://localhost:5000`

Verify it's working:
```bash
curl http://localhost:5000/stats
```

### 4. Stream from API to Kafka

#### Quick Start (Recommended)

Use the provided script:

```bash
./start_streaming.sh
```

Or with custom settings:
```bash
ACCELERATION=200 LIMIT=5000 ./start_streaming.sh
```

#### Manual Start

```bash
# Stream with default settings (100x acceleration, unlimited records)
python tle_api_to_kafka_producer.py

# Custom configuration
python tle_api_to_kafka_producer.py \
  --api-url http://localhost:5000 \
  --kafka-brokers localhost:9092 \
  --acceleration 200 \
  --limit 5000 \
  --mode adaptive
```

**Arguments**:
- `--api-url`: TLE streaming API URL (default: http://localhost:5000)
- `--kafka-brokers`: Kafka bootstrap servers (default: localhost:9092)
- `--kafka-topic`: Kafka topic name (default: space_debris_tle)
- `--acceleration`: Speed multiplier (default: 100x)
- `--limit`: Max records to stream (default: unlimited)
- `--mode`: Streaming mode - adaptive/proportional/fixed (default: adaptive)
- `--max-delay`: Max delay between records in seconds (default: 5.0)

#### Automated with Airflow (Optional)

For scheduled, automated streaming:

```bash
# Copy DAG to Airflow
cp airflow_dag_api_to_kafka.py $AIRFLOW_HOME/dags/

# Trigger manually
airflow dags trigger tle_api_to_kafka_streaming
```

Or access Airflow web UI at `http://localhost:8080`

**DAG Settings**:
- Schedule: Hourly (`@hourly`)
- Records per run: 1000
- Acceleration: 100x

Edit the DAG file to customize:
```python
STREAM_LIMIT = 1000  # Records per run
STREAM_ACCELERATION = 100  # Speed multiplier
TLE_API_BASE_URL = 'http://localhost:5000'  # API URL
```

### 5. Start Spark Consumer

Run the Spark streaming consumer:

```bash
# Local mode
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  space_debris_spark_consumer.py

# Or with Docker
docker exec -it <spark-container> \
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/space_debris_spark_consumer.py
```

## 📊 Data Flow

### TLE Data Schema

```json
{
  "message_id": "uuid",
  "message_timestamp": "2024-01-14T10:30:00",
  "satellite_id": "1576",
  "epoch": "1965-09-02 07:48:07.178975+00:00",
  "tle_line1": "1 01576U 58002...",
  "tle_line2": "2 01576 034.2104...",
  "inclination": 34.2104,
  "eccentricity": "0.1830315",
  "mean_motion": 10.84936988
}
```

### Debris Catalog Schema

```json
{
  "message_id": "uuid",
  "message_timestamp": "2024-01-14T10:30:00",
  "data": {
    "norad_cat_id": "1576",
    "object_name": "VANGUARD DEB",
    "object_type": "DEBRIS",
    "country": "US",
    "launch_date": "1958-03-17",
    "period": "121.81",
    "inclination": "34.22",
    "apogee": "2893.0",
    "perigee": "630.0",
    "rcs_size": "SMALL"
  }
}
```

## 🎯 Risk Scoring Algorithm

The pipeline calculates risk scores based on:

1. **Inclination Risk** (0.1 - 0.4)
   - High inclination (>80°): 0.4
   - Medium inclination (60-80°): 0.3
   - Low inclination (<60°): 0.1-0.2

2. **Altitude Risk** (0.1 - 0.3)
   - Low Earth Orbit (mean motion >14): 0.3
   - Medium altitude: 0.2
   - High altitude: 0.1

3. **Eccentricity Risk** (0.1 - 0.3)
   - High eccentricity (>0.1): 0.3
   - Low eccentricity: 0.1

**Total Risk Score**: Sum of above factors (0.3 - 1.0)

**Risk Levels**:
- HIGH: Score >= 0.7
- MEDIUM: Score >= 0.5
- LOW: Score < 0.5

## 📈 MonTLE Streaming API

First verify the API is working:

```bash
# Get statistics
curl http://localhost:5000/stats

# Test stream (first 10 records)
curl "http://localhost:5000/stream?limit=10&acceleration=1000"
```

### Test API to Kafka Producer

```bash
# Test with limited records
python tle_api_to_kafka_producer.py --limit 100 --acceleration 1000

# Monitor in real-time
python tle_api_to_kafka_producer.py --kafka-brokers localhost:9092
```

### Test File-Based Producer

### Console Outputs

The Spark consumer writes to console in real-time:

1. **Individual Risk Scores**: Per-object risk analysis
2. **Aggregated Statistics**: 5-minute windowed aggregations by risk level

### Optional: Persistent Storage

Uncomment the Parquet writer in `space_debris_spark_consumer.py` to persist data:

```python
query3 = self.write_to_parquet(
    risk_df,
    output_path="/tmp/space_debris_risk",
    checkpoint_path="/tmp/space_debris_risk_checkpoint",
    query_name="parquet_risk_data"
)
```

### Kafka Monitoring

Monitor topic lag and throughput:

```bash
# Check topic details
docker exec -it <kafka-container> kafka-topics --describe \
  --bootstrap-server broker:29092 \
  --topic space_debris_tle

# Monitor consumer group
docker exec -it <kafka-container> kafka-consumer-groups --describe \
  --bootstrap-server broker:29092 \
  --group space-debris-consumer
```

## 🔄 DAG Configuration

### Airflow DAG Settings

- **Schedule**: `@hourly` (runs every hour)
- **Stream Duration**: 300 seconds (5 minutes) per run
- **Retry Policy**: 3 retries with 5-minute delay
- **Streaming Speed**: ~100x real-time acceleration

### Customization

Edit `space_debris_kafka_producer.py`:

```python
# Change streaming duration
STREAM_DURATION_SECONDS = 600  # 10 minutes

# Change schedule interval
schedule_interval='@daily'  # Run daily

# Adjust streaming speed
time.sleep(0.006)  # Faster: smaller value, Slower: larger value
```

## 🧪 Testing

### Test Kafka Producer Standalone

```bash
python -c "
from space_debris_kafka_producer import stream_tle_data
stream_tle_data()
"
```

### Test Spark Consumer

```bash
# Dry run to verify schema
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  space_debris_spark_consumer.py
```

### Verify Data Flow

```bash
# Consume messages from CLI
docker exec -it <kafka-container> kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic space_debris_tle \
  --from-beginning \
  --max-messages 10
```

## 📊 Example Queries

### Query 1: High-Risk Objects

Objects with risk score >= 0.7 in the last hour

### Query 2: Risk Distribution

Count of objects by risk level in 5-minute windows

### Query 3: Orbital Patterns

Average inclination and mean motion by risk category

## 🐛 Troubleshooting

### Kafka Connection Issues

```bash
# Check if Kafka is running
docker ps | grep kafka

# Test connectivity
telnet broker 29092
```

### Airflow DAG Not Appearing
Check if API is running
curl http://localhost:5000/stats

# Verify API can be reached from Docker
docker exec -it <kafka-container> curl http://host.docker.internal:5000/stats

# Check producer logs
python tle_api_to_kafka_producer.py --limit 10

# For file-based: verify TLE data directory exists
ls -la ../../../../Output/TLE_History/

# Check Airflow task logs
airflow tasks test tle_api_to_kafka_streaming stream_api_to_kafka 2024-01-14
```

### API Connection Issues

```bash
# Ensure API is running
ps aux | grep tle_stream_api

# Test API connectivity
curl -v http://localhost:5000/

# Check firewall/network settings
netstat -an | grep 5000
# Refresh DAGs
airflow dags list-import-errors
```

### Spark Streaming Errors

```bash
# Check Spark logs
docker logs <spark-container>

# Verify Kafka package
spark-submit --version
```

### No Data in Topics

```bash
# Verify TLE data directory exists
ls -la ../../../../Output/TLE_History/

# Check Airflow task logs
airflow tasks test space_debris_risk_streaming stream_tle_data 2024-01-14
```

## 🚀 Production Deployment

### Scaling Considerations

1. **Kafka Partitions**: Increase partitions for higher throughput
   ```bash
   kafka-topics --alter --partitions 10 --topic space_debris_tle
   ```

2. **Spark Executors**: Add more Spark executors
   ```bash
   spark-submit --num-executors 4 --executor-cores 2 ...
   ```

3. **Airflow Workers**: Scale Airflow workers for concurrent DAG runs

### Monitoring Stack

Consider adding:
- **Prometheus**: Metrics collection
- **Grafana**: Visualization dashboards
- **Kafka Manager**: Kafka cluster management
- **Spark UI**: Job monitoring

## 📚 Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [TLE Format Specification](https://en.wikipedia.org/wiki/Two-line_element_set)

## 🤝 Contributing

Contributions welcome! Key areas for enhancement:

1. Advanced risk prediction models (ML-based)
2. Real-time collision detection algorithms
3. Integration with external space tracking APIs
4. Dashboard visualization with real-time updates
5. Alert system for high-risk scenarios

## 📝 License

[Add your license information here]

## 👥 Authors

Space Debris Risk Prediction Team

---

**Note**: This is a simulated streaming pipeline using historical TLE data. For production use with live data, integrate with real-time space tracking APIs like Space-Track.org or similar services.
