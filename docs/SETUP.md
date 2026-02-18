# Space Debris Risk Prediction

A big data pipeline for analyzing and predicting space debris risks using TLE (Two-Line Element) data.

## 🏗️ Architecture

```
┌─────────────────┐
│  Flask API      │  ← Runs on BASE MACHINE (localhost:5000)
│  (api.py)       │     Streams TLE data
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ Kafka Producer  │  ← Runs on BASE MACHINE
│(kafka_producer) │     Sends data to Kafka
└────────┬────────┘
         │
         ↓
┌─────────────────────────────────────────┐
│           DOCKER SERVICES                │
│  ┌─────────┐  ┌──────┐  ┌──────────┐   │
│  │  Kafka  │→ │ Spark│→ │   HDFS   │   │
│  └─────────┘  └──────┘  └──────────┘   │
│                    ↓                     │
│              ┌──────────┐               │
│              │Cassandra │               │
│              └──────────┘               │
└─────────────────────────────────────────┘
```

## 📋 Prerequisites

- **Docker & Docker Compose**: For running Kafka, Spark, HDFS, Cassandra
- **Python 3.8+**: For Flask API and Kafka producer (on your base machine)
- **pip**: Python package manager

## 🚀 Quick Start

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

The main packages needed:
- Flask
- kafka-python
- requests

### 2. Start Docker Services

```bash
./scripts/start.sh
```

This starts:
- **Kafka** (localhost:9092) - Message broker
- **Kafka UI** (localhost:8090) - Web interface for Kafka
- **HDFS** (localhost:9870) - Distributed file system
- **Spark** (localhost:8080) - Data processing
- **Cassandra** (localhost:9042) - Database
- **Airflow** (localhost:8088) - Workflow orchestration (admin/admin)

### 3. Start Flask API (on Base Machine)

In a new terminal:

```bash
python3 api.py
```

The API will be available at `http://localhost:5000`

### 4. Test the Setup

```bash
./scripts/test_pipeline.sh
```

### 5. Stream Data to Kafka

In another terminal:

```bash
# Stream 100 records (fast test)
python3 kafka_producer.py --limit 100

# Stream all data with 500x acceleration
python3 kafka_producer.py --acceleration 500

# Stream continuously
python3 kafka_producer.py
```

## 📁 Project Structure

```
.
├── api.py                      # Flask API (runs on base machine)
├── kafka_producer.py           # Kafka producer (runs on base machine)
├── docker-compose.yml          # Docker services configuration
├── requirements.txt            # Python dependencies
│
├── scripts/
│   ├── start.sh               # Start all Docker services
│   ├── stop.sh                # Stop all Docker services
│   ├── status.sh              # Check service status
│   └── test_pipeline.sh       # Test the complete pipeline
│
├── data/
│   └── processed/
│       └── TLE_History/       # TLE data files (*.csv)
│
├── pipelines/
│   ├── ingestion/             # Data ingestion scripts
│   └── processing/            # Data processing (Spark jobs)
│
└── notebooks/                 # Jupyter notebooks for analysis
```

## 🔧 Service Management

### Start Services
```bash
./scripts/start.sh
```

### Check Status
```bash
./scripts/status.sh
```

### Stop Services
```bash
./scripts/stop.sh
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
docker-compose logs -f spark-master
```

## 🌐 Service URLs

Once started, access these URLs:

| Service | URL | Description |
|---------|-----|-------------|
| Flask API | http://localhost:5000 | TLE streaming API |
| Kafka UI | http://localhost:8090 | Kafka management UI |
| HDFS NameNode | http://localhost:9870 | HDFS web interface |
| Spark Master | http://localhost:8080 | Spark cluster UI |
| Spark Worker | http://localhost:8081 | Spark worker UI |
| Airflow | http://localhost:8088 | Workflow orchestration (admin/admin) |

## 📡 Flask API Endpoints

### `GET /`
API documentation

### `GET /health`
Health check
```bash
curl http://localhost:5000/health
```

### `GET /stats`
Dataset statistics
```bash
curl http://localhost:5000/stats
```

### `GET /stream`
Stream TLE data

Parameters:
- `acceleration` - Speed multiplier (default: 100)
- `limit` - Max records to stream (optional)
- `mode` - Streaming mode: adaptive/fixed/proportional (default: adaptive)
- `max_delay` - Max delay between records in seconds (default: 5.0)

Examples:
```bash
# Stream with default settings
curl http://localhost:5000/stream

# Stream 10 records
curl "http://localhost:5000/stream?limit=10"

# Stream at 500x speed
curl "http://localhost:5000/stream?acceleration=500"
```

## 🔌 Kafka Producer Usage

```bash
# Basic usage - stream all data
python3 kafka_producer.py

# Stream limited records
python3 kafka_producer.py --limit 1000

# Fast streaming (1000x acceleration)
python3 kafka_producer.py --acceleration 1000

# Custom Kafka topic
python3 kafka_producer.py --topic my_custom_topic

# Full options
python3 kafka_producer.py \
    --api-url http://localhost:5000 \
    --kafka-servers localhost:9092 \
    --topic space_debris_tle \
    --acceleration 100 \
    --limit 5000 \
    --mode adaptive
```

## 🐛 Troubleshooting

### Flask API won't start
```bash
# Check if port 5000 is in use
lsof -i :5000

# Kill process using port 5000
kill -9 $(lsof -t -i:5000)
```

### Docker services not starting
```bash
# Check Docker status
docker ps -a

# Restart Docker
sudo systemctl restart docker

# Remove old containers and volumes
docker-compose down -v
./scripts/start.sh
```

### Kafka connection issues
```bash
# Check if Kafka is running
nc -z localhost 9092

# Check Kafka logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

### No TLE data found
```bash
# Verify data directory exists
ls -la data/processed/TLE_History/

# Check for CSV files
ls data/processed/TLE_History/*.csv | wc -l
```

## 🧪 Testing

### Test Flask API
```bash
# Health check
curl http://localhost:5000/health

# Get stats
curl http://localhost:5000/stats

# Stream 5 records
curl "http://localhost:5000/stream?limit=5"
```

### Test Kafka
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Create test topic
docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --partitions 1 \
    --replication-factor 1

# Consume from topic
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic space_debris_tle \
    --from-beginning \
    --max-messages 10
```

### Full Pipeline Test
```bash
./scripts/test_pipeline.sh
```

## 📊 Data Flow

1. **TLE Data** → CSV files in `data/processed/TLE_History/`
2. **Flask API** → Loads and streams TLE data from CSV files
3. **Kafka Producer** → Consumes Flask API stream and publishes to Kafka
4. **Kafka** → Stores streaming data in topics
5. **Spark** → Processes data from Kafka
6. **HDFS/Cassandra** → Stores processed results

## 🔐 Configuration

### Kafka Configuration
Edit [docker-compose.yml](docker-compose.yml):
- Port: 9092
- Internal broker: kafka:9093

### Flask API Configuration
Edit [api.py](api.py):
- Port: 5000
- Data directory: `data/processed/TLE_History`
- Default acceleration: 100x

### Kafka Producer Configuration
Edit [kafka_producer.py](kafka_producer.py):
- API URL: http://localhost:5000
- Kafka servers: localhost:9092
- Topic: space_debris_tle

## 📝 Development

### Add New Kafka Consumer
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'space_debris_tle',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(message.value)
```

### Add New Spark Job
Place your PySpark scripts in `pipelines/processing/` and run:
```bash
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /path/to/your/script.py
```

## 📚 Additional Documentation

- [Quick Reference](docs/QUICK_REFERENCE.md)
- [Streaming Architecture](docs/STREAMING_ARCHITECTURE.md)
- [SGP4 & HDFS Guide](docs/README_SGP4_HDFS.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is for educational purposes.

## 🆘 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review logs: `docker-compose logs -f`
3. Check service status: `./scripts/status.sh`

---

**Note**: The Flask API runs on your **base machine**, not in Docker. This allows for easier development and debugging. All other services (Kafka, Spark, HDFS, Cassandra) run in Docker containers.
