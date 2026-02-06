# Space Debris Risk Prediction 🛰️

> A scalable big data pipeline for space debris tracking, collision prediction, and real-time risk monitoring

[![Docker](https://img.shields.io/badge/Docker-Ready-blue)](docker-compose.yml)
[![Python](https://img.shields.io/badge/Python-3.8+-green)](requirements-api.txt)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-orange)](pipelines/)
[![Dashboard](https://img.shields.io/badge/Dashboard-Live-success)](http://localhost:8080)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Airflow Orchestration Layer                 │
│  • TLE API Ingestion → Kafka (every 2 min)             │
│  • SGP4 Processing → HDFS                               │
│  • Collision Prediction (every 6 hours)                 │
└────────────────────┬────────────────────────────────────┘
                     ↓
         ┌───────────────────┐
         │   Kafka Broker    │
         │  Topics:          │
         │  • space_debris_tle         │
         │  • space_debris_collisions  │
         └───────┬───────────┘
                 ↓
    ┌────────────────────────┐
    │   Spark Processing     │
    │  • SGP4 Streaming      │
    │  • Collision Detection │
    └────────┬───────────────┘
             ↓
    ┌────────────────────────────────────────┐
    │           Data Storage Layer           │
    │  ┌──────────┐      ┌──────────────┐   │
    │  │   HDFS   │      │  PostgreSQL  │   │
    │  │ (Archive)│      │ (Dashboard)  │   │
    │  └────┬─────┘      └──────┬───────┘   │
    └───────┼───────────────────┼───────────┘
            └─────────┬─────────┘
                      ↓
    ┌────────────────────────┐
    │   Dashboard System     │
    │  • Flask API (5001)    │
    │  • Web UI (8082)       │
    └────────────────────────┘
```

## 🚀 Quick Start

**Total setup time: ~2 minutes**

### Option 1: Full System with Collision Prediction
```bash
# Start complete system including collision prediction
./scripts/start_collision_system.sh

# Access dashboard
# Open browser: http://localhost:8080
```

### Option 2: Base System Only
```bash
# 1. Start all services
./scripts/start.sh

# 2. Start Flask API (new terminal)
python3 api.py
```


# 2. Check status
./scripts/status.sh

# 3. Monitor logs
tail -f logs/spark_streaming.log
```

📖 **Details**: [CLEAN_SOLUTION.md](CLEAN_SOLUTION.md)

## 📋 Services

| Service | Port | Description |
|---------|------|-------------|
| Dashboard Web UI | 8082 | Real-time collision dashboard |
| Dashboard API | 5001 | REST API for dashboard data |
| Airflow UI | 8088 | Workflow orchestration & monitoring |
| Kafka Broker | 9092 | Message streaming |
| Kafka UI | 8090 | Kafka monitoring |
| Spark Master | 8080 | Spark cluster UI |
| Spark App | 4040 | Running job monitoring |
| HDFS NameNode | 9870 | Distributed storage UI |
| PostgreSQL | 5433 | Dashboard database |
| Zookeeper | 2181 | Coordination service |

## 📁 Project Structure

```
.
├── docker-compose.yml           # All services configuration
├── scripts/
│   ├── start.sh                # Start all + Spark job ⭐
│   ├── stop.sh                 # Stop everything
│   └── submit_spark_job.sh     # Manual Spark job trigger
│
├── pipelines/
│   ├── ingestion/
│   │   ├── airflow_dag_api_to_kafka.py            # TLE API → Kafka
│   │   ├── airflow_dag_spark_streaming.py         # Trigger Spark job
│   │   └── airflow_dag_collision_prediction.py    # Collision prediction ✨
│   └── processing/
│       ├── spark_sgp4_to_hdfs.py                  # Kafka → SGP4 → HDFS
│       └── spark_collision_prediction.py          # Collision detection ✨
│
├── dashboard/                                      # Web Dashboard ✨
│   ├── index.html                                 # Main UI
│   ├── styles.css                                 # Styling
│   └── dashboard.js                               # Visualizations
│
├── dashboard_api.py                               # Flask API ✨
├── config/
│   ├── airflow/
│   │   └── submit_spark_wrapper.sh                # HTTP trigger client
│   └── docker/
│       └── spark_rest_trigger.py                  # HTTP trigger service
│
└── CLEAN_SOLUTION.md                              # Architecture docs ⭐
└── COLLISION_SYSTEM_SUMMARY.md                    # New features ✨
```

## 🔧 Management Commands

```bash
./scripts/start_collision_system.sh  # Start complete system with dashboard ✨
./scripts/test_collision_system.sh   # Test all components ✨
./scripts/start.sh                   # Start base system
./scripts/stop.sh                    # Stop everything
./scripts/status.sh                  # Check service status
./scripts/reload_dags.sh             # Reload Airflow DAGs
```

## 📊 Data Pipeline

### 1. TLE Data Ingestion (Automated)
- Airflow DAG runs every 2 minutes
- Fetches TLE data from space-track.org API
- Publishes to Kafka topic: `space_debris_tle`

### 2. SGP4 Vector Computation (Real-time)
- Spark streaming job reads from Kafka
- Computes orbital position/velocity vectors
- Writes to HDFS in Parquet format

### 3. Collision Prediction (Every 6 hours) ✨ NEW
- Reads SGP4 vectors from HDFS
- Propagates positions for next N days (configurable)
- Detects potential collisions via pairwise distance calculation
- Classifies risks: HIGH (<5km), MEDIUM (<10km), LOW (<50km)
- Saves predictions to HDFS (historical archive)
- Writes to PostgreSQL (real-time dashboard queries)
- Publishes alerts to Kafka topic: `space_debris_collisions`

### 4. PostgreSQL Dashboard Integration ✨ NEW
The collision prediction pipeline writes directly to PostgreSQL for fast dashboard queries:

```
Spark Collision Job
    ↓
    ├── HDFS (historical archive)
    ├── PostgreSQL (real-time dashboard)
    └── Kafka (streaming alerts)
```

**PostgreSQL Tables:**
- `satellites` - Current satellite tracking status
- `collision_alerts` - Active collision predictions
- `tracking_status_changes` - Audit log of status changes
- `system_metrics` - Dashboard metrics

**Why Spark → PostgreSQL?**
- Single pipeline, no additional components
- Near real-time updates (immediate after detection)
- Simple architecture with JDBC writes

### 5. Storage Structure
```bash
hdfs://namenode:9000/space-debris/
  ├── sgp4_vectors/                  # Computed vectors
  │   └── epoch_time=.../            # Partitioned by time
  ├── collision_predictions/         # Collision data ✨
  │   └── batch_YYYYMMDD_HHMMSS/    # Batch-timestamped
  └── tle_raw/                       # Raw TLE backup
      └── satellite_id=.../          # Partitioned by satellite

PostgreSQL (space_debris database):
  ├── satellites                     # Current tracking status
  ├── collision_alerts               # Active predictions (7-day window)
  ├── tracking_status_changes        # Audit log
  └── system_metrics                 # Dashboard metrics
```

## 🎨 Collision Dashboard ✨ NEW

Access the real-time dashboard at: **http://localhost:8080**

**Features:**
- 📊 Real-time collision statistics
- 📈 Risk distribution charts (HIGH/MEDIUM/LOW)
- ⏱️ Collision timeline (next 7 days)
- 🚨 High-risk alert table
- 🛰️ Satellite pair analysis
- 📏 Distance statistics
- 🔄 Auto-refresh every 30 seconds

**API Endpoints:**
- `GET /api/health` - System health
- `GET /api/collisions/stats` - Statistics
- `GET /api/collisions/high-risk` - Urgent alerts
- `GET /api/collisions/timeline` - Time-series data
- `GET /api/satellites/pairs` - Collision pairs
- `GET /api/config` - Configuration

## 🔍 Monitoring

### Dashboard Web UI ✨
```bash
# Access real-time visualization
open http://localhost:8080

# API health check
curl http://localhost:5001/api/health

# Get collision statistics
curl http://localhost:5001/api/collisions/stats | jq
```

### Check HDFS Data
```bash
# SGP4 vectors
docker exec namenode hdfs dfs -ls /space-debris/sgp4_vectors
docker exec namenode hdfs dfs -count /space-debris/sgp4_vectors

# Collision predictions ✨
docker exec namenode hdfs dfs -ls /space-debris/collision_predictions
docker exec namenode hdfs dfs -du -h /space-debris/collision_predictions
```
```

### Monitor Spark Job
```bash
# Check if running
curl -s http://localhost:8080 | grep "Running Applications"

# View application logs
tail -f logs/spark_streaming.log
```

### Airflow Monitoring
- Go to http://localhost:8088
- Check DAG: `tle_api_to_kafka_streaming`
- View execution history

## 🐛 Troubleshooting

```bash
# Check all services
./scripts/status.sh

# View logs
docker-compose logs -f [service-name]

# Restart Spark job if stopped
./scripts/submit_spark_job.sh

# Restart everything
./scripts/stop.sh && ./scripts/start.sh
```

## 📚 Documentation

- **[CLEAN_SOLUTION.md](CLEAN_SOLUTION.md)** - Architecture & design
- **[QUICKSTART.md](QUICKSTART.md)** - Quick reference guide
- **[docs/](docs/)** - Additional documentation

## 🎯 Key Features

- ✨ **Fully Automated**: TLE ingestion runs every 2 minutes via Airflow
- 🚀 **Real-time Processing**: Spark streaming processes data continuously
- 💾 **Efficient Storage**: Parquet format with time-based partitioning
- 📊 **Easy Monitoring**: Web UIs for all services
- 🔧 **Simple Management**: One command to start everything
- 🚀 **Fast Setup**: One command to start all services
- 📊 **Real-time Streaming**: TLE data streaming with configurable speed
- 🔍 **Monitoring**: Web UIs for all services
- 🧪 **Easy Testing**: Built-in test scripts
- 📦 **Scalable**: Spark + HDFS + Cassandra ready for big data

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repo
2. Create a feature branch
3. Test your changes
4. Submit a pull request

## 📄 License

MIT License - see LICENSE file

## 🆘 Need Help?

1. Check [QUICKSTART.md](QUICKSTART.md) for common issues
2. Review [SETUP.md](SETUP.md) for detailed troubleshooting
3. Check logs: `docker-compose logs -f [service-name]`
4. Run status check: `./scripts/status.sh`

---

**Made with ❤️ for Space Debris Risk Prediction**

*Flask API runs on your base machine for easy development. All other services run in Docker for isolation and scalability.*

│   │   └── test_*.sh               # Testing scripts
│   └── testing/                     # Test automation scripts
│
├── notebooks/                       # Jupyter notebooks for analysis
│   └── kafka_sgp4_pipeline_test.ipynb
│
├── docs/                            # Project documentation
│   ├── README_OLD.md               # Original readme
│   ├── README_SGP4_HDFS.md         # SGP4 pipeline documentation
│   ├── STREAMING_ARCHITECTURE.md   # Architecture details
│   └── PPT_OUTLINE.md              # Presentation outline
│
├── logs/                            # Application logs (gitignored)
├── .temp/                           # Temporary files (gitignored)
│
├── pyproject.toml                   # Python project configuration (uv)
└── requirements.txt                 # Python dependencies
```

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Docker & Docker Compose
- UV package manager (recommended) or pip

### Installation

1. **Install Dependencies**
   ```bash
   uv sync
   # or
   pip install -r requirements.txt
   ```

2. **Start Infrastructure**
   ```bash
   ./scripts/setup/start-containers.sh
   ```

3. **Run the Pipeline**
   
   In separate terminals:
   
   ```bash
   # Terminal 1: Start TLE streaming API
   uv run pipelines/ingestion/api/tle_stream_api.py
   
   # Terminal 2: Start Kafka producer
   uv run pipelines/ingestion/tle_api_to_kafka_producer.py --limit 100
   
   # Terminal 3: Start Spark streaming job
   docker exec -u root spark-master /opt/spark/bin/spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
     /opt/spark/work-dir/pipelines/processing/spark_sgp4_to_hdfs.py \
     --kafka broker:29092
   ```

## 🔧 Configuration

### Hadoop/HDFS Configuration
Located in [config/hadoop/](config/hadoop/)
- `core-site.xml` - Core Hadoop configuration
- `hdfs-site.xml` - HDFS-specific settings

### Airflow DAGs
Located in [config/airflow/](config/airflow/)
- Workflow automation and scheduling configurations

### Docker Services
Located in [deployment/](deployment/)
- Multi-container orchestration with Kafka, Spark, HDFS, Airflow

## 📊 Data Pipeline Flow

1. **Data Ingestion**: TLE data fetched from Space-Track API → Kafka topics
2. **Stream Processing**: Spark consumes Kafka streams → SGP4 propagation → orbital positions
3. **Storage**: Processed data stored in HDFS in Parquet format
4. **Orchestration**: Airflow schedules and monitors the entire pipeline

## 🔍 Key Components

### Ingestion Layer
- **Kafka Producer**: Streams TLE data from APIs to Kafka topics
- **REST API**: Provides HTTP endpoints for TLE data streaming

### Processing Layer
- **Spark Streaming**: Real-time SGP4 orbital propagation calculations
- **HDFS Storage**: Distributed storage for processed orbital predictions

### Orchestration
- **Airflow**: Automates and schedules pipeline workflows

## 📈 Monitoring & Testing

Run tests using scripts in `scripts/operations/`:
```bash
./scripts/operations/test_e2e_pipeline.sh
./scripts/operations/test_sgp4_pipeline.sh
```

## 📝 Documentation

- [Architecture Details](docs/STREAMING_ARCHITECTURE.md)
- [SGP4 Pipeline Guide](docs/README_SGP4_HDFS.md)
- [Presentation Outline](docs/PPT_OUTLINE.md)

## 🎯 Project Objectives

- Process **36,000+ tracked space objects** in real-time
- Compute orbital positions using SGP4 propagation algorithm
- Store and analyze TLE data at scale using big data technologies
- Automate workflows with Apache Airflow
- Provide collision risk assessment capabilities

## 🛠️ Technology Stack

- **Languages**: Python 3.9+
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark (Structured Streaming)
- **Storage**: HDFS (Hadoop Distributed File System)
- **Orchestration**: Apache Airflow
- **Containerization**: Docker & Docker Compose
- **Package Management**: UV / pip

## 📄 License

[Add your license information here]

## 👥 Contributors

[Add contributor information here]

---

**Note**: The `data/`, `logs/`, and `.temp/` directories are excluded from version control via `.gitignore`.
