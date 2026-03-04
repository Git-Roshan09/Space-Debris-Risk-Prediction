<div align="center">

# Space Debris Risk Prediction System

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?style=for-the-badge&logo=scala&logoColor=white)](https://www.scala-lang.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-8.1.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![React](https://img.shields.io/badge/React-TypeScript-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Branch](https://img.shields.io/badge/branch-final-brightgreen?style=for-the-badge&logo=git&logoColor=white)](https://github.com/Git-Roshan09/Space-Debris-Risk-Prediction/tree/final)

A real-time orbital collision risk assessment pipeline combining SGP4 propagation, Apache Spark, and Kafka to monitor thousands of tracked objects simultaneously and surface high-risk conjunction events to a live 3D dashboard.

</div>

---

## Architecture

```
HDFS Archive (710 files · 168M rows · real TLE data)
        │
        ▼
live_ingest.py  ──  SGP4 propagation (sgp4 library)
        │               └── ECI state vectors at T=now
        ├──► Kafka  ──► topic: state-vectors-live
        └──► HDFS   ──► /space-debris/state-vectors/live_sv_*.parquet
                                    │
                                    ▼
                     CollisionPrediction.scala  (Spark 3.5 / Orekit 12)
                          1. Read SGP4 state vectors
                          2. Classify SATELLITE / DEBRIS / UNKNOWN
                          3. Deduplicate: latest position per object
                          4. Apply tracking validity filters
                          5. Detect SAT-SAT + SAT-DEB conjunction pairs
                          6. Classify risk: CRITICAL / HIGH / MEDIUM / LOW
                          7. Write alerts
                                    │
                        ┌───────────┴───────────┐
                        ▼                       ▼
                  HDFS batch_*/          Kafka topic:
               collision-predictions   space_debris_collisions
                                               │
                                               ▼
                                    dashboard_api.py  (Flask · port 5050)
                                               │
                                               ▼
                                    React Dashboard  (globe.gl · port 3000)
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Collision engine | Apache Spark | 3.5.0 |
| Collision engine | Scala | 2.12.18 |
| Orbital mechanics | Orekit + Hipparchus | 12.0 / 3.0 |
| Ingestion | Python + sgp4 | 3 |
| Message bus | Confluent Kafka (KRaft) | CP-Server 8.1.0 |
| Storage | Apache HDFS | 3.2.1 |
| Stream analytics | ksqlDB | 8.1.0 |
| Cache / pub-sub | Redis | 7.2 |
| API server | Flask | — |
| Dashboard | React + TypeScript + globe.gl | Vite build |
| Infrastructure | Docker Compose | — |

---

## Prerequisites

- Docker + Docker Compose
- Java 11+ (for sbt / Spark)
- sbt 1.x
- Python 3.9+
- Node.js 18+ (dashboard build only)

Python dependencies:

```
pip install sgp4 pandas pyarrow kafka-python requests flask flask-cors pyspark
```

---

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
```

Wait ~30 seconds for HDFS, Kafka, and Redis to initialise.

### 2. Verify services are healthy

```bash
docker compose ps
curl -s http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus | grep -i state
```

### 3. Run the full pipeline once

```bash
./run_pipeline.sh once
```

### 4. Start the background scheduler

```bash
# Default interval: 30 minutes
./run_pipeline.sh start

# Custom interval (minutes)
./run_pipeline.sh start 15
```

### 5. Start the dashboard API

```bash
python3 dashboard_api.py
```

### 6. Serve the dashboard

```bash
cd dashboard && npx serve dist -l 3000
```

Open `http://localhost:3000` in your browser.

---

## Service Ports

| Service | Port | Notes |
|---|---|---|
| HDFS NameNode WebUI | 9870 | WebHDFS + file browser |
| HDFS RPC | 9000 | Spark / Python client endpoint |
| HDFS DataNode | 9864 | — |
| Kafka broker | 19092 | External listener |
| Schema Registry | 8081 | Avro schema management |
| Kafka Connect | 8083 | — |
| ksqlDB | 18088 | Stream SQL queries |
| Confluent Control Center | 9021 | Kafka management UI |
| Hive Metastore | 9083 | Thrift |
| HiveServer2 | 10000 | JDBC |
| Redis | 6379 | — |
| Redis Insight | 5540 | Redis GUI |
| Dashboard API | 5050 | Flask REST API |
| Dashboard | 3000 | React frontend |

---

## Project Structure

```
Space-Debris-Risk-Prediction/
├── live_ingest.py              # SGP4 ingestion: HDFS archive → Kafka + HDFS
├── pipeline_scheduler.py       # Task scheduler (Airflow replacement)
├── run_pipeline.sh             # Shell wrapper: start / stop / status / logs
├── dashboard_api.py            # Flask REST API (port 5050)
├── build.sbt                   # Scala/Spark project definition
├── docker-compose.yml          # Full infrastructure stack
├── Dockerfile
├── .env                        # Space-Track credentials (not committed)
│
├── src/main/scala/
│   ├── CollisionPrediction.scala       # Primary Spark pipeline (runMain target)
│   ├── CollisionDetector.scala
│   ├── MLlibTraining.scala
│   ├── TLEStreamProcessor.scala
│   ├── TLEBatchProcessor.scala
│   ├── TLEProcessor.scala
│   └── StreamingCollisionDetector.scala
│
├── dashboard/
│   ├── src/pages/GlobePage.tsx         # 3D globe visualisation
│   └── dist/                           # Production build (npm run build)
│
├── data/raw/                           # Historical TLE text files (2004–2025)
├── orekit-data/                        # Orbital mechanics data (EOP, ephemerides)
├── Output/
│   ├── space_debris_catalog.csv
│   └── TLE_Processed/
│
├── scripts/
│   ├── data_fetch/                     # One-time Space-Track data fetchers
│   └── utils/                          # HDFS inspection, data generation tools
│
└── archive/
    ├── old_pipeline/                   # 17 superseded scripts (reference only)
    └── old_scala/                      # Early Scala prototypes
```

---

## Pipeline Details

### Scheduler (`pipeline_scheduler.py`)

Three tasks run sequentially on a configurable interval:

1. **Health check** — verifies HDFS NameNode and Kafka broker are reachable
2. **Live ingest** (`live_ingest.py`) — reads latest TLE per NORAD_ID from HDFS archive, propagates with SGP4, writes ECI state vectors
3. **Collision detection** (`sbt "runMain CollisionPrediction"`) — Spark job reads the freshly written parquet files, detects conjunction pairs, and emits alerts

### `run_pipeline.sh` Commands

```bash
./run_pipeline.sh start [interval_minutes]   # start background scheduler
./run_pipeline.sh stop                        # stop background scheduler
./run_pipeline.sh restart [interval_minutes] # restart with optional new interval
./run_pipeline.sh status                      # show PID and running state
./run_pipeline.sh once                        # single pipeline run, then exit
./run_pipeline.sh logs                        # tail scheduler.log
```

---

## Collision Risk Thresholds

| Level | Miss Distance |
|---|---|
| CRITICAL | ≤ 1.0 km |
| HIGH | ≤ 20.0 km |
| MEDIUM | ≤ 35.0 km |
| LOW | ≤ 50.0 km |

Conjunction pairs are evaluated for **SAT-SAT** and **SAT-DEB** geometries. DEB-DEB pairs are excluded to limit combinatorial explosion at scale.

---

## Running Individual Components

### Ingestion only

```bash
python3 live_ingest.py \
    --n-sat 5000 \
    --n-deb 10000 \
    --sample-file 2 \
    --no-kafka
```

| Flag | Description |
|---|---|
| `--n-sat N` | Number of satellite TLEs to propagate |
| `--n-deb N` | Number of debris TLEs to propagate |
| `--sample-file N` | Number of HDFS archive files to sample from |
| `--no-kafka` | Skip Kafka write, output to HDFS only |
| `--interval N` | Run on a loop every N seconds |

### Collision detection (Spark)

```bash
sbt "runMain CollisionPrediction"
```

Reads `hdfs://namenode:9000/space-debris/state-vectors/live_sv_*.parquet`, writes results to `/space-debris/collision-predictions/batch_YYYYMMDD_HHMMSS/`.

### Dashboard API

```bash
python3 dashboard_api.py
# Endpoints: /api/collisions/globe  /api/collisions  /api/stats  /api/health
```

### Dashboard (development)

```bash
cd dashboard
npm install
npm run dev        # Vite dev server
# or
npm run build && npx serve dist -l 3000   # production
```

---

## HDFS Data Layout

| Path | Contents |
|---|---|
| `/space-debris/state-vectors-archive/` | 710 parquet files · 168M rows · historical TLE+ECI |
| `/space-debris/state-vectors/` | `live_sv_*.parquet` — current run output |
| `/space-debris/collision-predictions/` | `batch_YYYYMMDD_HHMMSS/` directories |
| `/space-debris/catalog` | Space debris catalog |

---

## Configuration

Create a `.env` file in the project root with Space-Track credentials if fetching fresh TLE data:

```env
SPACETRACK_USER=your_email@example.com
SPACETRACK_PASS=your_password
```

The ingestion pipeline (`live_ingest.py`) reads from the local HDFS archive by default and does not require Space-Track access during normal operation.

---

## Sample Output

Latest pipeline run (single execution, `--run-once`):

```
Objects analysed : 15,755  (5,501 satellites + 10,254 debris)
Conjunction pairs: 73       (6 SAT-SAT + 67 SAT-DEB)
HIGH risk alerts : 7
Execution time   : 18.7 s
```

---

## Acknowledgements

- [Orekit](https://www.orekit.org/) — open-source space dynamics library
- [sgp4](https://pypi.org/project/sgp4/) — Python SGP4/SDP4 propagator
- [Space-Track.org](https://www.space-track.org/) — TLE data source
- [globe.gl](https://globe.gl/) — WebGL globe visualisation
- [Confluent Platform](https://www.confluent.io/) — Kafka distribution
