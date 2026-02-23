# Complete System Architecture - Visual Overview

## System Components

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATION LAYER                                 │
│                          Apache Airflow (8091)                                │
│                                                                               │
│  DAGs:                                                                        │
│  ┌────────────────────┐  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │ TLE API to Kafka   │  │ SGP4 Streaming  │  │ Collision Prediction     │  │
│  │ (Every 2 min)      │  │ (Continuous)    │  │ (Every 6 hours)          │  │
│  └────────────────────┘  └─────────────────┘  └──────────────────────────┘  │
└───────┬──────────────────────────┬────────────────────────┬──────────────────┘
        │                          │                        │
        ▼                          ▼                        ▼
┌──────────────────┐      ┌──────────────────┐    ┌──────────────────┐
│   TLE Stream     │      │   Spark Master   │    │   Spark Master   │
│   API (5000)     │      │    (7077)        │    │    (7077)        │
│                  │      │                  │    │                  │
│ Space-track.org  │      │  SGP4 Worker     │    │ Collision Worker │
│ TLE Data Fetch   │      │  ├─ Read Kafka   │    │ ├─ Read HDFS     │
└─────────┬────────┘      │  ├─ Compute SGP4 │    │ ├─ Propagate     │
          │               │  └─ Write HDFS   │    │ ├─ Detect        │
          ▼               └──────────┬───────┘    │ └─ Classify      │
┌──────────────────┐               │             └─────────┬──────────┘
│  Kafka Broker    │◄──────────────┘                       │
│    (9092/9093)   │                                        │
│                  │                                        │
│ Topics:          │                                        │
│ • space_debris_tle          │                            │
│ • space_debris_collisions ◄─┘                            │
└─────────┬────────┘                                        │
          │                                                 │
          ▼                                                 │
┌──────────────────────────────────────────────────────────┼──────────┐
│                    HDFS STORAGE (9000)                    │          │
│                                                           ▼          │
│  Directories:                                    ┌────────────────┐  │
│  ┌────────────────────────────────────────┐     │  Collision     │  │
│  │  /space-debris/sgp4_vectors/           │     │  Predictions   │  │
│  │  ├─ Satellite positions (x,y,z)        │     │                │  │
│  │  ├─ Velocities (vx,vy,vz)              │     │  Partitioned:  │  │
│  │  ├─ Altitude, orbital params            │     │  • HIGH        │  │
│  │  └─ Partitioned by timestamp           │     │  • MEDIUM      │  │
│  └────────────────────────────────────────┘     │  • LOW         │  │
│                                                  └────────────────┘  │
│  Format: Parquet (compressed, columnar)                             │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Query
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       VISUALIZATION LAYER                            │
│                                                                      │
│  ┌────────────────────────────┐    ┌──────────────────────────┐    │
│  │   Dashboard API (5001)     │◄───│   Dashboard Web (8080)   │    │
│  │                            │    │                          │    │
│  │  Flask + PySpark           │    │  Nginx + Static Files    │    │
│  │                            │    │                          │    │
│  │  Endpoints:                │    │  Features:               │    │
│  │  • /api/health             │    │  • Risk Summary Cards    │    │
│  │  • /api/collisions/stats   │    │  • Timeline Charts       │    │
│  │  • /api/collisions/high-risk│   │  • Distance Statistics   │    │
│  │  • /api/collisions/timeline│    │  • Alert Tables          │    │
│  │  • /api/satellites/pairs   │    │  • Auto-refresh (30s)    │    │
│  │  • /api/satellites/tracking│    │  • Real-time updates     │    │
│  │  • /api/config             │    │                          │    │
│  └────────────────────────────┘    └──────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
┌─────────────┐
│ Space Track │  External TLE data source
│  API (Web)  │
└──────┬──────┘
       │ HTTPS (OAuth)
       ▼
┌─────────────┐
│  TLE API    │  Flask service (5000)
│  Service    │  Fetches & formats TLE data
└──────┬──────┘
       │ Publish
       ▼
┌─────────────┐
│    Kafka    │  Message broker
│   Topic 1   │  space_debris_tle
└──────┬──────┘
       │ Stream
       ▼
┌─────────────┐
│   Spark     │  Streaming job
│ SGP4 Worker │  Computes position vectors
└──────┬──────┘
       │ Write
       ▼
┌─────────────┐
│    HDFS     │  Distributed storage
│ SGP4 Vectors│  Parquet files
└──────┬──────┘
       │ Read (scheduled)
       ▼
┌─────────────┐
│   Spark     │  Batch job (every 6h)
│  Collision  │  Predicts & detects collisions
│   Worker    │
└──────┬──────┘
       │
       ├─────Write────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│    HDFS     │    │    Kafka    │  HIGH/MEDIUM alerts
│ Predictions │    │   Topic 2   │  space_debris_collisions
└──────┬──────┘    └─────────────┘
       │ Query
       ▼
┌─────────────┐
│ Dashboard   │  Flask API (5001)
│     API     │  Serves JSON data
└──────┬──────┘
       │ HTTP/REST
       ▼
┌─────────────┐
│ Dashboard   │  Web UI (8080)
│     Web     │  Visualization & monitoring
└─────────────┘
```

## Technology Stack

```
┌─────────────────────────────────────────────────────┐
│                  APPLICATION LAYER                  │
│                                                     │
│  Python 3.8+                                        │
│  • Flask 3.0 (API)                                  │
│  • PySpark 3.5 (Processing)                         │
│  • SGP4 2.23 (Orbital mechanics)                    │
│  • Kafka-Python 2.0 (Messaging)                     │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│                PROCESSING LAYER                     │
│                                                     │
│  Apache Spark 3.5.0                                 │
│  • Structured Streaming                             │
│  • DataFrame API                                    │
│  • UDFs for SGP4 computation                        │
│  • Adaptive Query Execution                         │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│                  STORAGE LAYER                      │
│                                                     │
│  Hadoop HDFS 3.3.4                                  │
│  • NameNode (metadata)                              │
│  • DataNode (blocks)                                │
│  • Parquet format                                   │
│  • Snappy compression                               │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│                 MESSAGING LAYER                     │
│                                                     │
│  Apache Kafka 7.5.0                                 │
│  • Zookeeper coordination                           │
│  • 2 Topics (TLE, Collisions)                       │
│  • 168h retention                                   │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│               ORCHESTRATION LAYER                   │
│                                                     │
│  Apache Airflow 2.7+                                │
│  • LocalExecutor                                    │
│  • PostgreSQL metadata DB                           │
│  • 3 DAGs (TLE, SGP4, Collision)                    │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              VISUALIZATION LAYER                    │
│                                                     │
│  Frontend:                                          │
│  • HTML5 + CSS3                                     │
│  • Vanilla JavaScript                               │
│  • Chart.js 4.4 (charts)                            │
│  • Nginx (web server)                               │
│                                                     │
│  Backend:                                           │
│  • Flask 3.0 (REST API)                             │
│  • Flask-CORS (cross-origin)                        │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              CONTAINERIZATION                       │
│                                                     │
│  Docker Compose                                     │
│  • 15+ microservices                                │
│  • Custom bridge network                            │
│  • Volume persistence                               │
│  • Health checks                                    │
└─────────────────────────────────────────────────────┘
```

## Port Map

| Service | Port | Description |
|---------|------|-------------|
| TLE API | 5000 | TLE data streaming API |
| Dashboard API | 5001 | Collision data REST API |
| Kafka | 9092 | External connections |
| Kafka | 9093 | Internal container network |
| Spark Master | 7077 | Spark submit endpoint |
| Spark Master UI | 8081 | Web UI for Spark cluster |
| Spark Worker UI | 8082 | Worker monitoring |
| Dashboard Web | 8080 | Main visualization UI |
| Kafka UI | 8090 | Kafka topic browser |
| Airflow Web | 8091 | DAG management UI |
| HDFS NameNode | 9870 | HDFS web UI |
| HDFS DataNode | 9864 | DataNode info |
| PostgreSQL | 5432 | Airflow metadata DB |
| Zookeeper | 2181 | Kafka coordination |

## Configuration Files

```
.env                          # Environment variables
docker-compose.yml            # Service definitions
requirements-api.txt          # TLE API dependencies
requirements-airflow.txt      # Airflow dependencies
requirements-dashboard.txt    # Dashboard dependencies
requirements.txt              # Base dependencies
```

## Key Features

✅ Real-time TLE data ingestion  
✅ SGP4 orbital propagation  
✅ Collision detection & prediction  
✅ Risk classification (HIGH/MEDIUM/LOW)  
✅ Time-series storage in HDFS  
✅ Automated scheduling with Airflow  
✅ Interactive web dashboard  
✅ RESTful API for data access  
✅ Kafka-based event streaming  
✅ Scalable Spark processing  
✅ Health monitoring & alerts  

## Data Models

### TLE Message (Kafka → Spark)
```json
{
  "message_id": "uuid",
  "satellite_id": "25544",
  "epoch": "2026-02-01T12:00:00Z",
  "tle_line1": "1 25544U...",
  "tle_line2": "2 25544...",
  "inclination": 51.6,
  "raan": 123.45,
  "eccentricity": "0.0001234",
  "argument_of_perigee": 67.89,
  "mean_anomaly": 123.45,
  "mean_motion": 15.54,
  "revolution_number": 12345
}
```

### SGP4 Vector (HDFS Storage)
```json
{
  "satellite_id": "25544",
  "timestamp": "2026-02-01T12:00:00Z",
  "pos_x": 1234.56,
  "pos_y": -2345.67,
  "pos_z": 3456.78,
  "vel_x": 1.234,
  "vel_y": -2.345,
  "vel_z": 3.456,
  "altitude_km": 408.5,
  "velocity_magnitude": 7.66,
  "tle_line1": "1 25544U...",
  "tle_line2": "2 25544..."
}
```

### Collision Prediction (HDFS + Kafka)
```json
{
  "satellite_1": "25544",
  "satellite_2": "12345",
  "collision_time": "2026-02-05T08:30:00Z",
  "distance_km": 3.45,
  "risk_level": "HIGH",
  "sat1_pos_x": 1234.56,
  "sat1_pos_y": -2345.67,
  "sat1_pos_z": 3456.78,
  "sat2_pos_x": 1230.11,
  "sat2_pos_y": -2340.22,
  "sat2_pos_z": 3460.33,
  "detection_timestamp": "2026-02-01T12:00:00Z"
}
```

## Scalability & Performance

- **Horizontal Scaling**: Add more Spark workers
- **Storage**: HDFS replication factor = 1 (adjustable)
- **Throughput**: ~1000 TLE messages/minute
- **Latency**: < 5 seconds (TLE → HDFS)
- **Collision Detection**: O(n²) optimized with Spark
- **Dashboard Update**: 30 seconds (configurable)

## Security Considerations

- Environment variables for credentials
- No exposed passwords in code
- Internal Docker network isolation
- OAuth for Space-Track API
- CORS enabled for dashboard API
- Health check endpoints

---

**Last Updated**: February 2026  
**Version**: 2.0.0 (Collision Prediction System)
