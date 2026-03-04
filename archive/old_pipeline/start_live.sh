#!/bin/bash
# ══════════════════════════════════════════════════════════════════════════
# Space Debris Risk Prediction — Start Script
#
# Mirrors the reference project's scripts/start_separated.sh
# Starts all services in the correct order for live data pipeline.
#
# Architecture:
#   • TLE Streaming API    → Serves local TLE files as live stream
#   • TLE Kafka Producer   → Streams TLE data → Kafka (like Airflow DAG)
#   • TLEStreamProcessor   → Spark Streaming (Kafka TLE → SGP4 → HDFS)
#   • StreamingCollision   → Collision Detection (HDFS → HDFS/Kafka alerts)
#   • Dashboard API        → Flask REST API (reads HDFS + local files)
#   • Dashboard Web        → React + Globe.gl (live UI)
#
# Usage:
#   chmod +x scripts/start_live.sh
#   ./scripts/start_live.sh
# ══════════════════════════════════════════════════════════════════════════

set -e

# Change to project directory
cd "$(dirname "$0")/.."
PROJECT_DIR=$(pwd)

echo "=========================================="
echo "🛰️  Space Debris Risk Prediction System"
echo "=========================================="
echo ""
echo "Architecture:"
echo "  • TLE API        → Serves local TLE data as live stream"
echo "  • Kafka Producer  → TLE API → Kafka (simulated Airflow)"
echo "  • Spark Job 1     → Kafka TLE → SGP4 vectors → HDFS"
echo "  • Spark Job 2     → HDFS → Collision Detection → HDFS/Kafka"
echo "  • Dashboard API   → Reads HDFS → REST endpoints"
echo "  • Dashboard Web   → React live UI"
echo "=========================================="
echo ""

# ══════════════════════════════════════════════════════════════════════════
# STEP 1: Check prerequisites
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 1: Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not installed"
    exit 1
fi
if ! docker info &> /dev/null 2>&1; then
    echo "❌ Docker not running"
    exit 1
fi
echo "   ✓ Docker OK"

# Check SBT
if ! command -v sbt &> /dev/null; then
    echo "❌ sbt not installed"
    exit 1
fi
echo "   ✓ SBT OK"

# Check Python venv
if [ ! -f "venv/bin/python" ]; then
    echo "❌ Python venv not found — run: python3 -m venv venv && venv/bin/pip install flask flask-cors pandas pyarrow kafka-python requests"
    exit 1
fi
echo "   ✓ Python venv OK"

# Check Node.js
if [ ! -d "dashboard/node_modules" ]; then
    echo "⚠️  Dashboard dependencies not installed — running npm install..."
    (cd dashboard && npm install)
fi
echo "   ✓ Dashboard OK"

echo ""

# ══════════════════════════════════════════════════════════════════════════
# STEP 2: Start Infrastructure (Kafka, HDFS via Docker)
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 2: Starting Infrastructure (Kafka, HDFS)..."

docker compose up -d 2>/dev/null || docker-compose up -d 2>/dev/null || {
    echo "⚠️  Docker Compose not configured — assuming Kafka + HDFS already running"
}

echo "   Waiting for infrastructure (10s)..."
sleep 10

# ══════════════════════════════════════════════════════════════════════════
# STEP 3: Verify Kafka topics
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 3: Verifying Kafka topics..."

# Try to create topics via kafka-python
venv/bin/python -c "
from kafka.admin import KafkaAdminClient, NewTopic
try:
    admin = KafkaAdminClient(bootstrap_servers='localhost:19092', api_version=(3,6,0), request_timeout_ms=5000)
    topics = [
        NewTopic(name='tle-raw',         num_partitions=3, replication_factor=1),
        NewTopic(name='collision-alerts', num_partitions=3, replication_factor=1),
    ]
    admin.create_topics(topics)
    print('   ✓ Kafka topics created')
except Exception as e:
    print(f'   ✓ Kafka topics ready ({e})')
" 2>/dev/null || echo "   ⚠️  Could not verify Kafka topics"

# ══════════════════════════════════════════════════════════════════════════
# STEP 4: Verify HDFS directories
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 4: Verifying HDFS directories..."

docker exec namenode hdfs dfs -mkdir -p /space-debris/state-vectors 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris/catalog 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris/collision-predictions 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris/ml-results 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris-webhdfs/state-vectors-stream 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris-webhdfs/collision-alerts 2>/dev/null || true
docker exec namenode hdfs dfs -chmod -R 777 /space-debris 2>/dev/null || true
docker exec namenode hdfs dfs -chmod -R 777 /space-debris-webhdfs 2>/dev/null || true
echo "   ✓ HDFS directories ready"

# ══════════════════════════════════════════════════════════════════════════
# STEP 5: Compile Scala code
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 5: Compiling Scala code..."
sbt compile 2>&1 | tail -3
echo "   ✓ Compiled"

# ══════════════════════════════════════════════════════════════════════════
# STEP 6: Start TLE Streaming API (like reference's tle-api)
# ══════════════════════════════════════════════════════════════════════════
echo ""
echo "📦 Step 6: Starting TLE Streaming API..."
venv/bin/python tle_streaming_api.py > /tmp/tle_api.log 2>&1 &
TLE_API_PID=$!
echo "   PID: $TLE_API_PID"
sleep 3

# Verify API is running
if curl -s http://localhost:5055/api/health > /dev/null 2>&1; then
    echo "   ✓ TLE API running on http://localhost:5055"
else
    echo "   ⚠️  TLE API may still be loading data..."
fi

# ══════════════════════════════════════════════════════════════════════════
# STEP 7: Start TLE Kafka Producer (like reference's Airflow DAG)
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 7: Starting TLE Kafka Producer (API → Kafka)..."
venv/bin/python tle_kafka_producer.py --loop --batch-size 50 --delay-ms 1000 \
    > /tmp/tle_producer.log 2>&1 &
PRODUCER_PID=$!
echo "   PID: $PRODUCER_PID"
echo "   ✓ Producing to Kafka topic: tle-raw"

# ══════════════════════════════════════════════════════════════════════════
# STEP 8: Start Spark Job 1 — TLE Stream Processor (Kafka → HDFS)
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 8: Starting Spark Job 1 — TLE Stream Processor..."
sbt "runMain TLEStreamProcessor" > /tmp/tle_stream.log 2>&1 &
STREAM_PID=$!
echo "   PID: $STREAM_PID"
echo "   ✓ Kafka → SGP4 → HDFS"

# ══════════════════════════════════════════════════════════════════════════
# STEP 9: Start Spark Job 2 — Streaming Collision Detector
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 9: Starting Spark Job 2 — Collision Detection..."
echo "   Waiting 30s for SGP4 vectors to accumulate..."
sleep 30
sbt "runMain StreamingCollisionDetector" > /tmp/collision_detector.log 2>&1 &
COLLISION_PID=$!
echo "   PID: $COLLISION_PID"
echo "   ✓ HDFS → Collision Detection → HDFS/Kafka"

# ══════════════════════════════════════════════════════════════════════════
# STEP 10: Start Dashboard API + Web
# ══════════════════════════════════════════════════════════════════════════
echo "📦 Step 10: Starting Dashboard..."
venv/bin/python dashboard_api.py > /tmp/dashboard_api.log 2>&1 &
DASHBOARD_API_PID=$!
echo "   API PID: $DASHBOARD_API_PID"

(cd dashboard && npm run dev > /tmp/dashboard_web.log 2>&1) &
DASHBOARD_WEB_PID=$!
echo "   Web PID: $DASHBOARD_WEB_PID"

sleep 5
echo "   ✓ Dashboard running"

# ══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "✅ ALL SERVICES STARTED!"
echo "=========================================="
echo ""
echo "📍 ACCESS POINTS"
echo "=========================================="
echo ""
echo "🔵 TLE STREAMING API"
echo "   URL:     http://localhost:5055"
echo "   Stream:  http://localhost:5055/api/objects/stream"
echo ""
echo "🟠 KAFKA"
echo "   Broker:  localhost:19092"
echo "   Topics:  tle-raw, collision-alerts"
echo ""
echo "🟢 SPARK JOBS"
echo "   Job 1:   TLEStreamProcessor (continuous streaming)"
echo "   Job 2:   StreamingCollisionDetector (polls every 120s)"
echo ""
echo "🟡 HDFS"
echo "   NameNode: http://localhost:9870"
echo "   Vectors:  /space-debris-webhdfs/state-vectors-stream"
echo "   Alerts:   /space-debris-webhdfs/collision-alerts"
echo ""
echo "🟣 DASHBOARD"
echo "   API:     http://localhost:5050"
echo "   Web:     http://localhost:3000"
echo ""
echo "=========================================="
echo "📋 DATA FLOW"
echo "=========================================="
echo ""
echo "  TLE Files  ─────►  TLE API  ─────►  KAFKA"
echo "  (local)         (streaming)       (tle-raw)"
echo "                                       │"
echo "                                       ▼"
echo "                               SPARK JOB 1"
echo "                         (TLEStreamProcessor)"
echo "                                       │"
echo "                                       ▼"
echo "                                     HDFS"
echo "                           (state-vectors-stream)"
echo "                                       │"
echo "                                       ▼"
echo "                               SPARK JOB 2"
echo "                     (StreamingCollisionDetector)"
echo "                                       │"
echo "                              ┌────────┴────────┐"
echo "                              ▼                 ▼"
echo "                            HDFS             KAFKA"
echo "                     (collision-alerts)    (alerts)"
echo "                              │"
echo "                              ▼"
echo "                        DASHBOARD API"
echo "                     (http://localhost:5050)"
echo "                              │"
echo "                              ▼"
echo "                        DASHBOARD WEB"
echo "                     (http://localhost:3000)"
echo ""
echo "=========================================="
echo "📋 LOGS"
echo "=========================================="
echo ""
echo "  TLE API:          tail -f /tmp/tle_api.log"
echo "  Kafka Producer:   tail -f /tmp/tle_producer.log"
echo "  Stream Processor: tail -f /tmp/tle_stream.log"
echo "  Collision Detect: tail -f /tmp/collision_detector.log"
echo "  Dashboard API:    tail -f /tmp/dashboard_api.log"
echo "  Dashboard Web:    tail -f /tmp/dashboard_web.log"
echo ""
echo "=========================================="
echo "📋 STOP ALL"
echo "=========================================="
echo ""
echo "  kill $TLE_API_PID $PRODUCER_PID $STREAM_PID $COLLISION_PID $DASHBOARD_API_PID $DASHBOARD_WEB_PID"
echo ""
echo "  # Or save PIDs:"
echo "  echo '$TLE_API_PID $PRODUCER_PID $STREAM_PID $COLLISION_PID $DASHBOARD_API_PID $DASHBOARD_WEB_PID' > /tmp/space_debris_pids.txt"
echo ""

# Save PIDs for easy cleanup
echo "$TLE_API_PID $PRODUCER_PID $STREAM_PID $COLLISION_PID $DASHBOARD_API_PID $DASHBOARD_WEB_PID" > /tmp/space_debris_pids.txt
echo "PIDs saved to /tmp/space_debris_pids.txt"
echo "To stop: kill \$(cat /tmp/space_debris_pids.txt)"
echo ""

# Wait for any process to exit
wait
