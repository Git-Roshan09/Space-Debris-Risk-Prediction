#!/bin/bash
# Start script for Space Debris Risk Prediction
# - Airflow ONLY handles API → Kafka ingestion
# - Spark Job 1: SGP4 Streaming (Kafka → HDFS) - runs continuously
# - Spark Job 2: Collision Prediction (HDFS → HDFS/Kafka) - runs periodically
# - Kafka runs independently

set -e

# Change to project directory
cd "$(dirname "$0")/.."

echo "=========================================="
echo "Space Debris Risk Prediction System"
echo "=========================================="
echo ""
echo "Architecture:"
echo "  • Airflow      → Data Ingestion ONLY (API → Kafka)"
echo "  • Spark Job 1  → SGP4 Streaming (Kafka TLE → HDFS vectors)"
echo "  • Spark Job 2  → Collision Prediction (HDFS → HDFS/Kafka alerts)"
echo "  • Kafka        → Independent Message Broker"
echo "=========================================="
echo ""

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo "❌ Error: docker is not installed"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Error: Docker is not running"
    exit 1
fi

# ==========================================
# STEP 1: Start Infrastructure Services
# ==========================================
echo "📦 Step 1: Starting Infrastructure (Zookeeper, Kafka, HDFS)..."
docker compose up -d zookeeper kafka namenode datanode

echo "   Waiting for infrastructure (30s)..."
sleep 30

# ==========================================
# STEP 2: Start Spark Cluster
# ==========================================
echo "📦 Step 2: Starting Spark Cluster..."
docker compose up -d spark-master spark-worker-1 spark-worker-2

echo "   Waiting for Spark (15s)..."
sleep 15

# ==========================================
# STEP 3: Create Kafka Topics
# ==========================================
echo "📦 Step 3: Creating Kafka topics..."
docker exec kafka kafka-topics --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic space_debris_tle 2>/dev/null || true

docker exec kafka kafka-topics --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic space_debris_collisions 2>/dev/null || true

echo "   ✓ Kafka topics ready"

# ==========================================
# STEP 4: Create HDFS Directories
# ==========================================
echo "📦 Step 4: Setting up HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /space-debris/sgp4_vectors 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris/collision_predictions 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /space-debris/tle_raw 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /tmp/spark-checkpoint-sgp4 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /tmp/checkpoint-collision 2>/dev/null || true
docker exec namenode hdfs dfs -chmod -R 777 /space-debris 2>/dev/null || true
docker exec namenode hdfs dfs -chmod -R 777 /tmp 2>/dev/null || true
echo "   ✓ HDFS directories ready"

# ==========================================
# STEP 5: Start TLE API
# ==========================================
echo "📦 Step 5: Starting TLE API..."
docker compose up -d tle-api

echo "   Waiting for API (30s)..."
sleep 30

# ==========================================
# STEP 6: Start Airflow (for ingestion ONLY)
# ==========================================
echo "📦 Step 6: Starting Airflow (Ingestion Only)..."
docker compose up -d postgres airflow-init
sleep 20
docker compose up -d airflow-webserver airflow-scheduler

echo "   Waiting for Airflow (30s)..."
sleep 30

# ==========================================
# STEP 6b: Auto-enable Airflow DAG
# ==========================================
echo "📦 Step 6b: Enabling Airflow ingestion DAG..."
for i in {1..10}; do
    if docker exec airflow-scheduler airflow dags list 2>/dev/null | grep -q "tle_data_ingestion"; then
        echo "   DAG found, enabling..."
        docker exec airflow-scheduler airflow dags unpause tle_data_ingestion 2>/dev/null || true
        echo "   ✓ DAG 'tle_data_ingestion' enabled"
        echo "   Triggering first DAG run..."
        docker exec airflow-scheduler airflow dags trigger tle_data_ingestion 2>/dev/null || true
        echo "   ✓ First DAG run triggered"
        break
    else
        echo "   Waiting for DAG to be discovered ($i/10)..."
        sleep 5
    fi
done

# ==========================================
# STEP 7: Start Spark Job 1 - SGP4 Streaming
# ==========================================
echo "📦 Step 7: Starting Spark Job 1 - SGP4 Streaming..."
docker compose up -d spark-sgp4-streaming

# ==========================================
# STEP 8: Start Spark Job 2 - Collision Prediction
# ==========================================
echo "📦 Step 8: Starting Spark Job 2 - Collision Prediction..."
docker compose up -d spark-collision-prediction

# ==========================================
# STEP 9: Start Dashboard
# ==========================================
echo "📦 Step 9: Starting Dashboard..."
docker compose up -d dashboard-api dashboard-web kafka-ui

echo "   Waiting for services (20s)..."
sleep 20

# ==========================================
# VERIFICATION
# ==========================================
echo ""
echo "=========================================="
echo "✅ Services Started!"
echo "=========================================="
echo ""
echo "Service Status:"
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || docker compose ps
echo ""
echo "=========================================="
echo "📍 ACCESS POINTS"
echo "=========================================="
echo ""
echo "🔵 AIRFLOW (Ingestion Only)"
echo "   URL:          http://localhost:8088"
echo "   Login:        admin / admin"
echo "   DAG to use:   tle_data_ingestion"
echo ""
echo "🟠 KAFKA"
echo "   Broker:       localhost:9092"
echo "   UI:           http://localhost:8090"
echo "   Topics:       space_debris_tle, space_debris_collisions"
echo ""
echo "🟢 SPARK JOBS (Running Standalone)"
echo "   Master UI:    http://localhost:8080"
echo "   Job 1:        SGP4 Streaming (continuous)"
echo "   Job 2:        Collision Prediction (every 10 seconds - DEMO)"
echo ""
echo "🟡 HDFS"
echo "   NameNode:     http://localhost:9870"
echo "   Vectors:      /space-debris/sgp4_vectors"
echo "   Collisions:   /space-debris/collision_predictions"
echo ""
echo "🟣 DASHBOARD"
echo "   API:          http://localhost:5001"
echo "   Web:          http://localhost:8082"
echo ""
echo "=========================================="
echo "📋 DATA FLOW"
echo "=========================================="
echo ""
echo "  TLE API  ─────►  AIRFLOW  ─────►  KAFKA"
echo "  (source)       (ingestion)      (broker)"
echo "                                     │"
echo "                                     ▼"
echo "                              SPARK JOB 1"
echo "                            (SGP4 Streaming)"
echo "                                     │"
echo "                                     ▼"
echo "                                   HDFS"
echo "                             (sgp4_vectors)"
echo "                                     │"
echo "                                     ▼"
echo "                              SPARK JOB 2"
echo "                          (Collision Prediction)"
echo "                                     │"
echo "                              ┌──────┴──────┐"
echo "                              ▼             ▼"
echo "                            HDFS         KAFKA"
echo "                        (collisions)   (alerts)"
echo ""
echo "=========================================="
echo "📋 COMMANDS"
echo "=========================================="
echo ""
echo "View Spark Job 1 logs (SGP4 Streaming):"
echo "  docker logs -f spark-sgp4-streaming"
echo ""
echo "View Spark Job 2 logs (Collision Prediction):"
echo "  docker logs -f spark-collision-prediction"
echo ""
echo "Check HDFS data:"
echo "  docker exec namenode hdfs dfs -ls /space-debris/"
echo ""
echo "Run collision prediction manually:"
echo "  docker exec spark-collision-prediction /opt/spark/bin/spark-submit \\"
echo "    --master spark://spark-master:7077 \\"
echo "    /opt/spark-apps/processing/spark_collision_prediction.py"
echo ""
