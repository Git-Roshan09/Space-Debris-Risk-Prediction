#!/bin/bash
# Start all Docker services for Space Debris Risk Prediction

set -e

echo "🚀 Starting Space Debris Risk Prediction - Big Data Stack"
echo "============================================================"

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Error: docker-compose is not installed"
    echo "   Please install docker-compose first"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Error: Docker is not running"
    echo "   Please start Docker first"
    exit 1
fi

echo ""
echo "📦 Starting Docker containers..."
echo "   This may take a few minutes on first run..."
echo ""

# Start services
docker-compose up -d

echo ""
echo "⏳ Waiting for services to initialize..."
echo "   - Zookeeper and Kafka need ~20 seconds"
echo "   - Airflow needs ~30 seconds for database migration"
echo "   - HDFS and Spark need ~15 seconds"
echo ""

# Wait for core services
sleep 25

echo "✅ Docker services started!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📍 SERVICE ENDPOINTS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🔵 AIRFLOW"
echo "   Web UI:        http://localhost:8088"
echo "   Credentials:   admin / admin"
echo ""
echo "🟠 KAFKA"
echo "   Broker:        localhost:9092 (external)"
echo "   Broker:        kafka:9093 (internal)"
echo "   UI Dashboard:  http://localhost:8090"
echo ""
echo "🟢 APACHE SPARK"
echo "   Master UI:     http://localhost:8080"
echo "   Worker UI:     http://localhost:8081"
echo "   Master URL:    spark://spark-master:7077"
echo "   App UI:        http://localhost:4040 (when job running)"
echo ""
echo "🟡 HDFS"
echo "   NameNode UI:   http://localhost:9870"
echo "   DataNode UI:   http://localhost:9864"
echo "   HDFS URI:      hdfs://namenode:9000"
echo ""
echo "🟣 SUPPORTING SERVICES"
echo "   Zookeeper:     localhost:2181"
echo "   PostgreSQL:    localhost:5432 (airflow/airflow)"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Service Status:"
docker-compose ps
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💡 NEXT STEPS:"
echo "   1. Check service status: ./scripts/status.sh"
echo "   2. View logs: docker-compose logs -f [service-name]"
echo "   3. Stop services: ./scripts/stop.sh"
echo ""

# Start Spark streaming job in background
echo "🔥 Starting Spark Streaming Job..."
nohup ./scripts/submit_spark_job.sh > logs/spark_streaming.log 2>&1 &
SPARK_PID=$!
echo "   Job started with PID: $SPARK_PID"
echo "   Logs: tail -f logs/spark_streaming.log"
echo ""

echo "✨ All services are ready!"
