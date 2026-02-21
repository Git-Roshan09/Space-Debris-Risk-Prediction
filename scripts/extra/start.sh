#!/bin/bash
# Quick start script for Space Debris Risk Prediction
# Starts all services (assumes images are already built)

set -e

# Change to project directory
cd "$(dirname "$0")/.."

echo "=========================================="
echo "Starting Space Debris System"
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

# Check if images are built
if ! docker compose images 2>/dev/null | grep -q "spark-sgp4"; then
    echo "⚠️  Warning: Images may not be built yet"
    echo "   Run './scripts/build.sh' first if this is your first time"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "🚀 Starting all services..."
docker compose up -d

echo ""
echo "⏳ Waiting for services to initialize (60s)..."
sleep 60

# Enable Airflow DAG
echo ""
echo "📦 Enabling Airflow ingestion DAG..."
for i in {1..10}; do
    if docker exec airflow-scheduler airflow dags list 2>/dev/null | grep -q "tle_data_ingestion"; then
        docker exec airflow-scheduler airflow dags unpause tle_data_ingestion 2>/dev/null || true
        docker exec airflow-scheduler airflow dags trigger tle_data_ingestion 2>/dev/null || true
        echo "   ✓ DAG enabled and triggered"
        break
    else
        echo "   Waiting for DAG ($i/10)..."
        sleep 5
    fi
done

echo ""
echo "=========================================="
echo "✅ System Started!"
echo "=========================================="
echo ""

# Show service status
echo "Service Status:"
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || docker compose ps

echo ""
echo "=========================================="
echo "📍 ACCESS POINTS"
echo "=========================================="
echo ""
echo "  • Airflow:    http://localhost:8088  (admin/admin)"
echo "  • Spark UI:   http://localhost:8080"
echo "  • HDFS UI:    http://localhost:9870"
echo "  • Kafka UI:   http://localhost:8090"
echo "  • Dashboard:  http://localhost:8082"
echo ""
echo "=========================================="
echo "📋 USEFUL COMMANDS"
echo "=========================================="
echo ""
echo "  Status:       ./scripts/status.sh"
echo "  Stop:         ./scripts/stop.sh"
echo "  Restart:      ./scripts/restart.sh [service]"
echo "  View logs:    docker compose logs -f [service]"
echo ""
