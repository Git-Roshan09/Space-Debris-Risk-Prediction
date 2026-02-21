#!/bin/bash
# Restart script for Space Debris Risk Prediction
# Restarts services without deleting containers or data

set -e

# Change to project directory
cd "$(dirname "$0")/.."

echo "=========================================="
echo "Restarting Space Debris System"
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

# Parse arguments
SERVICE=""
if [ $# -gt 0 ]; then
    SERVICE="$1"
fi

if [ -n "$SERVICE" ]; then
    echo "🔄 Restarting service: $SERVICE"
    docker compose restart "$SERVICE"
else
    echo "🔄 Restarting all services..."
    docker compose restart
fi

echo ""
echo "=========================================="
echo "✅ Restart Complete!"
echo "=========================================="
echo ""

# Show service status
echo "Service Status:"
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || docker compose ps

echo ""
echo "Access Points:"
echo "  • Airflow:    http://localhost:8088"
echo "  • Spark UI:   http://localhost:8080"
echo "  • HDFS UI:    http://localhost:9870"
echo "  • Kafka UI:   http://localhost:8090"
echo "  • Dashboard:  http://localhost:8082"
echo ""
