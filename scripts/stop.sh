#!/bin/bash
# Stop all Docker services

echo "🛑 Stopping Space Debris Risk Prediction - Docker Services"
echo "==========================================================="

# Stop standalone Spark services if running
if docker ps --format '{{.Names}}' | grep -q "spark-sgp4-streaming\|spark-collision-prediction"; then
    echo "Stopping standalone Spark services..."
    docker-compose -f docker-compose.yml -f docker-compose.standalone.yml down
else
    docker-compose down
fi

echo ""
echo "✅ All services stopped"
echo ""
echo "💡 To remove volumes (delete data): docker-compose down -v"
