#!/bin/bash
# Stop all Docker services

echo "🛑 Stopping Space Debris Risk Prediction - Docker Services"
echo "==========================================================="

docker-compose down

echo ""
echo "✅ All services stopped"
echo ""
echo "💡 To remove volumes (delete data): docker-compose down -v"
