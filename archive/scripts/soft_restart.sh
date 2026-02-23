#!/bin/bash
# Soft restart script - restarts containers without recreating them
# This preserves volumes and avoids re-downloading dependencies

set -e

echo "🔄 Starting soft restart of containers..."
echo ""

# Function to restart a service
restart_service() {
    local service=$1
    echo "Restarting $service..."
    docker-compose restart "$service"
}

# Check if specific services were provided
if [ $# -eq 0 ]; then
    echo "No specific services provided. Restarting all services..."
    docker-compose restart
else
    echo "Restarting specified services: $@"
    for service in "$@"; do
        restart_service "$service"
    done
fi

echo ""
echo "✓ Soft restart complete!"
echo ""
echo "📊 Container status:"
docker-compose ps
