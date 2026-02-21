#!/bin/bash
# Build script for Space Debris Risk Prediction
# Builds all Docker images for the system

set -e

# Change to project directory
cd "$(dirname "$0")/.."

echo "=========================================="
echo "Building Space Debris System Images"
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

echo "🔨 Building Docker images..."
echo ""

# Build all services
docker compose build

echo ""
echo "=========================================="
echo "✅ Build Complete!"
echo "=========================================="
echo ""
echo "Images built:"
docker compose images
echo ""
echo "Next steps:"
echo "  • Start services:    ./scripts/start_separated.sh"
echo "  • Check status:      ./scripts/status.sh"
echo ""
