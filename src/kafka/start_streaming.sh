#!/bin/bash
# Quick start script for TLE API to Kafka streaming pipeline

set -e

echo "=================================================="
echo "Space Debris TLE Streaming Pipeline - Quick Start"
echo "=================================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
API_URL="${API_URL:-http://localhost:5000}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
ACCELERATION="${ACCELERATION:-100}"
LIMIT="${LIMIT:-}"

# Function to check if service is running
check_service() {
    local service=$1
    local port=$2
    if nc -z localhost $port 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $service is running on port $port"
        return 0
    else
        echo -e "${RED}✗${NC} $service is NOT running on port $port"
        return 1
    fi
}

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check if Kafka is running
if check_service "Kafka" 9092; then
    KAFKA_OK=1
else
    KAFKA_OK=0
    echo -e "${YELLOW}  → Start Kafka with: docker-compose up -d${NC}"
fi

# Check if TLE API is running
if check_service "TLE Streaming API" 5000; then
    API_OK=1
else
    API_OK=0
    echo -e "${YELLOW}  → Start API with: cd ../demo/api && python tle_stream_api.py${NC}"
fi

echo ""

if [ $KAFKA_OK -eq 0 ] || [ $API_OK -eq 0 ]; then
    echo -e "${RED}Error: Required services are not running${NC}"
    echo ""
    echo "Please start the required services and try again."
    exit 1
fi

# Get API stats
echo "=================================================="
echo "Fetching TLE dataset statistics..."
echo "=================================================="
curl -s "$API_URL/stats" | python3 -m json.tool || {
    echo -e "${RED}Failed to get API stats${NC}"
    exit 1
}
echo ""

# Ask user for confirmation
echo "=================================================="
echo "Ready to start streaming"
echo "=================================================="
echo "Configuration:"
echo "  API URL:       $API_URL"
echo "  Kafka Brokers: $KAFKA_BROKERS"
echo "  Acceleration:  ${ACCELERATION}x"
if [ -n "$LIMIT" ]; then
    echo "  Record Limit:  $LIMIT"
else
    echo "  Record Limit:  Unlimited"
fi
echo ""
read -p "Press Enter to start streaming (Ctrl+C to cancel)..."
echo ""

# Build command
CMD="python3 tle_api_to_kafka_producer.py \
    --api-url $API_URL \
    --kafka-brokers $KAFKA_BROKERS \
    --acceleration $ACCELERATION"

if [ -n "$LIMIT" ]; then
    CMD="$CMD --limit $LIMIT"
fi

# Run the producer
echo "=================================================="
echo "Starting TLE API to Kafka Producer"
echo "=================================================="
echo ""
echo "Command: $CMD"
echo ""

$CMD

echo ""
echo "=================================================="
echo "Streaming Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "  1. Start Spark consumer: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 space_debris_spark_consumer.py"
echo "  2. Monitor Kafka: kafka-console-consumer --bootstrap-server $KAFKA_BROKERS --topic space_debris_tle --from-beginning"
echo ""
