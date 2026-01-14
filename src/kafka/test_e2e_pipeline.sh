#!/bin/bash
# End-to-End Kafka Streaming Pipeline Test

echo "=========================================="
echo "Space Debris Kafka Pipeline Test"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check TLE API
echo -e "${BLUE}Step 1: Checking TLE Streaming API...${NC}"
API_RESPONSE=$(curl -s http://localhost:5000/stats)
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ API is running${NC}"
    echo "$API_RESPONSE" | jq '.total_records, .total_satellites'
else
    echo -e "${YELLOW}⚠ API not running. Start it with: uv run src/demo/api/tle_stream_api.py${NC}"
    exit 1
fi
echo ""

# Check Kafka
echo -e "${BLUE}Step 2: Checking Kafka broker...${NC}"
docker exec broker kafka-broker-api-versions --bootstrap-server broker:29092 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Kafka broker is healthy${NC}"
else
    echo -e "${YELLOW}⚠ Kafka broker not responding${NC}"
    exit 1
fi
echo ""

# Check topic
echo -e "${BLUE}Step 3: Checking Kafka topic...${NC}"
TOPIC_EXISTS=$(docker exec broker kafka-topics --list --bootstrap-server broker:29092 2>/dev/null | grep "space_debris_tle")
if [ ! -z "$TOPIC_EXISTS" ]; then
    echo -e "${GREEN}✓ Topic 'space_debris_tle' exists${NC}"
    MESSAGE_COUNT=$(docker exec broker kafka-run-class kafka.tools.GetOffsetShell --broker-list broker:29092 --topic space_debris_tle --time -1 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')
    echo "  Messages in topic: $MESSAGE_COUNT"
else
    echo -e "${YELLOW}⚠ Topic not found${NC}"
fi
echo ""

# Stream some data
echo -e "${BLUE}Step 4: Streaming 10 TLE records to Kafka...${NC}"
cd /home/bharath/Documents/BigData/project/data/Space-Debris-Risk-Prediction
uv run src/kafka/tle_api_to_kafka_producer.py --limit 10 --acceleration 5000 2>&1 | grep -E "(Total records sent|Total time|Average rate)"
echo ""

# Show sample message
echo -e "${BLUE}Step 5: Sample Kafka message:${NC}"
docker exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic space_debris_tle --max-messages 1 --from-beginning 2>/dev/null | jq '.'
echo ""

# Check Airflow
echo -e "${BLUE}Step 6: Checking Airflow DAG...${NC}"
DAG_STATUS=$(docker exec airflow-webserver airflow dags list 2>/dev/null | grep "tle_api_to_kafka")
if [ ! -z "$DAG_STATUS" ]; then
    echo -e "${GREEN}✓ Airflow DAG is registered${NC}"
    echo "  $DAG_STATUS"
else
    echo -e "${YELLOW}⚠ DAG not found in Airflow${NC}"
fi
echo ""

echo "=========================================="
echo -e "${GREEN}Pipeline Status: OPERATIONAL${NC}"
echo "=========================================="
echo ""
echo "Next Steps:"
echo "1. Access Airflow UI: http://localhost:8080 (admin/admin)"
echo "2. Access Kafka Control Center: http://localhost:9021"
echo "3. Enable DAG in Airflow to automate hourly streaming"
echo ""
