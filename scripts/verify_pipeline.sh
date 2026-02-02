#!/bin/bash
# Verification script for the Space Debris API and Pipeline

echo "=================================="
echo "Space Debris Pipeline Verification"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if container is running
check_container() {
    local container=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        echo -e "${GREEN}✓${NC} $container is running"
        return 0
    else
        echo -e "${RED}✗${NC} $container is NOT running"
        return 1
    fi
}

# Check container health
check_health() {
    local container=$1
    local health=$(docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null)
    if [ "$health" = "healthy" ]; then
        echo -e "  ${GREEN}✓${NC} Health: $health"
    elif [ "$health" = "unhealthy" ]; then
        echo -e "  ${RED}✗${NC} Health: $health"
    elif [ -z "$health" ]; then
        echo -e "  ${YELLOW}○${NC} Health: no healthcheck configured"
    else
        echo -e "  ${YELLOW}○${NC} Health: $health"
    fi
}

echo "1. Checking Core Services:"
echo "-------------------------"
check_container "tle-api" && check_health "tle-api"
check_container "kafka" && check_health "kafka"
check_container "zookeeper" && check_health "zookeeper"
check_container "airflow-scheduler"
check_container "airflow-webserver" && check_health "airflow-webserver"
echo ""

echo "2. Testing API Endpoints:"
echo "------------------------"
# Test stats endpoint
echo -n "Testing /stats endpoint... "
if curl -sf http://localhost:5000/stats > /dev/null; then
    echo -e "${GREEN}✓${NC}"
    records=$(curl -s http://localhost:5000/stats | python3 -c "import sys, json; print(json.load(sys.stdin)['total_records'])")
    echo "  Total TLE records: $records"
else
    echo -e "${RED}✗${NC}"
fi

# Test streaming endpoint
echo -n "Testing /stream endpoint... "
if timeout 5 curl -sf "http://localhost:5000/stream?limit=1&acceleration=1000" > /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
fi
echo ""

echo "3. Testing API from Airflow:"
echo "---------------------------"
echo -n "Airflow can reach API... "
if docker exec airflow-scheduler curl -sf http://tle-api:5000/stats > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
fi
echo ""

echo "4. Checking Airflow DAGs:"
echo "------------------------"
dags=$(docker exec airflow-scheduler airflow dags list 2>/dev/null | grep tle_api_to_kafka_streaming)
if [ ! -z "$dags" ]; then
    echo -e "${GREEN}✓${NC} TLE DAG is registered"
    echo "$dags"
else
    echo -e "${RED}✗${NC} TLE DAG not found"
fi
echo ""

echo "5. Port Accessibility:"
echo "---------------------"
ports=("5000:API" "8088:Airflow-UI" "9092:Kafka" "8090:Kafka-UI" "9870:HDFS-NameNode" "8080:Spark-Master")
for port_info in "${ports[@]}"; do
    IFS=':' read -r port name <<< "$port_info"
    echo -n "Port $port ($name)... "
    if timeout 2 bash -c "echo > /dev/tcp/localhost/$port" 2>/dev/null; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗${NC}"
    fi
done
echo ""

echo "6. Quick Sample Test:"
echo "--------------------"
echo "Fetching sample TLE record from API..."
sample=$(curl -s "http://localhost:5000/stream?limit=1&acceleration=1000" | head -1)
if [ ! -z "$sample" ]; then
    echo -e "${GREEN}✓${NC} Sample record received:"
    echo "$sample" | python3 -m json.tool | head -10
else
    echo -e "${RED}✗${NC} Failed to get sample record"
fi
echo ""

echo "=================================="
echo "Verification Complete"
echo "=================================="
echo ""
echo "Access Points:"
echo "- API: http://localhost:5000"
echo "- Airflow UI: http://localhost:8088 (admin/admin)"
echo "- Kafka UI: http://localhost:8090"
echo "- Spark Master: http://localhost:8080"
echo "- HDFS NameNode: http://localhost:9870"
