#!/bin/bash
# Test script for Collision Prediction System

echo "=========================================="
echo "Testing Collision Prediction System"
echo "=========================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
PASSED=0
FAILED=0

# Function to test endpoint
test_endpoint() {
    local name=$1
    local url=$2
    local expected_code=${3:-200}
    
    echo -n "Testing $name... "
    response_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
    
    if [ "$response_code" -eq "$expected_code" ]; then
        echo -e "${GREEN}✓ PASSED${NC} (HTTP $response_code)"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAILED${NC} (HTTP $response_code, expected $expected_code)"
        ((FAILED++))
        return 1
    fi
}

# Test service availability
test_service() {
    local name=$1
    local container=$2
    
    echo -n "Testing $name container... "
    if docker ps | grep -q "$container"; then
        echo -e "${GREEN}✓ RUNNING${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}✗ NOT RUNNING${NC}"
        ((FAILED++))
        return 1
    fi
}

echo "1. Testing Docker Containers"
echo "------------------------------"
test_service "Dashboard API" "dashboard-api"
test_service "Dashboard Web" "dashboard-web"
test_service "Spark Master" "spark-master"
test_service "HDFS NameNode" "namenode"
test_service "Kafka" "kafka"
echo ""

echo "2. Testing API Endpoints"
echo "------------------------------"
test_endpoint "Health Check" "http://localhost:5001/api/health"
test_endpoint "Configuration" "http://localhost:5001/api/config"
test_endpoint "Collision Stats" "http://localhost:5001/api/collisions/stats"
test_endpoint "High Risk Collisions" "http://localhost:5001/api/collisions/high-risk"
test_endpoint "Collision Timeline" "http://localhost:5001/api/collisions/timeline?days=7"
test_endpoint "Satellite Pairs" "http://localhost:5001/api/satellites/pairs"
echo ""

echo "3. Testing Dashboard Access"
echo "------------------------------"
test_endpoint "Dashboard Home" "http://localhost:8080"
test_endpoint "Dashboard JS" "http://localhost:8080/dashboard.js"
test_endpoint "Dashboard CSS" "http://localhost:8080/styles.css"
echo ""

echo "4. Testing HDFS Data"
echo "------------------------------"
echo -n "Testing HDFS collision predictions directory... "
if docker exec spark-master hdfs dfs -test -d /space-debris/collision_predictions 2>/dev/null; then
    echo -e "${GREEN}✓ EXISTS${NC}"
    ((PASSED++))
    
    # Check if data exists
    echo -n "Testing HDFS collision predictions data... "
    file_count=$(docker exec spark-master hdfs dfs -ls /space-debris/collision_predictions 2>/dev/null | grep -c "^d" || echo "0")
    if [ "$file_count" -gt "0" ]; then
        echo -e "${GREEN}✓ HAS DATA${NC} ($file_count partitions)"
        ((PASSED++))
    else
        echo -e "${YELLOW}⚠ NO DATA${NC} (run collision prediction job first)"
        ((FAILED++))
    fi
else
    echo -e "${RED}✗ NOT FOUND${NC}"
    ((FAILED++))
fi
echo ""

echo "5. Testing Airflow DAG"
echo "------------------------------"
echo -n "Testing Airflow scheduler... "
if docker ps | grep -q "airflow-scheduler"; then
    echo -e "${GREEN}✓ RUNNING${NC}"
    ((PASSED++))
    
    echo -n "Testing collision prediction DAG... "
    if docker exec airflow-scheduler airflow dags list 2>/dev/null | grep -q "collision_prediction_pipeline"; then
        echo -e "${GREEN}✓ REGISTERED${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗ NOT FOUND${NC}"
        ((FAILED++))
    fi
else
    echo -e "${RED}✗ NOT RUNNING${NC}"
    ((FAILED++))
fi
echo ""

echo "6. Testing Kafka Topics"
echo "------------------------------"
echo -n "Testing collision alerts topic... "
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "space_debris_collisions"; then
    echo -e "${GREEN}✓ EXISTS${NC}"
    ((PASSED++))
else
    echo -e "${YELLOW}⚠ NOT CREATED${NC} (will be created when first message is published)"
    ((FAILED++))
fi
echo ""

# Get API response for detailed check
echo "7. Testing API Data Quality"
echo "------------------------------"
echo -n "Testing collision stats response... "
stats_response=$(curl -s http://localhost:5001/api/collisions/stats)
if echo "$stats_response" | grep -q "total_collisions"; then
    total=$(echo "$stats_response" | grep -oP '"total_collisions":\s*\K[0-9]+' || echo "0")
    echo -e "${GREEN}✓ VALID${NC} (Total: $total collisions)"
    ((PASSED++))
else
    echo -e "${RED}✗ INVALID RESPONSE${NC}"
    ((FAILED++))
fi

echo -n "Testing config response... "
config_response=$(curl -s http://localhost:5001/api/config)
if echo "$config_response" | grep -q "prediction_days"; then
    pred_days=$(echo "$config_response" | grep -oP '"prediction_days":\s*\K[0-9]+' || echo "?")
    threshold=$(echo "$config_response" | grep -oP '"collision_threshold_km":\s*\K[0-9.]+' || echo "?")
    echo -e "${GREEN}✓ VALID${NC} (Prediction: $pred_days days, Threshold: $threshold km)"
    ((PASSED++))
else
    echo -e "${RED}✗ INVALID RESPONSE${NC}"
    ((FAILED++))
fi
echo ""

# Summary
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "Total Tests: $((PASSED + FAILED))"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ ALL TESTS PASSED!${NC}"
    echo ""
    echo "System is ready. Access dashboard at: http://localhost:8080"
    exit 0
else
    echo -e "${YELLOW}⚠ SOME TESTS FAILED${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Ensure all services are running: docker-compose ps"
    echo "  2. Check logs: docker-compose logs dashboard-api"
    echo "  3. Verify HDFS has data: docker exec spark-master hdfs dfs -ls /space-debris/"
    echo "  4. Run collision prediction job in Airflow"
    exit 1
fi
