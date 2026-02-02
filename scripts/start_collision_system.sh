#!/bin/bash
# Quick Start Script for Collision Prediction System

echo "=========================================="
echo "Collision Prediction System - Quick Start"
echo "=========================================="

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "✓ Environment variables loaded"
else
    echo "⚠ Warning: .env file not found"
fi

# Step 1: Start core services
echo ""
echo "Step 1: Starting core services (HDFS, Kafka, Spark)..."
docker-compose up -d zookeeper kafka namenode datanode spark-master spark-worker

echo "Waiting for services to be healthy (60 seconds)..."
sleep 60

# Step 2: Verify HDFS
echo ""
echo "Step 2: Checking HDFS..."
docker exec spark-master hdfs dfs -ls /space-debris/ 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✓ HDFS is accessible"
else
    echo "⚠ HDFS directories not found, creating..."
    docker exec spark-master hdfs dfs -mkdir -p /space-debris/sgp4_vectors
    docker exec spark-master hdfs dfs -mkdir -p /space-debris/collision_predictions
fi

# Step 3: Start Airflow
echo ""
echo "Step 3: Starting Airflow..."
docker-compose up -d postgres airflow-init
sleep 30
docker-compose up -d airflow-webserver airflow-scheduler

# Step 4: Start TLE API
echo ""
echo "Step 4: Starting TLE streaming API..."
docker-compose up -d tle-api

# Step 5: Start Dashboard services
echo ""
echo "Step 5: Starting Dashboard API and Web UI..."
docker-compose up -d dashboard-api dashboard-web

echo "Waiting for dashboard services (30 seconds)..."
sleep 30

# Step 6: Verify services
echo ""
echo "Step 6: Verifying services..."
echo ""

echo "Checking Kafka..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep space_debris && echo "✓ Kafka is running" || echo "⚠ Kafka check failed"

echo "Checking HDFS NameNode..."
curl -s http://localhost:9870 > /dev/null && echo "✓ HDFS NameNode is running" || echo "⚠ HDFS NameNode not accessible"

echo "Checking Spark Master..."
curl -s http://localhost:8081 > /dev/null && echo "✓ Spark Master is running" || echo "⚠ Spark Master not accessible"

echo "Checking Airflow..."
curl -s http://localhost:8091 > /dev/null && echo "✓ Airflow is running" || echo "⚠ Airflow not accessible"

echo "Checking Dashboard API..."
curl -s http://localhost:5001/api/health > /dev/null && echo "✓ Dashboard API is running" || echo "⚠ Dashboard API not accessible"

echo "Checking Dashboard Web..."
curl -s http://localhost:8080 > /dev/null && echo "✓ Dashboard Web is running" || echo "⚠ Dashboard Web not accessible"

# Step 7: Instructions
echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "Access Points:"
echo "  • Dashboard:        http://localhost:8080"
echo "  • Dashboard API:    http://localhost:5001"
echo "  • Airflow:          http://localhost:8091 (airflow/airflow)"
echo "  • Spark Master:     http://localhost:8081"
echo "  • HDFS NameNode:    http://localhost:9870"
echo "  • Kafka UI:         http://localhost:8090"
echo ""
echo "Next Steps:"
echo "  1. Trigger TLE data ingestion DAG in Airflow:"
echo "     Visit http://localhost:8091 → Enable 'tle_api_to_kafka_streaming'"
echo ""
echo "  2. Trigger SGP4 processing DAG:"
echo "     Enable 'spark_sgp4_streaming' in Airflow"
echo ""
echo "  3. Trigger Collision Prediction:"
echo "     Enable 'collision_prediction_pipeline' in Airflow"
echo ""
echo "  4. View Results:"
echo "     Open http://localhost:8080 to see the dashboard"
echo ""
echo "Logs:"
echo "  docker-compose logs -f dashboard-api"
echo "  docker-compose logs -f dashboard-web"
echo ""
echo "=========================================="
