#!/bin/bash

# Space Debris Pipeline - Container Startup Script with HDFS
set -e

echo "🚀 Starting Space Debris Pipeline with HDFS..."
echo "==============================================="

# Start all containers
echo "📦 Starting Docker containers..."
docker compose up -d

echo ""
echo "⏳ Waiting for containers to initialize..."
sleep 15

# Check container status
echo ""
echo "📊 Container Status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(NAMES|spark|kafka|broker|zookeeper|airflow|cassandra|postgres|control-center|schema-registry|namenode|datanode)"

# Wait for HDFS to be ready
echo ""
echo "⏳ Waiting for HDFS to be ready..."
sleep 5

# Create HDFS directories
echo ""
echo "📁 Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /space-debris/sgp4_vectors 2>/dev/null && echo "  ✓ /space-debris/sgp4_vectors" || echo "  ⚠ Directory may already exist"

# Install Python dependencies in Spark containers
echo ""
echo "📚 Installing Python dependencies in Spark containers..."

echo "  ↳ Installing sgp4 in spark-master..."
docker exec -u root spark-master pip install -q sgp4 2>/dev/null || echo "    ⚠ Failed to install in spark-master"

echo "  ↳ Installing sgp4 in spark-worker..."
docker exec -u root spark-worker pip install -q sgp4 2>/dev/null || echo "    ⚠ Failed to install in spark-worker"

echo ""
echo "✅ All containers started successfully!"
echo ""
echo "🔗 Access Points:"
echo "  • Spark Master UI:      http://localhost:9090"
echo "  • HDFS NameNode UI:     http://localhost:9870"
echo "  • Kafka Control Center: http://localhost:9021"
echo "  • Airflow Webserver:    http://localhost:8080"
echo "  • Jupyter Notebook:     http://localhost:8888"
echo ""
echo "📝 To run the Spark SGP4 pipeline (stores to HDFS):"
echo "  docker exec -u root spark-master /opt/spark/bin/spark-submit \\"
echo "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "    /opt/spark/work-dir/src/kafka/spark_sgp4_to_hdfs.py \\"
echo "    --kafka broker:29092"
echo ""
echo "🗂️  HDFS Management:"
echo "  ./hdfs-cli.sh ls /space-debris/sgp4_vectors/    # List files"
echo "  ./hdfs-cli.sh browse                            # Open Web UI"
echo "  ./hdfs-cli.sh sgp4                              # Check SGP4 data"
echo "  ./hdfs-cli.sh report                            # HDFS health"
echo ""
