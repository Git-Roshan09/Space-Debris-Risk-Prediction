#!/bin/bash
# Test script for SGP4 to HDFS streaming pipeline

echo "========================================"
echo "SGP4 → HDFS Pipeline Setup & Test"
echo "========================================"
echo ""

# Check if API is running
echo "1. Checking TLE API..."
if curl -s http://localhost:5000/stats > /dev/null; then
    echo "   ✓ TLE API is running"
else
    echo "   ✗ TLE API not running. Start with:"
    echo "     uv run src/demo/api/tle_stream_api.py"
    exit 1
fi
echo ""

# Check Kafka
echo "2. Checking Kafka..."
docker exec broker kafka-topics --list --bootstrap-server broker:29092 2>/dev/null | grep -q "space_debris_tle"
if [ $? -eq 0 ]; then
    echo "   ✓ Kafka topic exists"
else
    echo "   ✗ Kafka topic not found"
    exit 1
fi
echo ""

# Check Spark
echo "3. Checking Spark..."
if docker exec spark-master ls /opt/bitnami/spark/work-dir/src/kafka/spark_sgp4_to_hdfs.py > /dev/null 2>&1; then
    echo "   ✓ Spark code mounted correctly"
else
    echo "   ✗ Spark code not mounted. Check docker-compose volumes"
    exit 1
fi
echo ""

# Install sgp4 in Spark
echo "4. Installing sgp4 in Spark..."
docker exec spark-master pip install sgp4 -q
echo "   ✓ sgp4 installed"
echo ""

# Stream some test data
echo "5. Streaming 20 TLE records to Kafka..."
cd /home/bharath/Documents/BigData/project/data/Space-Debris-Risk-Prediction
uv run src/kafka/tle_api_to_kafka_producer.py --limit 20 --acceleration 10000 2>&1 | grep "Total records sent"
echo ""

# Show example to run Spark job
echo "========================================"
echo "✓ Setup Complete!"
echo "========================================"
echo ""
echo "Next: Start Spark Streaming Job"
echo ""
echo "Option 1 - Run in Docker container:"
echo "-----------------------------------"
echo "docker exec -it spark-master spark-submit \\"
echo "  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "  --driver-memory 1g \\"
echo "  /opt/bitnami/spark/work-dir/src/kafka/spark_sgp4_to_hdfs.py \\"
echo "  --kafka broker:29092 \\"
echo "  --hdfs-path /tmp/sgp4_vectors"
echo ""
echo "Option 2 - Run locally with uv:"
echo "-----------------------------------"
echo "uv run src/kafka/spark_sgp4_to_hdfs.py \\"
echo "  --kafka localhost:9092 \\"
echo "  --hdfs-path /tmp/sgp4_vectors"
echo ""
echo "Monitor: http://localhost:9090 (Spark UI)"
echo ""
