#!/bin/bash
# Test script for Spark SGP4 streaming pipeline

echo "=========================================="
echo "Testing Spark SGP4 Streaming Pipeline"
echo "=========================================="
echo ""

# Check if Kafka has data
echo "1. Checking Kafka topic for TLE data..."
KAFKA_CHECK=$(timeout 5 docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic space_debris_tle --from-beginning --max-messages 1 2>/dev/null | head -1)
if [ ! -z "$KAFKA_CHECK" ]; then
    echo "✓ Kafka topic 'space_debris_tle' has data"
    echo "  Sample: $(echo $KAFKA_CHECK | cut -c1-80)..."
else
    echo "✗ No data found in Kafka topic"
    exit 1
fi
echo ""

# Check HDFS
echo "2. Checking HDFS connectivity..."
if timeout 2 bash -c 'cat < /dev/null > /dev/tcp/localhost/9870' 2>/dev/null; then
    echo "✓ HDFS NameNode is accessible on port 9870"
else
    echo "✗ HDFS NameNode is not accessible"
    exit 1
fi
echo ""

# Check Spark Master
echo "3. Checking Spark Master..."
if timeout 2 bash -c 'cat < /dev/null > /dev/tcp/localhost/8080' 2>/dev/null; then
    echo "✓ Spark Master is accessible on port 8080"
else
    echo "✗ Spark Master is not accessible"
    exit 1
fi
echo ""

# Verify sgp4 is installed
echo "4. Verifying sgp4 library on Spark..."
docker exec spark-master python3 -c "import sgp4; print('✓ sgp4 version:', sgp4.__version__)"
echo ""

# Check if Spark app file exists
echo "5. Checking Spark application file..."
if docker exec spark-master test -f /opt/spark-apps/processing/spark_sgp4_to_hdfs.py; then
    echo "✓ Spark app found: /opt/spark-apps/processing/spark_sgp4_to_hdfs.py"
else
    echo "✗ Spark app not found"
    exit 1
fi
echo ""

echo "=========================================="
echo "All prerequisites passed!"
echo "=========================================="
echo ""
echo "To manually test Spark streaming job, run:"
echo ""
echo "docker exec spark-master /opt/spark/bin/spark-submit \\"
echo "  --master spark://spark-master:7077 \\"
echo "  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "  --conf spark.executor.memory=1g \\"
echo "  /opt/spark-apps/processing/spark_sgp4_to_hdfs.py \\"
echo "  --kafka kafka:9093"
echo ""
echo "Or trigger from Airflow UI: http://localhost:8088"
echo "  DAG: spark_sgp4_streaming"
echo ""
