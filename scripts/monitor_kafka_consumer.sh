#!/bin/bash
# Monitor Kafka Consumer - Shows visible consumer group in Kafka UI
# This creates a dummy consumer group that appears in Kafka UI for monitoring

echo "Starting monitoring consumer for Kafka UI visibility..."

docker exec -d kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic space_debris_tle \
    --group spark-monitoring-group \
    --from-beginning \
    > /dev/null 2>&1

echo "✓ Monitoring consumer group 'spark-monitoring-group' started"
echo "  This will now be visible in Kafka UI at http://localhost:8090"
echo ""
echo "To stop monitoring consumer:"
echo "  docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group spark-monitoring-group --delete"