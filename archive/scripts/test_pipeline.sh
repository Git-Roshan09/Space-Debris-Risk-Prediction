#!/bin/bash
# Test the complete pipeline

echo "🧪 Testing Space Debris Pipeline"
echo "================================="

echo ""
echo "1️⃣  Checking Flask API..."
if ! curl -s http://localhost:5000/health > /dev/null 2>&1; then
    echo "❌ Flask API is not running. Start it with: python3 src/apis/api.py"
    exit 1
fi

API_HEALTH=$(curl -s http://localhost:5000/health)
echo "✅ Flask API is healthy"
echo "$API_HEALTH" | jq '.'

echo ""
echo "2️⃣  Checking Flask API stats..."
curl -s http://localhost:5000/stats | jq '.'

echo ""
echo "3️⃣  Testing stream endpoint (first 5 records)..."
curl -s "http://localhost:5000/stream?limit=5" | head -n 5 | jq '.'

echo ""
echo "4️⃣  Checking Kafka..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo "❌ Kafka is not running. Start it with: ./scripts/start.sh"
    exit 1
fi
echo "✅ Kafka is running"

echo ""
echo "5️⃣  Listing Kafka topics..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "✅ Pipeline test complete!"
echo ""
echo "💡 To run full stream to Kafka:"
echo "   python3 pipelines/ingestion/kafka_producer.py --limit 100"
