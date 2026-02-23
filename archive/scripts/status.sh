#!/bin/bash
set -e

echo "=========================================="
echo "Space Debris Pipeline - Service Status"
echo "=========================================="
echo ""

# Check if using separated architecture
if docker ps --format '{{.Names}}' | grep -q "spark-sgp4-streaming"; then
    echo "Architecture: SEPARATED (Spark runs standalone)"
    echo ""
    docker-compose -f docker-compose.yml -f docker-compose.standalone.yml ps 2>/dev/null || docker-compose ps
else
    echo "Architecture: STANDARD"
    echo ""
    docker-compose ps
fi

echo ""
echo "=========================================="
echo "Web UIs & Access Points"
echo "=========================================="
echo ""
echo "🔵 AIRFLOW (Ingestion Only)"
echo "   URL:   http://localhost:8088"
echo "   Login: admin / admin"
echo ""
echo "🟠 KAFKA"
echo "   UI:    http://localhost:8090"
echo ""
echo "🟢 SPARK"
echo "   Master: http://localhost:8080"
echo "   Worker: http://localhost:8081"
echo ""
echo "🟡 HDFS"
echo "   NameNode: http://localhost:9870"
echo ""
echo "🟣 DASHBOARD"
echo "   API: http://localhost:5001"
echo "   Web: http://localhost:8082"
echo ""
echo "=========================================="
echo "Quick Commands"
echo "=========================================="
echo ""
echo "View Spark SGP4 Streaming logs:"
echo "  docker logs -f spark-sgp4-streaming"
echo ""
echo "View Collision Prediction logs:"
echo "  docker logs -f spark-collision-prediction"
echo ""
echo "Check HDFS data:"
echo "  docker exec namenode hdfs dfs -ls /space-debris/"
echo ""
