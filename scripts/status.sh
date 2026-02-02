#!/bin/bash
set -e

echo "Service Status Check"
echo "===================="
docker-compose ps
echo ""
echo "Web UIs:"
echo "  Airflow:  http://localhost:8088 (admin/admin)"
echo "  Kafka UI: http://localhost:8090"
echo "  Spark:    http://localhost:8080"
echo "  HDFS:     http://localhost:9870"
