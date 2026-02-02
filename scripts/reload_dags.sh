#!/bin/bash
# Reload DAGs without restarting containers
# Useful when you only changed DAG files and want Airflow to pick them up

set -e

echo "🔄 Reloading Airflow DAGs..."
echo ""

# Copy DAG files to ensure they're synced
echo "Syncing DAG files..."
docker exec airflow-scheduler airflow dags list-import-errors

echo ""
echo "Triggering DAG folder rescan..."
docker exec airflow-scheduler airflow dags reserialize

echo ""
echo "✓ DAGs reloaded!"
echo ""
echo "📋 Available DAGs:"
docker exec airflow-scheduler airflow dags list
