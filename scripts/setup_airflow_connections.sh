#!/bin/bash
# Script to configure Airflow connections for Spark

echo "Setting up Airflow connections..."

# Add Spark connection
docker exec airflow-scheduler airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark-master' \
    --conn-port '7077' \
    --conn-extra '{"queue": "root.default", "deploy-mode": "client"}'

echo "✓ Spark connection configured"

# Verify connections
docker exec airflow-scheduler airflow connections list | grep spark

echo "Done! Airflow is now configured to submit jobs to Spark cluster."
