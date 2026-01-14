#!/bin/bash
set -e

# Initialize Airflow database if not already initialized
if [ ! -f /opt/airflow/airflow.db ]; then
    airflow db init
fi

# Install Python requirements
if [ -f /opt/airflow/requirements.txt ]; then
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

# Execute the command passed to the script
exec airflow "$@"
