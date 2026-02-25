# Simulation Clock Integration Examples

## Overview

The `advance_simulation_day.py` script can run in different environments. Here are practical integration examples.

## Running from Host Machine

### Basic Usage
```bash
# Run from project root
python scripts/advance_simulation_day.py --status
python scripts/advance_simulation_day.py --days 1
```

### After Manual Processing
```bash
# 1. Process data
docker exec -it spark-master spark-submit /opt/spark-apps/pipelines/processing/spark_sgp4_streaming.py
docker exec -it spark-master spark-submit /opt/spark-apps/pipelines/processing/spark_collision_prediction.py

# 2. Advance simulation
python scripts/advance_simulation_day.py --days 1
```

## Running from Inside Container

### Add to docker-compose.yml

You can create a helper container that runs the script:

```yaml
  simulation-controller:
    build:
      context: ./config/docker
      dockerfile: Dockerfile.spark
    image: spark-sgp4:3.5.0
    container_name: simulation-controller
    hostname: simulation-controller
    depends_on:
      - dashboard-api
    environment:
      - DASHBOARD_API_URL=http://dashboard-api:5001  # Internal Docker network
      - TZ=Asia/Kolkata
    volumes:
      - ./scripts:/opt/scripts:ro
    networks:
      - bigdata-net
    command: >
      bash -c "
        pip3 install requests &&
        python3 /opt/scripts/advance_simulation_day.py --status
      "
    restart: "no"
```

### Manual Container Execution

```bash
# Run inside any container on the bigdata-net network
docker exec -it spark-master bash -c "
  export DASHBOARD_API_URL=http://dashboard-api:5001
  python3 /path/to/advance_simulation_day.py --days 1
"
```

## Airflow DAG Integration

### Option 1: Direct Python Call (Recommended)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests

def advance_simulation_clock(**context):
    """Advance simulation after all processing completes."""
    # Use internal Docker network URL when running from Airflow container
    api_url = "http://dashboard-api:5001"
    
    response = requests.post(
        f"{api_url}/api/simulation/advance",
        json={"days": 1},
        timeout=10
    )
    response.raise_for_status()
    
    result = response.json()
    print(f"✅ Simulation advanced to: {result['current_simulated_time']}")
    return result

def check_simulation_status(**context):
    """Check current simulation status before processing."""
    api_url = "http://dashboard-api:5001"
    
    response = requests.get(f"{api_url}/api/simulation/time", timeout=10)
    response.raise_for_status()
    
    result = response.json()
    print(f"📅 Current simulated time: {result['current_simulated_time']}")
    print(f"📊 Elapsed days: {result['elapsed_simulated_days']:.2f}")
    return result

with DAG(
    'space_debris_daily_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['space-debris', 'simulation']
) as dag:
    
    # Check simulation status before starting
    check_status = PythonOperator(
        task_id='check_simulation_status',
        python_callable=check_simulation_status
    )
    
    # Process SGP4 vectors
    run_sgp4 = SparkSubmitOperator(
        task_id='run_sgp4_processing',
        application='/opt/spark-apps/pipelines/processing/spark_sgp4_streaming.py',
        conn_id='spark_default'
    )
    
    # Detect collisions
    detect_collisions = SparkSubmitOperator(
        task_id='detect_collisions',
        application='/opt/spark-apps/pipelines/processing/spark_collision_prediction.py',
        conn_id='spark_default'
    )
    
    # Advance simulation clock (ONLY after all processing completes)
    advance_clock = PythonOperator(
        task_id='advance_simulation_clock',
        python_callable=advance_simulation_clock
    )
    
    # Define pipeline flow
    check_status >> run_sgp4 >> detect_collisions >> advance_clock
```

### Option 2: Using BashOperator

```python
from airflow.operators.bash import BashOperator

with DAG(...) as dag:
    
    advance_clock = BashOperator(
        task_id='advance_simulation_clock',
        bash_command='''
            export DASHBOARD_API_URL=http://dashboard-api:5001
            python3 /opt/spark-apps/scripts/advance_simulation_day.py --days 1
        '''
    )
```

## Automated Daily Processing Script

Create a shell script for automated processing:

```bash
#!/bin/bash
# scripts/run_daily_processing.sh

set -e  # Exit on error

echo "======================================"
echo "Starting Daily Processing Pipeline"
echo "======================================"

# 1. Check current simulation status
echo "📅 Checking simulation status..."
python scripts/advance_simulation_day.py --status

# 2. Run SGP4 processing
echo "🚀 Running SGP4 processing..."
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/pipelines/processing/spark_sgp4_streaming.py

# 3. Run collision detection
echo "💥 Running collision detection..."
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/pipelines/processing/spark_collision_prediction.py

# 4. Advance simulation clock
echo "⏩ Advancing simulation clock..."
python scripts/advance_simulation_day.py --days 1

echo "======================================"
echo "✅ Daily Processing Complete!"
echo "======================================"
python scripts/advance_simulation_day.py --status
```

Make it executable:
```bash
chmod +x scripts/run_daily_processing.sh
```

Run it:
```bash
./scripts/run_daily_processing.sh
```

## Kubernetes Integration (Future)

If you deploy to Kubernetes:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: advance-simulation
spec:
  template:
    spec:
      containers:
      - name: simulation-controller
        image: your-registry/spark-sgp4:3.5.0
        command:
        - python3
        - /scripts/advance_simulation_day.py
        - --days
        - "1"
        env:
        - name: DASHBOARD_API_URL
          value: "http://dashboard-api-service:5001"
        volumeMounts:
        - name: scripts
          mountPath: /scripts
      volumes:
      - name: scripts
        configMap:
          name: simulation-scripts
      restartPolicy: Never
```

## Testing

### Test from Host
```bash
# Should work immediately
python scripts/advance_simulation_day.py --status
```

### Test from Container
```bash
# Enter any container on bigdata-net
docker exec -it spark-master bash

# Set API URL and test
export DASHBOARD_API_URL=http://dashboard-api:5001
pip3 install requests
python3 /path/to/advance_simulation_day.py --status
```

## Troubleshooting

### Script can't connect to API

**From Host:**
```bash
# Check if port is exposed
docker ps | grep dashboard-api
# Should show: 0.0.0.0:5001->5001/tcp

# Test API directly
curl http://localhost:5001/api/health
```

**From Container:**
```bash
# Check container can resolve hostname
docker exec -it spark-master ping dashboard-api

# Test API from container
docker exec -it spark-master curl http://dashboard-api:5001/api/health
```

### Wrong API URL

```bash
# From host - use localhost
export DASHBOARD_API_URL=http://localhost:5001

# From container - use container name
export DASHBOARD_API_URL=http://dashboard-api:5001

# Or specify via command line
python scripts/advance_simulation_day.py --api-url http://dashboard-api:5001
```

## Best Practices

1. **Always check status first** before advancing
2. **Only advance after processing completes** - don't advance mid-processing
3. **Use environment variables** for flexibility across environments
4. **Add the advancement step last** in your pipeline
5. **Monitor simulation drift** - ensure simulated time matches expected progression
