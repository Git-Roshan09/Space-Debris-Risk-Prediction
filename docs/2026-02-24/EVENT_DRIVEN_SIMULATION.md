# Event-Driven Simulation Clock

## Overview

The dashboard APIs now use an **event-driven simulation clock** instead of real-time based simulation. This means:

- ✅ **Time advances only when you tell it to** - typically after processing completes for a day
- ✅ **No conflicts with processing time** - processing can take as long as needed
- ✅ **Each datapoint gets a datetime label** from the simulation clock
- ✅ **SGP4 predicted vectors are used** when data isn't received for a specific time period

## How It Works

### Current Implementation

1. **Simulation starts at epoch**: January 1, 2004, 00:00:00
2. **Time is frozen** until you explicitly advance it
3. **After processing completes** for a day, you call the advance endpoint
4. **The simulation day moves forward** by the specified amount
5. **Data queries use the simulation time** to filter results

### Key Differences from Time-Based Simulation

| Aspect | Time-Based (Old) | Event-Driven (New) |
|--------|------------------|-------------------|
| **Advancement** | Automatic (e.g., 10 days/minute) | Manual after processing |
| **Processing conflicts** | Yes - can't keep up | No - processing-aware |
| **Control** | Limited | Full control |
| **Use case** | Real-time demos | Production pipelines |

## API Endpoints

### Get Current Simulation Time

```bash
curl http://localhost:5001/api/simulation/time
```

Response:
```json
{
  "current_simulated_time": "2004-01-05T00:00:00",
  "simulation_epoch": "2004-01-01T00:00:00",
  "simulation_mode": "event-driven",
  "simulation_description": "Time advances when processing completes for a day",
  "elapsed_simulated_days": 4.0,
  "simulation_locked": false
}
```

### Advance Time (After Processing Completes)

```bash
# Advance by 1 day
curl -X POST http://localhost:5001/api/simulation/advance \
  -H "Content-Type: application/json" \
  -d '{"days": 1}'

# Advance by 1 day and 6 hours
curl -X POST http://localhost:5001/api/simulation/advance \
  -H "Content-Type: application/json" \
  -d '{"days": 1, "hours": 6}'
```

### Set Specific Time

```bash
curl -X POST http://localhost:5001/api/simulation/set \
  -H "Content-Type: application/json" \
  -d '{"time": "2004-02-15T12:00:00"}'
```

### Reset to Epoch

```bash
curl -X POST http://localhost:5001/api/simulation/set \
  -H "Content-Type: application/json" \
  -d '{"reset": true}'
```

## Integration with Processing Pipeline

### Using the Helper Script

After your daily processing completes, advance the simulation:

```bash
# After processing finishes for the day
python scripts/advance_simulation_day.py --days 1

# Check status anytime
python scripts/advance_simulation_day.py --status
```

### Programmatic Integration

```python
import requests

def run_daily_processing():
    """Run your daily data processing."""
    # Process TLE data
    # Run SGP4 calculations
    # Detect collisions
    # Save results
    print("Processing completed for simulated day")

def advance_simulation_day():
    """Advance simulation clock after processing."""
    response = requests.post(
        "http://localhost:5001/api/simulation/advance",
        json={"days": 1}
    )
    if response.status_code == 200:
        print(f"Simulation advanced to: {response.json()['current_simulated_time']}")
    else:
        print(f"Error advancing simulation: {response.text}")

# Main processing loop
while True:
    run_daily_processing()
    advance_simulation_day()
```

### Airflow DAG Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def advance_simulation(**context):
    """Advance simulation time after processing."""
    import requests
    response = requests.post(
        "http://localhost:5001/api/simulation/advance",
        json={"days": 1}
    )
    response.raise_for_status()
    return response.json()

with DAG(
    'space_debris_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Process TLE data
    process_tle = BashOperator(
        task_id='process_tle',
        bash_command='python pipelines/ingestion/tle_ingestion.py'
    )
    
    # Run SGP4 calculations
    run_sgp4 = BashOperator(
        task_id='run_sgp4',
        bash_command='spark-submit pipelines/processing/spark_sgp4.py'
    )
    
    # Detect collisions
    detect_collisions = BashOperator(
        task_id='detect_collisions',
        bash_command='spark-submit pipelines/processing/spark_collision_prediction.py'
    )
    
    # Advance simulation time (only after all processing completes)
    advance_sim = PythonOperator(
        task_id='advance_simulation',
        python_callable=advance_simulation
    )
    
    # Define pipeline
    process_tle >> run_sgp4 >> detect_collisions >> advance_sim
```

## SGP4 Prediction Fallback

The system still maintains SGP4 prediction capabilities for handling missing data:

1. **Primary data source**: Recent TLE observations
2. **Fallback mechanism**: When TLE data isn't available for a specific time, SGP4 propagates from the last known state
3. **Collision prediction**: Uses SGP4 vectors to predict future positions
4. **Continue on gaps**: Processing doesn't stop if data is missing for certain satellites

This is handled automatically in the collision prediction pipeline at [pipelines/processing/spark_collision_prediction.py](../pipelines/processing/spark_collision_prediction.py).

## Benefits

### For Development
- **Replay scenarios** - Reset and rerun from any point
- **Debug at your pace** - No pressure from real-time clock
- **Test edge cases** - Jump to specific dates easily

### For Production
- **Handle variable processing times** - Some days process faster/slower
- **Recover from failures** - Resume from last successful day
- **Batch processing** - Process multiple days when catching up
- **Resource management** - Don't overwhelm system trying to keep up

### For Demos
- **Controlled progression** - Show exactly what you want
- **Pause and explain** - Stop time to discuss results
- **Skip ahead** - Jump to interesting time periods

## Example Workflow

```bash
# Day 1: Initial setup
python scripts/advance_simulation_day.py --reset
python scripts/advance_simulation_day.py --status

# Day 2: Process and advance
python pipelines/ingestion/tle_to_kafka.py
spark-submit pipelines/processing/spark_sgp4.py
spark-submit pipelines/processing/spark_collision_prediction.py
python scripts/advance_simulation_day.py --days 1

# Day 3: Check results
curl http://localhost:5001/api/collisions?use_simulation=true
python scripts/advance_simulation_day.py --status

# Continue processing...
```

## Monitoring

All dashboard endpoints include `simulated_time` in responses:

```bash
# Check current collisions for simulated time
curl http://localhost:5001/api/collisions

# Response includes simulation context
{
  "count": 42,
  "simulated_time": "2004-01-05T00:00:00",
  "collisions": [...]
}
```

## Notes

- **Logs use real time** - Server logs show actual timestamps for debugging
- **Data filtering uses simulated time** - API responses filtered by simulation clock
- **Thread-safe** - Uses locking to prevent concurrent updates
- **Persistent across restarts** - Store simulation state in database if needed (future enhancement)
