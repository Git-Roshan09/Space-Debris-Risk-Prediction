# TLE API Connection Issue - Resolution Guide

## Problem Summary

The Airflow DAG `tle_api_to_kafka_streaming` was failing with a connection timeout error:
```
Error getting API stats: HTTPConnectionPool(host='host.docker.internal', port=5000): 
Max retries exceeded with url: /stats (Caused by ConnectTimeoutError)
```

**Root Cause**: The Flask TLE API was running on the host machine, but Airflow containers couldn't reach it due to Docker networking restrictions between containers and the host.

## Solution Implemented

**Containerized the TLE API** - Added the Flask API as a Docker service in the stack.

### Changes Made

#### 1. Added TLE API Service to docker-compose.yml
- New service: `tle-api`
- Runs Flask API inside Docker alongside other services
- Connected to the same network as Airflow
- Health check to ensure availability

#### 2. Created Dockerfile for TLE API
- File: [deployment/Dockerfile.tle-api](deployment/Dockerfile.tle-api)
- Based on `python:3.9-slim`
- Installs Flask and required dependencies
- Exposes port 5000

#### 3. Updated Airflow DAG Configuration
- Changed `TLE_API_BASE_URL` from `http://host.docker.internal:5000` to `http://tle-api:5000`
- Uses Docker service name for DNS resolution
- Updated in both:
  - [config/airflow/airflow_dag_api_to_kafka.py](config/airflow/airflow_dag_api_to_kafka.py)
  - [pipelines/ingestion/airflow_dag_api_to_kafka.py](pipelines/ingestion/airflow_dag_api_to_kafka.py)

#### 4. Created Helper Script
- Script: [scripts/operations/restart_with_tle_api.sh](scripts/operations/restart_with_tle_api.sh)
- Stops host-based TLE API
- Rebuilds and restarts all Docker services
- Verifies TLE API health

## How to Apply the Fix

### Option 1: Using the Helper Script (Recommended)

```bash
cd /home/bharath/Documents/BigData/project/data/Space-Debris-Risk-Prediction
./scripts/operations/restart_with_tle_api.sh
```

This script will:
1. Stop the existing TLE API process on the host
2. Rebuild the TLE API Docker image
3. Restart all services with the containerized API
4. Verify health and show status

### Option 2: Manual Steps

```bash
cd deployment

# Stop the host-based TLE API (if running)
pkill -f "tle_stream_api.py"

# Rebuild and restart services
docker-compose down
docker-compose build tle-api
docker-compose up -d

# Verify TLE API is working
docker exec tle-api curl http://localhost:5000/stats
```

## Verification

After applying the fix, verify the solution:

### 1. Check TLE API Health
```bash
curl http://localhost:5000/stats
```

Expected output: JSON with `total_records`, `total_satellites`, etc.

### 2. Check Airflow DAG
- Open Airflow UI: http://localhost:8080
- Navigate to `tle_api_to_kafka_streaming` DAG
- Trigger a manual run
- Check task logs for `get_api_stats` - should now succeed without timeout errors

### 3. View Container Logs
```bash
# TLE API logs
docker-compose logs -f tle-api

# Airflow scheduler logs
docker-compose logs -f scheduler
```

## Alternative Solutions (Not Implemented)

### Alternative 1: Use Host's Actual IP Address
Instead of `host.docker.internal`, use the host machine's IP:
```python
TLE_API_BASE_URL = 'http://10.12.234.196:5000'
```

**Pros**: Quick fix, no Docker changes needed
**Cons**: IP address may change; doesn't work if host firewall blocks Docker bridge

### Alternative 2: Configure Host Firewall
Allow Docker containers to access host services:
```bash
sudo ufw allow from 172.17.0.0/16 to any port 5000
```

**Pros**: Keeps API on host
**Cons**: Security risk; requires firewall configuration

### Alternative 3: Use Docker Host Network Mode
Run Airflow with `network_mode: "host"`:
```yaml
services:
  scheduler:
    network_mode: "host"
```

**Pros**: Direct access to host services
**Cons**: Loses network isolation; port conflicts possible

## Why the Chosen Solution is Best

1. **Network Isolation**: All services communicate within Docker's internal network
2. **Consistency**: Same environment for development and potential deployment
3. **Portability**: Works on any machine without firewall/IP configuration
4. **Maintainability**: Defined in docker-compose.yml, easy to version control
5. **Scalability**: Easy to add replicas or load balancing later

## Troubleshooting

### Issue: TLE API container fails to start

**Check logs**:
```bash
docker-compose logs tle-api
```

**Common causes**:
- Missing data volume: Verify `data/processed/TLE_History` exists
- Port 5000 in use: Stop other services using port 5000

### Issue: Airflow still shows connection errors

**Verify DNS resolution**:
```bash
docker exec airflow-scheduler ping -c 2 tle-api
docker exec airflow-scheduler curl http://tle-api:5000/stats
```

**Solution**: Restart Airflow scheduler:
```bash
docker-compose restart scheduler
```

### Issue: Data not found in TLE API container

**Check volume mount**:
```bash
docker exec tle-api ls -la /app/data/processed/TLE_History
```

**Solution**: Verify the path in docker-compose.yml matches your data location

## Additional Notes

### Secret Key Warning
The warning about Airflow's `secret_key` is unrelated to this issue but should be addressed:

1. Generate a secure key:
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

2. Add to docker-compose.yml environment for all Airflow services:
```yaml
environment:
  - AIRFLOW_WEBSERVER_SECRET_KEY=<your-generated-key>
```

### Log Access (403 Forbidden)
The 403 error for log access is also unrelated. It's a known issue with Airflow's log server configuration when using SequentialExecutor. The logs are still accessible in the local files.

## References

- [Airflow Docker Networking](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Docker Compose Networking](https://docs.docker.com/compose/networking/)
- [Flask Deployment Options](https://flask.palletsprojects.com/en/2.3.x/deploying/)
