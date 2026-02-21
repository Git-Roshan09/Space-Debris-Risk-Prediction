# 🚀 Quick Start Guide

## Setup in 5 Minutes

### Step 1: Install Python Dependencies (30 seconds)
```bash
pip install -r requirements-api.txt
```

### Step 2: Start Docker Services (2 minutes)
```bash
./scripts/start.sh
```

Wait for all services to start. You should see:
- ✅ Zookeeper
- ✅ Kafka
- ✅ HDFS
- ✅ Spark
- ✅ Cassandra

### Step 3: Start Flask API (5 seconds)
Open a **new terminal** and run:
```bash
python3 api.py
```

You should see:
```
Starting Flask API on http://localhost:5000
```

### Step 4: Test Everything (30 seconds)
Open **another terminal** and run:
```bash
./scripts/test_pipeline.sh
```

### Step 5: Stream Data to Kafka (1 minute)
```bash
# Test with 100 records
python3 kafka_producer.py --limit 100
```

## 🎉 Success!

You now have a fully functional big data pipeline running:
- Flask API streaming TLE data
- Kafka receiving and storing messages
- Spark ready to process data
- HDFS for distributed storage
- Cassandra for database storage
- Airflow for workflow orchestration

## 📊 View Your Data

### Kafka UI
Open browser: http://localhost:8090
- View topics
- See messages
- Monitor throughput

### Spark UI
Open browser: http://localhost:8080
- View cluster status
- Monitor jobs

### HDFS UI
Open browser: http://localhost:9870
- Browse files
- Check storage

### Airflow UI
Open browser: http://localhost:8088
- Login: admin / admin
- Manage DAGs
- Monitor workflows

## 🔄 Common Commands

```bash
# Check all services
./scripts/status.sh

# Stop everything
./scripts/stop.sh

# Restart Docker services
./scripts/stop.sh && ./scripts/start.sh

# View Kafka messages
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic space_debris_tle \
    --from-beginning \
    --max-messages 10
```

## 🐛 Something Wrong?

### Flask API won't start?
```bash
# Port 5000 might be in use
lsof -i :5000
# Kill it if needed
kill -9 $(lsof -t -i:5000)
```

### Docker issues?
```bash
# Check Docker
docker ps

# Clean restart
docker-compose down -v
./scripts/start.sh
```

### Need help?
Check [SETUP.md](SETUP.md) for detailed troubleshooting.

## 📖 Next Steps

1. **Explore the API**: `curl http://localhost:5000`
2. **Stream more data**: `python3 kafka_producer.py --acceleration 500`
3. **Write Spark jobs**: See `pipelines/processing/`
4. **Analyze data**: Use Jupyter notebooks in `notebooks/`

---

**Pro Tip**: Keep 3 terminals open:
1. Flask API (`python3 api.py`)
2. Kafka Producer (`python3 kafka_producer.py`)
3. Commands & monitoring
