# Space Debris Pipeline - Issues & Problems Checklist
**Date:** February 21, 2026  
**Status:** Analysis Complete  
**Priority:** High - Multiple critical issues affecting performance and reliability

---

## 🔴 CRITICAL ISSUES

### 1. Kafka Producer - Invalid Bootstrap Server Configuration
- **File:** `pipelines/ingestion/dag_tle_ingestion_only.py` (line 33)
- **Issue:** `KAFKA_BOOTSTRAP_SERVERS = ['broker:29092', 'kafka:9093']`
  - `broker:29092` does NOT exist in docker-compose.yml
  - Only `kafka:9093` and `localhost:9092` are configured
- **Impact:** 
  - Connection delays on every Airflow run (tries non-existent broker first)
  - 5-10 second delay every 20 minutes
  - Unnecessary error logging
- **Fix Required:** Remove `'broker:29092'` from list
- **Priority:** 🔴 HIGH

### 2. Synchronous Kafka Producer - Performance Bottleneck
- **File:** `pipelines/ingestion/dag_tle_ingestion_only.py` (lines 198-203)
- **Issue:** 
  ```python
  future = producer.send(...)
  future.get(timeout=10)  # BLOCKS on EVERY message!
  ```
- **Impact:**
  - **10-50x slower** than async batch sending
  - Waits for acknowledgment before sending next message
  - With 1000 objects: adds 10+ seconds of network wait time
- **Fix Required:** Remove `.get()` call, use batch flush instead
- **Priority:** 🔴 CRITICAL

### 3. Spark SGP4 - Reprocesses All Data on Restart
- **File:** `pipelines/processing/spark_sgp4_to_hdfs.py` (line 173)
- **Issue:** `.option("startingOffsets", "earliest")`
- **Impact:**
  - Every container restart reprocesses ALL messages from topic start
  - Duplicate data written to HDFS
  - Hours of wasted computation on millions of old records
  - PostgreSQL gets duplicate/stale updates
- **Fix Required:** Change to `"latest"` or remove (let checkpoint handle it)
- **Priority:** 🔴 HIGH

### 4. No Pipeline Coordination - SGP4 → Collision Prediction
- **File:** `scripts/spark_collision_prediction.sh` (lines 19-41)
- **Issue:** 
  - Collision prediction runs every 10 seconds in infinite loop
  - No awareness of new SGP4 data availability
  - Runs 30 times per 20-minute ingestion cycle
  - Processes same HDFS data repeatedly
- **Impact:**
  - **95% wasted computation** - cross joins on millions of objects
  - Massive CPU/memory waste
  - Same collision results written 30 times
  - No coordination between components
- **Fix Required:** Implement state-based coordination (database flags or event-driven)
- **Priority:** 🔴 CRITICAL

---

## 🟡 HIGH PRIORITY ISSUES

### 5. SGP4 PostgreSQL Writes - Append Mode Creates Duplicates
- **File:** `pipelines/processing/spark_sgp4_to_hdfs.py` (lines 329-332)
- **Issue:**
  ```python
  pg.write_table(
      satellite_updates,
      table_name="satellites",
      mode="append",  # ❌ Keeps adding duplicates!
  ```
- **Impact:**
  - PostgreSQL table gets duplicate satellite records every 30 seconds
  - `norad_id` is PRIMARY KEY → will cause insert failures OR grows infinitely
  - Violates database constraints
  - Inconsistent state for dashboard
- **Fix Required:** 
  - Use upsert/ON CONFLICT UPDATE logic
  - OR delete before insert
  - OR use proper merge strategy
- **Priority:** 🟡 HIGH

### 6. Collision Prediction PostgreSQL - Delete Before Insert Pattern
- **File:** `pipelines/processing/spark_collision_prediction.py` (lines 562-566)
- **Issue:**
  ```python
  # Deletes ALL existing satellites, then reinserts
  cursor.execute(f"DELETE FROM satellites WHERE norad_id IN ({norad_ids_str})")
  ```
- **Impact:**
  - Race condition: Dashboard queries fail during delete window
  - No transaction isolation between delete and insert
  - If insert fails, satellites are permanently deleted
  - Foreign key cascade could delete collision_alerts
- **Fix Required:** Use UPSERT or transaction wrapping
- **Priority:** 🟡 HIGH

### 7. No Backpressure Control in Spark Streaming
- **File:** `pipelines/processing/spark_sgp4_to_hdfs.py`
- **Issue:** Missing `.option("maxOffsetsPerTrigger", ...)`
- **Impact:**
  - If Kafka accumulates 100K+ messages during downtime
  - Spark tries to process ALL in single micro-batch
  - Memory overflow / executor failures
  - No rate limiting on consumption
- **Fix Required:** Add `maxOffsetsPerTrigger` (e.g., 10000)
- **Priority:** 🟡 HIGH

### 8. Silent Data Loss Configuration
- **File:** `pipelines/processing/spark_sgp4_to_hdfs.py` (line 174)
- **Issue:** `.option("failOnDataLoss", "false")`
- **Impact:**
  - If Kafka deletes old messages (retention policy)
  - Spark silently skips them without alerts
  - No visibility into missing data
- **Fix Required:** Set to `"true"` or implement monitoring
- **Priority:** 🟡 MEDIUM

---

## 🟢 MEDIUM PRIORITY ISSUES

### 9. Multiple Checkpoints - Potential Offset Drift
- **File:** `pipelines/processing/spark_sgp4_to_hdfs.py`
- **Issue:** 4 separate writeStream queries with different checkpoints:
  1. `/stopped_tracking` (line 290)
  2. `/postgres_metadata` (line 346)
  3. `/tle_raw` (line 359)
  4. `/hdfs` (line 374)
- **Impact:**
  - Each tracks Kafka offsets independently
  - If one fails and restarts, may be out of sync with others
  - Complex recovery scenario
  - Difficult to guarantee exactly-once semantics
- **Fix Required:** Consolidate or ensure coordination
- **Priority:** 🟢 MEDIUM

### 10. PostgreSQL Connection Pool Not Used
- **Files:** 
  - `pipelines/processing/spark_sgp4_to_hdfs.py` (line 307)
  - `pipelines/processing/spark_collision_prediction.py` (line 543)
- **Issue:** 
  - Creates new connection every 30 seconds (SGP4)
  - Creates new connection every run (collision)
  - No connection pooling
- **Impact:**
  - Connection overhead on every batch
  - PostgreSQL connection limit exhaustion risk
  - Slower writes
- **Fix Required:** Implement connection pooling
- **Priority:** 🟢 MEDIUM

### 11. Timing Mismatch - Ingestion vs Processing
- **Files:** Multiple
- **Issue:**
  - Airflow ingests: Every **20 minutes**
  - SGP4 processes: Every **30 seconds** (micro-batches)
  - Collision runs: Every **10 seconds** (wasteful loop)
- **Impact:**
  - Most collision runs process no new data
  - Resource waste during idle periods
  - No alignment between components
- **Fix Required:** Align schedules or use event-driven triggers
- **Priority:** 🟢 MEDIUM

### 12. PostgreSQL Default Host Wrong
- **File:** `pipelines/processing/postgres_utils.py` (line 206)
- **Issue:** Default host is `'postgres'` but container is `'postgres-debris'`
- **Impact:**
  - Only works because env var overrides default
  - Confusing for debugging
  - Breaks if env var missing
- **Fix Required:** Update default to `'postgres-debris'`
- **Priority:** 🟢 LOW

---

## 🔵 ARCHITECTURAL ISSUES

### 13. No Pipeline State Management
- **Issue:** No centralized tracking of:
  - Last successful ingestion time
  - Last SGP4 batch completion
  - Data version/freshness
  - Component health status
- **Impact:**
  - Components can't coordinate
  - No visibility into pipeline state
  - Difficult to debug failures
  - Can't implement smart scheduling
- **Recommendation:** Implement pipeline_state table in PostgreSQL
- **Priority:** 🔵 ARCHITECTURAL

### 14. No Communication Between SGP4 and Collision Prediction
- **Issue:** 
  - SGP4 writes to HDFS silently
  - Collision prediction polls blindly every 10 seconds
  - No event notification when new data available
- **Impact:**
  - Massive waste of computation
  - Can't scale efficiently
  - No guarantee collision uses latest data
- **Recommendation:** Implement one of:
  - Event-driven Kafka topic for pipeline events
  - PostgreSQL state table with timestamps
  - HDFS marker files (_SUCCESS flags)
  - Airflow orchestration of full pipeline
- **Priority:** 🔵 ARCHITECTURAL

### 15. Mixed Streaming and Batch Processing
- **Issue:**
  - SGP4 is streaming (continuous)
  - Collision is batch (periodic loop)
  - Airflow is scheduled (20 minutes)
  - No unified processing model
- **Impact:**
  - Complex operational model
  - Different failure modes
  - Difficult to reason about end-to-end latency
  - Resource utilization unpredictable
- **Recommendation:** Choose unified approach (all streaming OR all batch)
- **Priority:** 🔵 ARCHITECTURAL

---

## 📊 PERFORMANCE IMPACT SUMMARY

| Component | Current State | Problem | Est. Waste |
|-----------|--------------|---------|------------|
| Kafka Producer | Synchronous | Blocks on every send | 10-50x slower |
| Collision Loop | 10s interval | Runs on same data 30x | 95% waste |
| SGP4 Restart | Reprocess all | Reads from earliest | Hours lost |
| Cross Joins | No coordination | Millions of comparisons | High CPU |
| PostgreSQL | No pooling | Reconnect every batch | Connection overhead |

**Estimated Overall Waste:** 70-90% of compute resources

---

## 🎯 RECOMMENDED FIX PRIORITY

### Immediate (Do Today)
1. ✅ Fix Kafka broker address (`broker:29092` → remove)
2. ✅ Remove synchronous `.get()` from Kafka producer
3. ✅ Change `startingOffsets` to `"latest"`
4. ✅ Add `maxOffsetsPerTrigger` to Spark streaming

### Short Term (This Week)
5. ✅ Implement collision prediction coordination (state table)
6. ✅ Fix PostgreSQL upsert logic in both SGP4 and collision
7. ✅ Increase collision check interval from 10s to 5 minutes
8. ✅ Add connection pooling for PostgreSQL

### Medium Term (This Sprint)
9. ✅ Implement pipeline_state table for coordination
10. ✅ Consolidate Spark checkpoints or document strategy
11. ✅ Align component timings (20 min ingestion, 20 min collision)
12. ✅ Add monitoring and alerting for data loss

### Long Term (Architectural)
13. ✅ Design event-driven pipeline coordination
14. ✅ Evaluate streaming vs batch trade-offs
15. ✅ Implement proper state management system

---

## 🔍 VERIFICATION COMMANDS

### Check Current Issues

```bash
# 1. Check Airflow Kafka connection errors
docker logs airflow-scheduler 2>&1 | grep -i "broker\|kafka\|connection error"

# 2. Check Spark SGP4 offset behavior
docker exec namenode hdfs dfs -cat /tmp/spark-checkpoint-sgp4/offsets/*/offsets

# 3. Check PostgreSQL for duplicate satellites
docker exec postgres-debris psql -U postgres -d space_debris -c \
  "SELECT norad_id, COUNT(*) FROM satellites GROUP BY norad_id HAVING COUNT(*) > 1;"

# 4. Check collision prediction frequency
docker logs spark-collision-prediction --tail 50 | grep "Running collision prediction"

# 5. Check Kafka consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spark-sgp4-consumer-group --describe

# 6. Check HDFS data growth (detect duplicates)
docker exec namenode hdfs dfs -du -s -h /space-debris/sgp4_vectors
```

### Monitor Performance

```bash
# Track ingestion rate
docker logs airflow-scheduler 2>&1 | grep "msg/s"

# Track Spark processing
docker logs spark-sgp4-streaming | grep "Batch.*Updated.*PostgreSQL"

# Track collision waste
docker logs spark-collision-prediction | grep "No new data\|collision pairs detected"
```

---

## 📝 NOTES

- **Cross joins are intentional** for collision detection - not a bug
- **PostgreSQL schema is well-designed** with proper indexes
- **Checkpointing is configured** but offset strategy needs review
- **Error handling exists** but may be too permissive (failOnDataLoss=false)
- **20-minute ingestion interval is reasonable** for orbital mechanics

---

## ✅ NEXT STEPS

1. [ ] Review this checklist with team
2. [ ] Prioritize fixes based on impact
3. [ ] Create tracking issues for each item
4. [ ] Implement immediate fixes first
5. [ ] Design long-term architecture improvements
6. [ ] Add monitoring and alerting
7. [ ] Document resolution of each issue

---

**Generated:** 2026-02-21  
**Last Updated:** 2026-02-21  
**Reviewer:** Pending  
**Status:** Ready for Team Review
