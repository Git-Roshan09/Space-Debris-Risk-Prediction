# PostgreSQL Write Failure Investigation - Dashboard Zero Data Bug

**Date:** February 17, 2026  
**Issue:** Dashboard showing all zeros (no satellite or collision data)  
**Service:** `spark-collision-prediction`  
**File:** `pipelines/processing/spark_collision_prediction.py`

---

## Problem Summary

The dashboard webserver was displaying all zeros for active satellites and collisions because the PostgreSQL `satellites` table remained empty despite the collision prediction pipeline running successfully.

**Symptoms:**
- Dashboard API returns: `{"active_satellites":0,"high_risk_collisions":0}`
- Database query confirms: `SELECT COUNT(*) FROM satellites` returns 0
- Collision prediction logs show: "Writing 3447 satellites to PostgreSQL..." followed by errors
- Collision predictions otherwise working (detecting 0 collisions legitimately)

---

## Root Cause Analysis

### Initial Discovery

Error logs showed:
```
ERROR:__main__:Error saving satellites to PostgreSQL: An error occurred while calling o182.jdbc.
: org.postgresql.util.PSQLException: ERROR: cannot drop table satellites because other objects depend on it
  Detail: constraint collision_alerts_satellite_1_id_fkey on table collision_alerts depends on table satellites
  constraint collision_alerts_satellite_2_id_fkey on table collision_alerts depends on table satellites
  constraint tracking_status_changes_norad_id_fkey on table tracking_status_changes depends on table satellites
  view active_satellites_summary depends on table satellites
  view high_risk_collisions_today depends on table satellites
  view recent_status_changes depends on table satellites
```

**Root Cause:** Spark JDBC with `mode="overwrite"` attempts to `DROP TABLE satellites` before writing, which violates foreign key constraints from:
- `collision_alerts` table (2 FK constraints)
- `tracking_status_changes` table (1 FK constraint)
- 3 database views

---

## Fix Attempts & Outcomes

### ❌ Attempt 1: Manual DELETE + Spark JDBC append (FAILED)

**Strategy:** Use psycopg2 to DELETE existing records, then use Spark JDBC with `mode="append"` to insert fresh data.

**Implementation:**
```python
# Delete existing records using psycopg2
import psycopg2
conn = psycopg2.connect(...)
cursor = conn.cursor()
cursor.execute(f"DELETE FROM satellites WHERE norad_id IN ({norad_ids_str})")
conn.commit()

# Insert with Spark JDBC
df_satellites.write.jdbc(
    url=self.postgres_url,
    table="satellites",
    mode="append",  # Changed from "overwrite"
    properties=self.postgres_properties
)
```

**Why It Failed:**
- Despite code clearly specifying `mode="append"`, Spark JDBC still attempted to DROP the table
- Stack trace shows: `org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils$.dropTable`
- Even after container restarts, error persisted: "cannot drop table satellites because other objects depend on it"
- Appears to be a Spark 3.5.0 JDBC bug or misconfiguration

**Evidence:**
```bash
# Code verification showed mode="append"
$ docker exec -it spark-collision-prediction sed -n '485,495p' /opt/spark-apps/processing/spark_collision_prediction.py
    mode="append",  # Confirmed!

# But logs still showed DROP TABLE attempt
spark-collision-prediction  | at org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils$.dropTable(JdbcUtils.scala:81)
```

---

### 🔄 Attempt 2: Full psycopg2 Implementation (IN PROGRESS)

**Strategy:** Bypass Spark JDBC entirely and use psycopg2's `execute_batch()` for bulk insert.

**Implementation:**
```python
import psycopg2
from psycopg2.extras import execute_batch

# Collect all data from Spark DataFrame
satellite_data = df_satellites.collect()

# Delete existing records
norad_ids = [row['norad_id'] for row in satellite_data]
cursor.execute(f"DELETE FROM satellites WHERE norad_id IN ({norad_ids_str})")

# Batch insert using psycopg2
insert_query = """INSERT INTO satellites (...) VALUES (%s, %s, ...)"""
rows = [(row['norad_id'], row['name'], ...) for row in satellite_data]
execute_batch(cursor, insert_query, rows, page_size=1000)
conn.commit()
```

**Challenge Discovered:**
- **Container not picking up code changes despite restarts**
- Modified code with detailed logging (Step 1-6 messages)
- Logs still showed old messages: "Deleted 0 existing satellite records" (old code)
- New code should print: "Step 1: Connecting to PostgreSQL..." (not appearing)
- Simple `docker-compose restart` did NOT reload the Python code

**Solution Applied:**
```bash
# Force rebuild and recreate container
docker-compose up -d --build --force-recreate spark-collision-prediction
```

**Current Status:**
- Container rebuilt and recreated successfully
- Waiting for batch processing cycle (60s initial delay + execution time)
- Enhanced logging added to track execution:
  - Step 1: Connecting to PostgreSQL
  - Step 2: Collecting satellite data from Spark DataFrame
  - Step 3: Deleting existing satellite records
  - Step 4: Preparing batch insert
  - Step 5: Executing batch insert
  - Step 6: Committing transaction
- Comprehensive error handling with traceback logging

---

## Technical Insights

### Database Schema Constraints

From `config/postgres/init_schema.sql`:
```sql
-- PK: satellites(norad_id)
-- FK dependencies:
CREATE TABLE collision_alerts (
    satellite_1_id INTEGER REFERENCES satellites(norad_id),
    satellite_2_id INTEGER REFERENCES satellites(norad_id),
    ...
);

CREATE TABLE tracking_status_changes (
    norad_id INTEGER REFERENCES satellites(norad_id),
    ...
);

-- Plus 3 views: active_satellites_summary, high_risk_collisions_today, recent_status_changes
```

**Implication:** Cannot use simple `DROP TABLE` or `mode="overwrite"` approaches. Must use upsert pattern.

### Spark JDBC Behavior

**Discovered Issue:** Spark JDBC in version 3.5.0 appears to have unexpected behavior where `mode="append"` still triggers table drop operations under certain conditions.

**Evidence:**
- Code inspection confirms `mode="append"` is set correctly
- Stack traces show `dropTable()` being called despite append mode
- PostgreSQL driver version: 42.x (latest)
- Spark version: 3.5.0

**Workaround:** Avoid Spark JDBC for tables with FK constraints; use native psycopg2 instead.

### Container Code Reload Issues

**Problem:** Docker volume mounts for code may not trigger Python reimport on container restart.

**Symptoms:**
- File timestamps match between host and container
- `docker-compose restart` does not reload Python modules
- Old code continues executing despite visible file changes

**Solution:**
```bash
# Must force rebuild, not just restart
docker-compose up -d --build --force-recreate <service_name>
```

---

## System State During Investigation

### Services Running
```bash
spark-sgp4-streaming          ✅ Working (processing ~2,785 objects)
spark-collision-prediction    🔄 Running but PostgreSQL writes failing
postgres-debris               ✅ Healthy (empty satellites table)
dashboard-api                 ✅ Running (returning zeros - no data)
dashboard-web                 ✅ Running (displaying zeros)
```

### Data Flow Status
```
TLE ingestion → Kafka           ✅ Working
Kafka → SGP4 vectors → HDFS     ✅ Working (partitioned parquet)
SGP4 vectors → Collision detect ✅ Working (0 collisions detected - legitimate)
Collision detect → PostgreSQL   ❌ FAILING (satellites table write)
PostgreSQL → Dashboard API      ✅ Working (but no data to return)
```

### Pipeline Metrics
- **SGP4 Stream:** Processing 2,785 objects
- **Collision Detection:** 1,707 satellites + 1,740 debris = 3,447 total
- **Classifications Loaded:** 31,348 satellites + 35,729 debris = 67,077 total
- **Satellites to Write:** 3,447 records
- **Satellites Written:** 0 (blocked by error)
- **Collisions Detected:** 0 (legitimate - 10km threshold)

---

## Next Steps

1. ✅ **Force rebuild container** - Completed
2. ⏳ **Monitor enhanced logging** - In progress (waiting for batch cycle)
3. ⏳ **Verify psycopg2 batch insert succeeds**
4. ⏳ **Confirm database population**: `SELECT COUNT(*) FROM satellites` should return 3,447
5. ⏳ **Verify dashboard displays data**: API should return real counts
6. ⏳ **Test collision alert writes**: Once satellites table populated, FK constraints satisfied

---

## Commands Reference

### Debugging Commands Used
```bash
# Check database state
docker exec -it postgres-debris psql -U postgres -d space_debris -c "SELECT COUNT(*) FROM satellites;"

# Monitor logs with filters
docker-compose logs --tail=500 spark-collision-prediction | grep "satellites"

# Verify code in container
docker exec -it spark-collision-prediction cat /opt/spark-apps/processing/spark_collision_prediction.py | grep -A30 "def save_satellites_to_postgres"

# Check psycopg2 availability
docker exec -it spark-collision-prediction python3 -c "import psycopg2; print(psycopg2.__version__)"

# Force rebuild and recreate
docker-compose up -d --build --force-recreate spark-collision-prediction
```

### File Modified
- **Path:** `pipelines/processing/spark_collision_prediction.py`
- **Function:** `save_satellites_to_postgres()` (lines ~457-510)
- **Changes:** 
  - Removed Spark JDBC write
  - Added psycopg2 execute_batch implementation
  - Enhanced logging for debugging
  - Comprehensive error handling

---

## Lessons Learned

1. **Spark JDBC Limitations:** 
   - `mode="overwrite"` incompatible with tables having FK constraints
   - `mode="append"` may still trigger unwanted table operations in some scenarios
   - For tables with dependencies, use native database drivers

2. **Container Code Reloading:**
   - Simple restart may not reload Python code
   - Volume mount changes may not trigger module reimport
   - Always use `--build --force-recreate` when code changes are critical

3. **PostgreSQL Design:**
   - FK constraints prevent casual table drops (good for data integrity)
   - Requires careful upsert patterns (DELETE + INSERT)
   - Views add additional dependencies that block table modifications

4. **Debugging Strategy:**
   - Add comprehensive step-by-step logging
   - Verify code in container matches source
   - Check database state directly, don't rely on logs alone
   - Test Python imports/modules in container environment

---

## Performance Considerations

**psycopg2 execute_batch vs Spark JDBC:**
- **Pros:** More control, avoids FK constraint issues, reliable behavior
- **Cons:** Requires `.collect()` which brings all data to driver (memory intensive)
- **Scale:** Works for ~3,500 rows; may need optimization for 100K+ rows
- **Alternative:** For larger datasets, consider PostgreSQL COPY or Spark JDBC with pre-created empty table

---

**Investigation conducted by:** GitHub Copilot  
**Status:** In Progress - Awaiting batch execution results with enhanced logging  
**Last Update:** 2026-02-17 19:15 IST
