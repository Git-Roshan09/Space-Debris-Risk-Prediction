# Data Storage Strategy: HDFS vs Kafka vs PostgreSQL

## Quick Decision Guide

```
Is it REAL-TIME streaming data?
├─ YES → Use KAFKA (temporary message queue)
│         Data flows THROUGH Kafka, not stored permanently
│
└─ NO → Is it HIGH-VOLUME time-series or historical data?
    ├─ YES → Use HDFS (long-term storage)
    │         Millions of rows, analytical queries
    │
    └─ NO → Use POSTGRESQL (operational database)
              Metadata, current state, fast lookups
```

---

## 1. KAFKA - Message Queue (Temporary Transit)

### **Purpose**: Real-time data streaming & event distribution

### **Characteristics**:
- ⏱️ **Temporary storage** (data retained 7 days by default)
- 🔄 **Multiple consumers** can read same stream
- 📊 **Ordered message delivery**
- 🚀 **High throughput** streaming

### **What to Put in Kafka**:

| Data Type | Retention | Why Kafka |
|-----------|-----------|-----------|
| **TLE Stream from API** | 7 days | Real-time ingestion, multiple Spark jobs can consume |
| **Collision Alerts** (optional) | 1-3 days | Push notifications to dashboard, real-time alerts |
| **System Events** (optional) | 1 day | Job status, errors, monitoring events |

### **Example Topics**:
```yaml
Topics:
  - space_debris_tle          # TLE data streaming from API
  - space_debris_collisions   # High-risk collision alerts (optional)
  - system_events             # Pipeline monitoring (optional)
```

### **Data Flow**:
```
TLE API → Kafka → Spark Streaming → [HDFS + PostgreSQL]
                    ↓
              (processes in real-time,
               then saves to permanent storage)
```

### **DO NOT use Kafka for**:
- ❌ Long-term storage (use HDFS)
- ❌ Historical analysis (use HDFS)
- ❌ Current state queries (use PostgreSQL)
- ❌ Transactional updates (use PostgreSQL)

---

## 2. HDFS - Data Lake (Long-term Bulk Storage)

### **Purpose**: Store massive volumes of historical data for analytics

### **Characteristics**:
- 💾 **Large-scale storage** (terabytes to petabytes)
- 📈 **Time-series data** optimized
- 🗜️ **Compressed** Parquet format
- 📊 **Batch analytics** with Spark
- 💰 **Cost-effective** for big data

### **What to Put in HDFS**:

| Data Type | Volume | Retention | Query Pattern |
|-----------|--------|-----------|---------------|
| **SGP4 Position Vectors** | Millions/day | Forever | Historical analysis, ML training |
| **Raw TLE Archive** | Thousands/day | Forever | Long-term tracking, audit trail |
| **Collision Predictions (Archive)** | Thousands/week | Forever | Pattern analysis, model validation |
| **Stopped Satellites Log** | Hundreds/day | Forever | Trend analysis, audit |

### **HDFS Directory Structure**:
```
hdfs://namenode:9000/space-debris/
├── sgp4_vectors/                    # 🎯 PRIMARY DATA
│   ├── epoch_time=2026-02-01/       # Partitioned by date
│   ├── epoch_time=2026-02-02/
│   └── epoch_time=2026-02-03/
│
├── tle_raw/                         # 📦 RAW ARCHIVE
│   ├── satellite_id=25544/          # Partitioned by satellite
│   ├── satellite_id=12345/
│   └── ...
│
├── collision_predictions/           # 📊 HISTORICAL PREDICTIONS
│   ├── batch_20260201_120000/       # Batch processing timestamp
│   ├── batch_20260202_120000/
│   └── ...
│
└── stopped_tracking/                # 🛑 AUDIT LOG
    ├── tracking_status=STOPPED_SGP4_ERROR/
    ├── tracking_status=STOPPED_LOW_ALTITUDE/
    └── tracking_status=STOPPED_STALE_TLE/
```

### **Example Queries**:
```python
# Read 7 days of SGP4 vectors for analysis
df = spark.read.parquet("hdfs://.../sgp4_vectors")
df.filter(col("epoch_time") >= "2026-01-29").show()

# Analyze collision patterns over 6 months
df = spark.read.parquet("hdfs://.../collision_predictions")
df.groupBy("risk_level", month("detection_timestamp")).count().show()

# Historical TLE evolution for satellite
df = spark.read.parquet("hdfs://.../tle_raw/satellite_id=25544")
df.orderBy("epoch_time").show()
```

### **DO NOT use HDFS for**:
- ❌ Real-time dashboard queries (too slow, use PostgreSQL)
- ❌ Single row lookups (use PostgreSQL)
- ❌ Transactional updates (use PostgreSQL)
- ❌ Small frequently-changing data (use PostgreSQL)

---

## 3. POSTGRESQL - Operational Database (Current State)

### **Purpose**: Fast queries for current operational data & metadata

### **Characteristics**:
- ⚡ **Fast indexed queries** (milliseconds)
- 🔄 **UPDATE/DELETE** support (transactional)
- 🔍 **Complex SQL queries** (JOINs, aggregations)
- 📊 **Small to medium data** (thousands to millions of rows)
- 🎯 **Point lookups** by ID

### **What to Put in PostgreSQL**:

| Table | Rows | Update Frequency | Why PostgreSQL |
|-------|------|------------------|----------------|
| **satellites** | ~1,100 | Updated hourly | Current tracking status, fast lookups by NORAD ID |
| **collision_alerts** | ~100-1,000 | Updated every batch run | Dashboard needs ONLY current/recent alerts |
| **tracking_status_changes** | Growing | Append-only | Audit log of status changes |
| **system_metrics** | Growing | Every minute | Dashboard performance stats |

### **Database Schema**:

```sql
-- Table 1: Satellite Catalog & Current Status
CREATE TABLE satellites (
    norad_id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    object_type VARCHAR(50),
    country VARCHAR(100),
    
    -- Current Tracking Status
    tracking_status VARCHAR(50) NOT NULL,  -- ACTIVE, STOPPED_SGP4_ERROR, etc.
    
    -- Latest Observations
    last_tle_epoch TIMESTAMP,
    tle_age_days INTEGER,
    last_altitude_km DOUBLE PRECISION,
    last_velocity_kms DOUBLE PRECISION,
    last_sgp4_error_code INTEGER,
    
    -- Metadata
    total_observations INTEGER DEFAULT 0,
    first_observed_at TIMESTAMP,
    status_updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_tracking_status ON satellites(tracking_status);
CREATE INDEX idx_last_tle_epoch ON satellites(last_tle_epoch DESC);
CREATE INDEX idx_altitude ON satellites(last_altitude_km);


-- Table 2: Active Collision Alerts (Recent/High-Priority ONLY)
CREATE TABLE collision_alerts (
    id SERIAL PRIMARY KEY,
    satellite_1_id INTEGER NOT NULL,
    satellite_2_id INTEGER NOT NULL,
    
    -- Prediction Details
    predicted_time TIMESTAMP NOT NULL,
    miss_distance_km DOUBLE PRECISION NOT NULL,
    relative_velocity_kms DOUBLE PRECISION,
    
    -- Risk Assessment
    risk_level VARCHAR(20) NOT NULL,  -- HIGH, MEDIUM, LOW
    collision_probability DOUBLE PRECISION,
    
    -- Metadata
    detected_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,
    archived_at TIMESTAMP,
    
    FOREIGN KEY (satellite_1_id) REFERENCES satellites(norad_id),
    FOREIGN KEY (satellite_2_id) REFERENCES satellites(norad_id),
    
    CONSTRAINT unique_collision UNIQUE (satellite_1_id, satellite_2_id, predicted_time)
);

CREATE INDEX idx_risk_level ON collision_alerts(risk_level);
CREATE INDEX idx_predicted_time ON collision_alerts(predicted_time);
CREATE INDEX idx_active_high_risk ON collision_alerts(is_active, risk_level) WHERE is_active = TRUE;
CREATE INDEX idx_satellites ON collision_alerts(satellite_1_id, satellite_2_id);


-- Table 3: Tracking Status Change History
CREATE TABLE tracking_status_changes (
    id SERIAL PRIMARY KEY,
    norad_id INTEGER NOT NULL,
    old_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,
    
    -- Why status changed
    reason TEXT,
    altitude_km DOUBLE PRECISION,
    tle_age_days INTEGER,
    sgp4_error_code INTEGER,
    
    changed_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (norad_id) REFERENCES satellites(norad_id)
);

CREATE INDEX idx_status_changes_satellite ON tracking_status_changes(norad_id, changed_at DESC);
CREATE INDEX idx_status_changes_new ON tracking_status_changes(new_status, changed_at DESC);


-- Table 4: System Metrics for Dashboard
CREATE TABLE system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_unit VARCHAR(50),
    recorded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_metrics_name_time ON system_metrics(metric_name, recorded_at DESC);
```

### **Example Queries**:
```sql
-- Dashboard: Get all active satellites
SELECT norad_id, name, tracking_status, last_altitude_km 
FROM satellites 
WHERE tracking_status = 'ACTIVE';

-- Dashboard: Get high-risk collisions in next 24 hours
SELECT * FROM collision_alerts 
WHERE risk_level = 'HIGH' 
  AND is_active = TRUE 
  AND predicted_time BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
ORDER BY miss_distance_km ASC;

-- Dashboard: Summary statistics
SELECT 
    tracking_status, 
    COUNT(*) as count,
    AVG(last_altitude_km) as avg_altitude
FROM satellites 
GROUP BY tracking_status;

-- Audit: Find satellites that stopped tracking today
SELECT * FROM tracking_status_changes 
WHERE new_status != 'ACTIVE' 
  AND changed_at >= CURRENT_DATE;
```

### **DO NOT use PostgreSQL for**:
- ❌ Millions of time-series records (use HDFS)
- ❌ High-frequency writes (use Kafka → HDFS)
- ❌ Long-term historical data (use HDFS)
- ❌ Unstructured data (use HDFS)

---

## Complete Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   TLE STREAMING API                          │
│              (Historical TLE Data Source)                    │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            v
                    ┌───────────────┐
                    │  KAFKA TOPIC  │  🔄 Temporary Queue (7 days)
                    │space_debris_tle│  
                    └───────┬───────┘
                            │
                            v
        ┌───────────────────────────────────────┐
        │     SPARK STREAMING JOBS              │
        │  (SGP4 Computation + Collision Detect)│
        └─────┬─────────────────────────┬───────┘
              │                         │
              v                         v
    ┌──────────────────┐      ┌─────────────────┐
    │      HDFS        │      │   POSTGRESQL    │
    │   (Data Lake)    │      │  (Operational)  │
    ├──────────────────┤      ├─────────────────┤
    │                  │      │                 │
    │ 🗂️ SGP4 Vectors  │      │ 📋 Satellites   │
    │   Millions/day   │      │    ~1,100 rows  │
    │   Partitioned    │      │                 │
    │   by date        │      │ ⚠️ Collision    │
    │                  │      │    Alerts       │
    │ 📦 Raw TLE       │      │    ~100 rows    │
    │   Archive        │      │    (active)     │
    │   All history    │      │                 │
    │                  │      │ 📊 Status       │
    │ 📊 Collision     │      │    Changes      │
    │   Predictions    │      │    Growing      │
    │   All history    │      │                 │
    │                  │      │ 📈 Metrics      │
    │ 🛑 Stopped       │      │    Real-time    │
    │   Tracking Log   │      │                 │
    │   Audit trail    │      │                 │
    │                  │      │                 │
    └──────────────────┘      └─────────────────┘
              │                         │
              │                         │
              └──────────┬──────────────┘
                         │
                         v
              ┌──────────────────┐
              │   DASHBOARD      │
              ├──────────────────┤
              │ • Historical     │
              │   Charts         │ ← HDFS via Spark
              │                  │
              │ • Current        │
              │   Status         │ ← PostgreSQL (fast)
              │                  │
              │ • Live Alerts    │ ← PostgreSQL (indexed)
              └──────────────────┘
```

---

## Data Lifecycle Examples

### **Example 1: TLE Data Point**

```
1. TLE arrives from API
   ↓ (seconds)
2. Kafka topic: space_debris_tle
   ↓ (milliseconds)
3. Spark consumes & computes SGP4
   ↓ (writes to both)
4a. HDFS: sgp4_vectors/          ← PERMANENT (historical analysis)
4b. PostgreSQL: satellites table  ← UPDATE status (dashboard queries)
```

### **Example 2: Collision Detection**

```
1. Spark detects collision (miss distance < 10 km)
   ↓
2a. HDFS: collision_predictions/  ← ARCHIVE (all predictions forever)
2b. PostgreSQL: collision_alerts  ← INSERT (only HIGH/MEDIUM risk)
   ↓
3. Dashboard queries PostgreSQL   ← FAST (indexed, recent data only)
```

### **Example 3: Satellite Stops Tracking**

```
1. Spark detects altitude < 150 km
   ↓
2a. HDFS: stopped_tracking/       ← LOG (audit trail, analysis)
2b. PostgreSQL: satellites        ← UPDATE tracking_status
2c. PostgreSQL: tracking_status_changes ← INSERT (history record)
   ↓
3. Dashboard shows updated status ← PostgreSQL query (milliseconds)
```

---

## Performance Comparison

| Operation | HDFS | PostgreSQL | Winner |
|-----------|------|------------|--------|
| Store 1M SGP4 vectors | ✅ Fast | ❌ Slow | HDFS |
| Query satellite by ID | ❌ Scan all files | ✅ Indexed lookup | PostgreSQL |
| Historical analysis (6 months) | ✅ Spark parallel | ❌ Table scan | HDFS |
| Update tracking status | ❌ Not supported | ✅ Single UPDATE | PostgreSQL |
| Dashboard: Recent collisions | ❌ 5-10 seconds | ✅ 10-50 ms | PostgreSQL |
| Store raw TLE archive | ✅ Compressed | ❌ Expensive | HDFS |

---

## Migration Strategy

### **Phase 1: Setup PostgreSQL** (Do This First)
1. Create database schema
2. Add PostgreSQL connector to Spark
3. Test basic read/write operations

### **Phase 2: Dual-Write from Spark**
1. Modify SGP4 job to write to HDFS + PostgreSQL
2. Write satellite metadata to PostgreSQL
3. Keep full vectors in HDFS

### **Phase 3: Update Dashboard**
1. Query PostgreSQL for live data
2. Query HDFS for historical charts
3. Remove slow HDFS queries from dashboard

---

## Rules of Thumb

### **Use HDFS when**:
- ✅ Data volume > 1 million rows
- ✅ Time-series or historical data
- ✅ Batch analytics with Spark
- ✅ Long-term archival
- ✅ Write-once, read-many pattern

### **Use PostgreSQL when**:
- ✅ Data volume < 10 million rows
- ✅ Need UPDATE/DELETE operations
- ✅ Dashboard queries (need fast response)
- ✅ Current state / metadata
- ✅ Complex SQL queries with JOINs

### **Use Kafka when**:
- ✅ Real-time data ingestion
- ✅ Multiple consumers need same data
- ✅ Event streaming
- ✅ Decoupling producers/consumers
- ✅ Temporary message queue (< 7 days)

---

## Decision Tree

```
START: Where should I store this data?

1. Is it streaming in real-time?
   YES → Put in KAFKA (temporarily)
        └─> Then process and store permanently
   NO  → Go to step 2

2. Will I need to UPDATE/DELETE records?
   YES → Use POSTGRESQL
   NO  → Go to step 3

3. Is it more than 1 million rows?
   YES → Use HDFS
   NO  → Go to step 4

4. Is it time-series or historical data?
   YES → Use HDFS
   NO  → Use POSTGRESQL

5. Do I need millisecond query response?
   YES → Use POSTGRESQL
   NO  → Use HDFS
```

---

## Summary Table

| System | Purpose | Data Size | Update Pattern | Query Speed | Cost |
|--------|---------|-----------|----------------|-------------|------|
| **Kafka** | Message Queue | Temporary | Append-only | N/A (streaming) | Low |
| **HDFS** | Data Lake | Massive (TB+) | Write-once | Slow (batch) | Very Low |
| **PostgreSQL** | Operational DB | Small-Medium | CRUD | Fast (ms) | Medium |

**Your Space Debris Project Needs ALL THREE** - Each plays a different role in the architecture.
