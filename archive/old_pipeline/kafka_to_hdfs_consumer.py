"""
TLE Kafka to HDFS Consumer - Layer 2: Storage
Consumes TLE data from Kafka and writes to HDFS Data Lake.
Also calculates state vectors and stores structured data.

Architecture: Kafka → Consumer → HDFS (Raw + Processed)
"""

import os
import json
import io
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer

# =============================
# CONFIG
# =============================
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC_TLE = os.getenv("KAFKA_TOPIC_TLE", "tle-raw")
KAFKA_TOPIC_CATALOG = os.getenv("KAFKA_TOPIC_CATALOG", "debris-catalog")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "hdfs-writer-group")

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_BASE_PATH = "/space-debris"

# Batch Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))  # Write to HDFS after this many records
BATCH_TIMEOUT_SEC = int(os.getenv("BATCH_TIMEOUT_SEC", 30))  # Or after this many seconds

def write_to_hdfs(data, hdfs_path, filename, replication=1):
    """Write data to HDFS using WebHDFS REST API."""
    if isinstance(data, pd.DataFrame):
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer, index=False)
        content = csv_buffer.getvalue().encode('utf-8')
    else:
        content = json.dumps(data).encode('utf-8')
    
    full_path = f"{hdfs_path}/{filename}"
    
    # Create directory
    mkdir_url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}"
    requests.put(mkdir_url)
    
    # Create file
    create_url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{full_path}?op=CREATE&overwrite=true&replication={replication}&user.name={HDFS_USER}"
    
    response = requests.put(create_url, allow_redirects=False)
    
    if response.status_code == 307:
        datanode_url = response.headers['Location']
        datanode_url = datanode_url.replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
        
        upload_response = requests.put(datanode_url, data=content,
                                       headers={'Content-Type': 'application/octet-stream'})
        return upload_response.status_code == 201
    
    return False

def calculate_state_vector(tle_line1, tle_line2, epoch_str):
    """Calculate state vector from TLE using SGP4."""
    try:
        from sgp4.api import Satrec, jday
        
        if not tle_line1 or not tle_line2 or not epoch_str:
            return None
        
        satellite = Satrec.twoline2rv(tle_line1, tle_line2)
        
        # Parse epoch
        epoch_clean = epoch_str.replace('Z', '').split('+')[0]
        dt = datetime.fromisoformat(epoch_clean)
        
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                      dt.second + dt.microsecond/1e6)
        
        error_code, position, velocity = satellite.sgp4(jd, fr)
        
        if error_code != 0:
            return None
        
        return {
            "pos_x": round(position[0], 6),
            "pos_y": round(position[1], 6),
            "pos_z": round(position[2], 6),
            "vel_x": round(velocity[0], 9),
            "vel_y": round(velocity[1], 9),
            "vel_z": round(velocity[2], 9)
        }
    except Exception:
        return None

def process_catalog_batch(records):
    """Process and write catalog records to HDFS."""
    if not records:
        return
    
    df = pd.DataFrame([r['data'] for r in records])
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"catalog_{timestamp}.csv"
    
    if write_to_hdfs(df, f"{HDFS_BASE_PATH}/catalog", filename):
        print(f"  💾 Wrote {len(records)} catalog records to HDFS: {filename}")
    else:
        print(f"  ❌ Failed to write catalog to HDFS")

def process_tle_batch(records):
    """Process TLE records: calculate state vectors and write to HDFS."""
    if not records:
        return
    
    processed = []
    
    for record in records:
        sv = calculate_state_vector(
            record.get('tle_line1'),
            record.get('tle_line2'),
            record.get('epoch')
        )
        
        row = {
            "norad_id": record.get('norad_id'),
            "epoch": record.get('epoch'),
            "tle_line1": record.get('tle_line1'),
            "tle_line2": record.get('tle_line2'),
        }
        
        if sv:
            row.update(sv)
        
        processed.append(row)
    
    df = pd.DataFrame(processed)
    
    # Write raw TLEs
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    raw_filename = f"tle_batch_{timestamp}.csv"
    
    if write_to_hdfs(df[['norad_id', 'epoch', 'tle_line1', 'tle_line2']], 
                     f"{HDFS_BASE_PATH}/raw-tle", raw_filename):
        print(f"  💾 Raw TLEs: {len(records)} records → {raw_filename}")
    
    # Write processed state vectors
    sv_df = df[df['pos_x'].notna()]
    if not sv_df.empty:
        sv_filename = f"state_vectors_{timestamp}.csv"
        if write_to_hdfs(sv_df, f"{HDFS_BASE_PATH}/state-vectors", sv_filename):
            print(f"  💾 State Vectors: {len(sv_df)} records → {sv_filename}")

def main():
    print("=" * 60)
    print("🚀 KAFKA → HDFS CONSUMER (Layer 2: Storage)")
    print("=" * 60)
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📂 Topics: {KAFKA_TOPIC_CATALOG}, {KAFKA_TOPIC_TLE}")
    print(f"💾 HDFS: {HDFS_BASE_PATH}")
    print(f"📦 Batch: {BATCH_SIZE} records or {BATCH_TIMEOUT_SEC}s timeout")
    print("=" * 60 + "\n")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_TLE,
            KAFKA_TOPIC_CATALOG,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=BATCH_TIMEOUT_SEC * 1000
        )
        print("✅ Connected to Kafka\n")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return
    
    catalog_batch = []
    tle_batch = []
    total_processed = 0
    last_write_time = time.time()
    
    print("⏳ Consuming messages... (Press Ctrl+C to stop)\n")
    
    try:
        while True:
            try:
                for message in consumer:
                    data = message.value
                    msg_type = data.get('type', 'unknown')
                    
                    if msg_type == 'catalog':
                        catalog_batch.append(data)
                    elif msg_type == 'tle':
                        tle_batch.append(data)
                    
                    total_processed += 1
                    
                    # Check if we should write batch
                    should_write = (
                        len(tle_batch) >= BATCH_SIZE or
                        len(catalog_batch) >= BATCH_SIZE or
                        (time.time() - last_write_time) >= BATCH_TIMEOUT_SEC
                    )
                    
                    if should_write:
                        print(f"\n📦 Writing batch (processed {total_processed} total)...")
                        process_catalog_batch(catalog_batch)
                        process_tle_batch(tle_batch)
                        
                        catalog_batch = []
                        tle_batch = []
                        last_write_time = time.time()
                        
            except StopIteration:
                # Timeout reached, write remaining
                if catalog_batch or tle_batch:
                    print(f"\n⏰ Timeout - writing remaining batch...")
                    process_catalog_batch(catalog_batch)
                    process_tle_batch(tle_batch)
                    catalog_batch = []
                    tle_batch = []
                    last_write_time = time.time()
                
                print("   Waiting for more messages...")
                time.sleep(5)
                
    except KeyboardInterrupt:
        print(f"\n\n🛑 Stopping...")
        
        # Write remaining records
        if catalog_batch or tle_batch:
            print("📦 Writing final batch...")
            process_catalog_batch(catalog_batch)
            process_tle_batch(tle_batch)
        
        print(f"✅ Total processed: {total_processed} messages")
    
    finally:
        consumer.close()
        print("✅ Consumer closed")

if __name__ == "__main__":
    main()
