"""
TLE Streaming Producer - Fetches from Space-Track and publishes to Kafka
This replaces n_debris_tle_hdfs.py with a Kafka-based streaming approach.

Flow: Space-Track API → Kafka Topic (tle-raw)
"""

import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from kafka import KafkaProducer

# =============================
# CONFIG
# =============================
load_dotenv()

# Space-Track credentials
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tle-raw")

# Processing Configuration
N_DEBRIS = int(os.getenv("N_DEBRIS", 35731))
START_INDEX = int(os.getenv("START_INDEX", 2055))
SLEEP_SEC = 2  # Space-Track rate limit
LOCAL_CATALOG = "Output/space_debris_catalog.csv"

def create_kafka_producer():
    """Create Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        acks='all',
        retries=3
    )

def load_norad_ids():
    """Load NORAD IDs from local catalog."""
    import pandas as pd
    if not os.path.exists(LOCAL_CATALOG):
        print(f"❌ Catalog not found: {LOCAL_CATALOG}")
        print("   Run 'python debris.py' first to download the catalog")
        return []
    
    df = pd.read_csv(LOCAL_CATALOG)
    norad_ids = df['NORAD_CAT_ID'].dropna().astype(int).tolist()
    return norad_ids[START_INDEX:START_INDEX + N_DEBRIS]

def fetch_and_stream(st, producer, norad_id, idx, total):
    """Fetch TLE history for one NORAD ID and stream to Kafka."""
    print(f"[{idx}/{total}] NORAD {norad_id}: ", end="", flush=True)
    
    try:
        response = st.gp_history(
            norad_cat_id=norad_id,
            orderby="epoch asc",
            format="json"
        )
        
        data = json.loads(response) if isinstance(response, str) else response
        
        if not data:
            print("⚠️ No TLEs found")
            return 0
        
        count = 0
        for record in data:
            if all(k in record for k in ["EPOCH", "TLE_LINE1", "TLE_LINE2"]):
                message = {
                    "norad_id": str(norad_id),
                    "epoch": record["EPOCH"],
                    "tle_line1": record["TLE_LINE1"],
                    "tle_line2": record["TLE_LINE2"],
                    "ingested_at": datetime.utcnow().isoformat()
                }
                
                producer.send(KAFKA_TOPIC, key=str(norad_id), value=message)
                count += 1
        
        producer.flush()
        print(f"✅ {count} TLEs → Kafka")
        return count
        
    except Exception as e:
        print(f"❌ Error: {str(e)[:50]}")
        return 0

def main():
    print("=" * 70)
    print("🚀 TLE STREAMING PRODUCER")
    print("=" * 70)
    print(f"📡 Source:      Space-Track API")
    print(f"📤 Destination: Kafka ({KAFKA_BOOTSTRAP_SERVERS})")
    print(f"📂 Topic:       {KAFKA_TOPIC}")
    print(f"📊 Processing:  {N_DEBRIS} debris objects (from index {START_INDEX})")
    print("=" * 70 + "\n")
    
    # Load NORAD IDs
    norad_ids = load_norad_ids()
    if not norad_ids:
        return
    
    print(f"📦 Loaded {len(norad_ids)} NORAD IDs from catalog\n")
    
    # Initialize clients
    st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)
    
    try:
        producer = create_kafka_producer()
        print("✅ Connected to Kafka\n")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("   Make sure Kafka is running: docker-compose up -d broker")
        return
    
    # Stream TLE data
    total_tles = 0
    
    for idx, norad_id in enumerate(norad_ids, start=1):
        count = fetch_and_stream(st, producer, norad_id, idx, len(norad_ids))
        total_tles += count
        time.sleep(SLEEP_SEC)
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 STREAMING COMPLETE")
    print(f"   📊 Total TLEs sent to Kafka: {total_tles}")
    print(f"   📂 Topic: {KAFKA_TOPIC}")
    print("=" * 70)
    print("\n💡 Now run the Spark Streaming consumer to process the data:")
    print("   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 tle_stream_processor.py")
    
    producer.close()

if __name__ == "__main__":
    main()
