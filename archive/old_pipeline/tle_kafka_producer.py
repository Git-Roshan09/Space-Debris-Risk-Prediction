"""
TLE Kafka Producer - Layer 1: Data Ingestion
Fetches TLE data directly from Space-Track API and publishes to Kafka.
Kafka acts as a high-throughput buffer before HDFS storage.

Architecture: Space-Track API → Kafka → (Consumer) → HDFS
"""

import os
import json
import time
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from kafka import KafkaProducer
from datetime import datetime

# =============================
# CONFIG
# =============================
load_dotenv()

# Space-Track credentials
USERNAME = (os.getenv("SPACETRACK_USER") or "").strip()
PASSWORD = (os.getenv("SPACETRACK_PASS") or "").strip()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC_TLE = os.getenv("KAFKA_TOPIC_TLE", "tle-raw")
KAFKA_TOPIC_CATALOG = os.getenv("KAFKA_TOPIC_CATALOG", "debris-catalog")

# Ingestion Configuration
N_DEBRIS = int(os.getenv("N_DEBRIS", 100))  # Number of debris objects
START_INDEX = int(os.getenv("START_INDEX", 0))
SLEEP_SEC = 2  # Space-Track rate limit
MESSAGE_DELAY = float(os.getenv("MESSAGE_DELAY", 0.01))  # Delay between Kafka messages

def create_kafka_producer():
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        acks='all',
        retries=3,
        # Fix for Confluent Platform: skip API version negotiation
        api_version=(3, 6, 0),
        request_timeout_ms=30000,
        max_block_ms=30000,
    )

def fetch_debris_catalog(st):
    """Fetch debris catalog from Space-Track."""
    print("📡 Fetching debris catalog from Space-Track...")
    
    response = st.satcat(
        object_type="DEBRIS",
        current="Y",
        orderby="launch asc",
        format="json"
    )
    
    data = json.loads(response) if isinstance(response, str) else response
    
    if not data:
        return []
    
    # Filter to essential columns
    keep_columns = [
        "NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "COUNTRY",
        "LAUNCH", "PERIOD", "INCLINATION", "APOGEE", "PERIGEE"
    ]
    
    filtered = []
    for record in data:
        filtered.append({col: record.get(col) for col in keep_columns})
    
    return filtered

def fetch_tle_history(st, norad_id):
    """Fetch TLE history for a single object from Space-Track."""
    response = st.gp_history(
        norad_cat_id=norad_id,
        orderby="epoch asc",
        format="json"
    )
    
    data = json.loads(response) if isinstance(response, str) else response
    
    if not data:
        return []
    
    tle_records = []
    for record in data:
        if all(k in record for k in ["EPOCH", "TLE_LINE1", "TLE_LINE2"]):
            tle_records.append({
                "EPOCH": record["EPOCH"],
                "TLE_LINE1": record["TLE_LINE1"],
                "TLE_LINE2": record["TLE_LINE2"]
            })
    
    return tle_records

def _ensure_topics():
    """Create Kafka topics if they don't already exist."""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            api_version=(3, 6, 0),
            request_timeout_ms=10000,
        )
        topics = [
            NewTopic(name=KAFKA_TOPIC_TLE,     num_partitions=3, replication_factor=1),
            NewTopic(name=KAFKA_TOPIC_CATALOG,  num_partitions=1, replication_factor=1),
            NewTopic(name="collision-alerts",   num_partitions=3, replication_factor=1),
        ]
        try:
            admin.create_topics(topics)
            print(f"✅ Kafka topics created: {KAFKA_TOPIC_TLE}, {KAFKA_TOPIC_CATALOG}, collision-alerts")
        except TopicAlreadyExistsError:
            print(f"✅ Kafka topics already exist")
        except Exception as e:
            print(f"   Topics may already exist: {e}")
        admin.close()
    except Exception as e:
        print(f"⚠️  Could not verify topics (will proceed anyway): {e}")


def main():
    print("=" * 60)
    print("🚀 SPACE DEBRIS TLE KAFKA PRODUCER")
    print("=" * 60)
    print(f"📡 Space-Track API → Kafka")
    print(f"🔗 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📂 Topics: {KAFKA_TOPIC_CATALOG}, {KAFKA_TOPIC_TLE}")
    print(f"📊 Processing: {N_DEBRIS} debris objects (starting at {START_INDEX})")
    print("=" * 60 + "\n")
    
    # Initialize clients
    st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

    # Ensure Kafka topics exist
    _ensure_topics()

    try:
        producer = create_kafka_producer()
        print("✅ Connected to Kafka\n")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return
    
    # Step 1: Fetch and publish debris catalog
    print("📦 STEP 1: Publishing Debris Catalog to Kafka...")
    catalog = fetch_debris_catalog(st)
    
    catalog_count = 0
    for record in catalog:
        producer.send(
            KAFKA_TOPIC_CATALOG,
            key=record.get("NORAD_CAT_ID"),
            value={
                "type": "catalog",
                "data": record,
                "ingested_at": datetime.utcnow().isoformat()
            }
        )
        catalog_count += 1
    
    producer.flush()
    print(f"✅ Published {catalog_count} catalog records to '{KAFKA_TOPIC_CATALOG}'\n")
    
    # Get NORAD IDs to process
    norad_ids = [int(r["NORAD_CAT_ID"]) for r in catalog if r.get("NORAD_CAT_ID")]
    norad_ids = norad_ids[START_INDEX:START_INDEX + N_DEBRIS]
    
    # Step 2: Fetch and publish TLE history for each debris object
    print(f"🛰️ STEP 2: Streaming TLE History to Kafka...")
    print(f"   Processing {len(norad_ids)} objects...\n")
    
    total_tle_count = 0
    
    for i, norad_id in enumerate(norad_ids, start=1):
        print(f"[{i}/{len(norad_ids)}] Fetching NORAD {norad_id}...", end=" ")
        
        try:
            tle_records = fetch_tle_history(st, norad_id)
            
            if not tle_records:
                print("⚠️ No TLEs")
                continue
            
            # Publish each TLE record to Kafka
            for tle in tle_records:
                message = {
                    "type": "tle",
                    "norad_id": str(norad_id),
                    "epoch": tle["EPOCH"],
                    "tle_line1": tle["TLE_LINE1"],
                    "tle_line2": tle["TLE_LINE2"],
                    "ingested_at": datetime.utcnow().isoformat()
                }
                
                producer.send(
                    KAFKA_TOPIC_TLE,
                    key=str(norad_id),
                    value=message
                )
                
                total_tle_count += 1
                time.sleep(MESSAGE_DELAY)
            
            producer.flush()
            print(f"✅ {len(tle_records)} TLEs published")
            
            # Rate limit for Space-Track API
            time.sleep(SLEEP_SEC)
            
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print(f"🎯 INGESTION COMPLETE")
    print(f"   📊 Catalog records: {catalog_count}")
    print(f"   🛰️ TLE records: {total_tle_count}")
    print("=" * 60)
    
    producer.close()

if __name__ == "__main__":
    main()
