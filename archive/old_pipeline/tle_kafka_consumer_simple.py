"""
Simple TLE Kafka Consumer - Console output for testing
Reads TLE records from Kafka and prints state vectors to console.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer

# =============================
# CONFIG
# =============================
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tle-raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "tle-consumer-group")

def calculate_state_vector(tle_line1, tle_line2, epoch_str):
    """
    Calculate state vector from TLE using SGP4.
    Returns: dict with position and velocity
    """
    try:
        from sgp4.api import Satrec, jday
        
        if not tle_line1 or not tle_line2 or not epoch_str:
            return None
        
        # Parse the TLE
        satellite = Satrec.twoline2rv(tle_line1, tle_line2)
        
        # Parse epoch
        epoch_str = epoch_str.replace('Z', '+00:00').replace('+00:00', '')
        if '+' in epoch_str:
            epoch_str = epoch_str.split('+')[0]
        dt = datetime.fromisoformat(epoch_str)
        
        # Calculate Julian date
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, 
                      dt.second + dt.microsecond/1e6)
        
        # Propagate
        error_code, position, velocity = satellite.sgp4(jd, fr)
        
        if error_code != 0:
            return None
        
        return {
            "pos_x": round(position[0], 3),
            "pos_y": round(position[1], 3),
            "pos_z": round(position[2], 3),
            "vel_x": round(velocity[0], 6),
            "vel_y": round(velocity[1], 6),
            "vel_z": round(velocity[2], 6)
        }
    except Exception as e:
        return None

def create_kafka_consumer():
    """Create a Kafka consumer."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

def main():
    print("🚀 Starting TLE Kafka Consumer")
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📂 Topic: {KAFKA_TOPIC}")
    print(f"👥 Group: {KAFKA_GROUP_ID}")
    print("\n⏳ Waiting for messages... (Press Ctrl+C to stop)\n")
    print("-" * 80)
    
    try:
        consumer = create_kafka_consumer()
        print("✅ Connected to Kafka\n")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return
    
    message_count = 0
    
    try:
        for message in consumer:
            data = message.value
            norad_id = data.get("norad_id", "?")
            epoch = data.get("epoch", "?")
            tle_line1 = data.get("tle_line1", "")
            tle_line2 = data.get("tle_line2", "")
            
            # Calculate state vector
            state_vector = calculate_state_vector(tle_line1, tle_line2, epoch)
            
            message_count += 1
            
            if state_vector:
                print(f"[{message_count}] NORAD {norad_id} @ {epoch[:19]}")
                print(f"    Position (km): X={state_vector['pos_x']:>10}, Y={state_vector['pos_y']:>10}, Z={state_vector['pos_z']:>10}")
                print(f"    Velocity (km/s): X={state_vector['vel_x']:>10}, Y={state_vector['vel_y']:>10}, Z={state_vector['vel_z']:>10}")
                print()
            else:
                print(f"[{message_count}] NORAD {norad_id} @ {epoch[:19]} - ⚠️ SGP4 propagation failed")
            
            if message_count % 100 == 0:
                print(f"--- Processed {message_count} messages ---\n")
                
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping... Processed {message_count} messages total")
    finally:
        consumer.close()
        print("✅ Consumer closed")

if __name__ == "__main__":
    main()
