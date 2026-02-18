#!/usr/bin/env python3
"""
Test script to manually push some TLE data to Kafka
This verifies our pipeline can process data.
"""

import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests

def test_manual_ingestion():
    """Send some test TLE data directly to Kafka."""
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )
    
    print("🚀 Testing manual TLE data ingestion to Kafka...")
    
    # Get some sample data from our API
    print("📡 Fetching sample data from optimized TLE API...")
    try:
        response = requests.get('http://localhost:5000/api/objects/satellites?limit=5', timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success') and 'data' in data:
            objects = data['data']['objects']
            print(f"✅ Got {len(objects)} satellite objects from API")
            
            # Send each object to Kafka
            for i, obj in enumerate(objects):
                # Create a message in the new API format
                message = {
                    'message_id': f'test-{obj["norad_id"]}-{int(time.time())}',
                    'message_timestamp': time.time(),
                    'source': 'test_manual_ingestion',
                    'norad_id': obj['norad_id'],
                    'object_name': obj['name'], 
                    'tle_line1': obj['tle_line1'],
                    'tle_line2': obj['tle_line2'],
                    'classification': obj['classification'],
                    'metadata': obj['metadata'],
                    'source_file': obj.get('source_file', 'test'),
                }
                
                # Send to Kafka
                future = producer.send(
                    'space_debris_tle',
                    key=str(obj['norad_id']),
                    value=message
                )
                
                print(f"📤 Sent object {i+1}: {obj['name'][:50]}... (NORAD ID: {obj['norad_id']}, Class: {obj['classification']})")
                time.sleep(0.5)  # Small delay between messages
            
            # Wait for all messages to be sent
            producer.flush()
            print("✅ All test messages sent to Kafka!")
            
    except Exception as e:
        print(f"❌ Error during ingestion test: {e}")
    
    finally:
        producer.close()

def check_kafka_messages():
    """Check if messages are in Kafka."""
    import subprocess
    
    print("\n🔍 Checking Kafka topic for messages...")
    try:
        # Use docker exec to check Kafka messages
        cmd = [
            'docker', 'exec', 'kafka', 
            'kafka-console-consumer', 
            '--bootstrap-server', 'localhost:9092',
            '--topic', 'space_debris_tle',
            '--from-beginning',
            '--timeout-ms', '5000'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            print(f"✅ Found {len(lines)} messages in Kafka topic!")
            print("📋 Sample message:")
            if lines:
                try:
                    sample_msg = json.loads(lines[0])
                    print(f"   NORAD ID: {sample_msg.get('norad_id')}")
                    print(f"   Name: {sample_msg.get('object_name', '')[:50]}...")
                    print(f"   Classification: {sample_msg.get('classification')}")
                    print(f"   Source: {sample_msg.get('source')}")
                except:
                    print(f"   Raw: {lines[0][:100]}...")
        else:
            print("❌ No messages found in Kafka topic")
            
    except Exception as e:
        print(f"❌ Error checking Kafka: {e}")

if __name__ == "__main__":
    test_manual_ingestion()
    check_kafka_messages()