#!/usr/bin/env python3
"""
Kafka Producer - Streams data from Flask API to Kafka
Connects to API on localhost:5000 and Kafka on localhost:9092
"""

import requests
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TLEStreamToKafka:
    """Stream TLE data from Flask API to Kafka."""
    
    def __init__(self, 
                 api_url='http://localhost:5000',
                 kafka_servers='localhost:9092',
                 kafka_topic='space_debris_tle'):
        """
        Initialize the streamer.
        
        Args:
            api_url: Flask API base URL
            kafka_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic name
        """
        self.api_url = api_url
        self.kafka_topic = kafka_topic
        
        # Initialize Kafka producer
        logger.info(f"Connecting to Kafka: {kafka_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info("Kafka producer initialized")
    
    def check_api_health(self):
        """Check if the API is healthy."""
        try:
            response = requests.get(f"{self.api_url}/health", timeout=5)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"API health: {data}")
                return True
            else:
                logger.error(f"API health check failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"API health check error: {e}")
            return False
    
    def stream_to_kafka(self, acceleration=100, limit=None, mode='adaptive'):
        """
        Stream TLE data from API to Kafka.
        
        Args:
            acceleration: Speed multiplier
            limit: Maximum records to stream
            mode: Streaming mode
        """
        if not self.check_api_health():
            logger.error("API is not healthy, aborting")
            return
        
        # Build stream URL
        stream_url = f"{self.api_url}/stream?acceleration={acceleration}&mode={mode}"
        if limit:
            stream_url += f"&limit={limit}"
        
        logger.info(f"Starting stream from: {stream_url}")
        logger.info(f"Publishing to Kafka topic: {self.kafka_topic}")
        
        record_count = 0
        error_count = 0
        start_time = time.time()
        
        try:
            response = requests.get(stream_url, stream=True, timeout=30)
            response.raise_for_status()
            
            for line in response.iter_lines():
                if line:
                    try:
                        record = json.loads(line.decode('utf-8'))
                        
                        # Skip summary messages
                        if record.get('type') == 'summary':
                            logger.info(f"Stream summary: {record}")
                            continue
                        
                        # Add metadata
                        enriched_record = {
                            **record,
                            'ingestion_timestamp': datetime.utcnow().isoformat(),
                            'source': 'tle_api',
                            'kafka_topic': self.kafka_topic
                        }
                        
                        # Send to Kafka
                        key = record['satellite_id']
                        future = self.producer.send(
                            self.kafka_topic,
                            key=key,
                            value=enriched_record
                        )
                        
                        # Wait for acknowledgment (blocking)
                        future.get(timeout=10)
                        
                        record_count += 1
                        
                        # Log progress
                        if record_count % 100 == 0:
                            elapsed = time.time() - start_time
                            rate = record_count / elapsed if elapsed > 0 else 0
                            logger.info(f"Processed {record_count} records ({rate:.1f} records/sec)")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")
                        error_count += 1
                    except KafkaError as e:
                        logger.error(f"Kafka error: {e}")
                        error_count += 1
                    except Exception as e:
                        logger.error(f"Unexpected error: {e}")
                        error_count += 1
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request error: {e}")
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        finally:
            # Flush and close producer
            self.producer.flush()
            logger.info(f"Stream complete: {record_count} records sent, {error_count} errors")
            
            elapsed = time.time() - start_time
            if elapsed > 0:
                logger.info(f"Average rate: {record_count / elapsed:.1f} records/sec")
    
    def close(self):
        """Close the Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream TLE data from API to Kafka')
    parser.add_argument('--api-url', default='http://localhost:5000',
                       help='Flask API URL (default: http://localhost:5000)')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='space_debris_tle',
                       help='Kafka topic name (default: space_debris_tle)')
    parser.add_argument('--acceleration', type=int, default=100,
                       help='Stream acceleration factor (default: 100)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maximum records to stream (default: all)')
    parser.add_argument('--mode', default='adaptive',
                       choices=['adaptive', 'fixed', 'proportional'],
                       help='Streaming mode (default: adaptive)')
    
    args = parser.parse_args()
    
    # Create streamer
    streamer = TLEStreamToKafka(
        api_url=args.api_url,
        kafka_servers=args.kafka_servers,
        kafka_topic=args.topic
    )
    
    try:
        # Start streaming
        streamer.stream_to_kafka(
            acceleration=args.acceleration,
            limit=args.limit,
            mode=args.mode
        )
    finally:
        streamer.close()


if __name__ == '__main__':
    main()
