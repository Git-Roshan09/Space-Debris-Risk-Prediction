#!/usr/bin/env python3
"""
Kafka Producer that consumes TLE data from the streaming API
and publishes it to Kafka in real-time.
"""

import requests
import json
import time
import logging
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TLEApiToKafkaProducer:
    """Stream TLE data from API to Kafka."""
    
    def __init__(self, 
                 api_base_url='http://localhost:5000',
                 kafka_bootstrap_servers='broker:29092',
                 kafka_topic='space_debris_tle',
                 catalog_topic='space_debris_catalog'):
        """
        Initialize the TLE API to Kafka producer.
        
        Args:
            api_base_url: Base URL of the TLE streaming API
            kafka_bootstrap_servers: Kafka broker address(es)
            kafka_topic: Kafka topic for TLE data
            catalog_topic: Kafka topic for catalog data
        """
        self.api_base_url = api_base_url
        self.kafka_topic = kafka_topic
        self.catalog_topic = catalog_topic
        
        # Initialize Kafka producer
        logger.info(f"Initializing Kafka producer for {kafka_bootstrap_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers.split(',') if isinstance(kafka_bootstrap_servers, str) else kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            max_block_ms=5000,
            acks='all',  # Wait for all replicas
            retries=3,
            compression_type='gzip'
        )
        logger.info("Kafka producer initialized successfully")
    
    def parse_tle_elements(self, tle_line1, tle_line2):
        """
        Parse orbital elements from TLE lines.
        
        Args:
            tle_line1: First line of TLE
            tle_line2: Second line of TLE
            
        Returns:
            Dictionary with parsed orbital elements
        """
        try:
            line1_parts = tle_line1.split()
            line2_parts = tle_line2.split()
            
            return {
                'inclination': float(line2_parts[2]) if len(line2_parts) > 2 else None,
                'raan': float(line2_parts[3]) if len(line2_parts) > 3 else None,
                'eccentricity': f"0.{line2_parts[4]}" if len(line2_parts) > 4 else None,
                'argument_of_perigee': float(line2_parts[5]) if len(line2_parts) > 5 else None,
                'mean_anomaly': float(line2_parts[6]) if len(line2_parts) > 6 else None,
                'mean_motion': float(line2_parts[7][:11]) if len(line2_parts) > 7 else None,
                'revolution_number': int(line2_parts[7][11:]) if len(line2_parts) > 7 and len(line2_parts[7]) > 11 else None
            }
        except (ValueError, IndexError) as e:
            logger.warning(f"Error parsing TLE elements: {e}")
            return {}
    
    def enrich_tle_record(self, record):
        """
        Enrich TLE record with additional metadata and parsed elements.
        
        Args:
            record: Raw TLE record from API
            
        Returns:
            Enriched record
        """
        # Parse orbital elements
        orbital_elements = self.parse_tle_elements(
            record['tle_line1'],
            record['tle_line2']
        )
        
        # Create enriched message
        enriched = {
            'message_id': str(uuid.uuid4()),
            'message_timestamp': datetime.utcnow().isoformat(),
            'source': 'tle_stream_api',
            'satellite_id': record['satellite_id'],
            'epoch': record['epoch'],
            'tle_line1': record['tle_line1'],
            'tle_line2': record['tle_line2'],
            'sequence_number': record.get('sequence_number'),
            'time_gap_seconds': record.get('time_gap_seconds', 0),
            **orbital_elements  # Add all parsed orbital elements
        }
        
        return enriched
    
    def send_to_kafka(self, record, topic=None):
        """
        Send a record to Kafka.
        
        Args:
            record: Record to send
            topic: Kafka topic (defaults to self.kafka_topic)
            
        Returns:
            True if successful, False otherwise
        """
        if topic is None:
            topic = self.kafka_topic
        
        try:
            # Use satellite_id as key for partitioning
            key = record.get('satellite_id')
            
            # Send to Kafka
            future = self.producer.send(topic, key=key, value=record)
            
            # Wait for send to complete (with timeout)
            future.get(timeout=10)
            
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            return False
    
    def stream_from_api_to_kafka(self, 
                                  acceleration=100, 
                                  limit=None,
                                  mode='adaptive',
                                  max_delay=5.0):
        """
        Stream TLE data from API to Kafka.
        
        Args:
            acceleration: Acceleration factor for streaming
            limit: Maximum number of records to stream
            mode: Streaming mode (proportional, fixed, adaptive)
            max_delay: Maximum delay between records
        """
        # Build stream URL
        stream_url = f"{self.api_base_url}/stream"
        params = {
            'acceleration': acceleration,
            'mode': mode,
            'max_delay': max_delay
        }
        if limit:
            params['limit'] = limit
        
        logger.info(f"Connecting to TLE stream API: {stream_url}")
        logger.info(f"Parameters: {params}")
        logger.info(f"Target Kafka topic: {self.kafka_topic}")
        logger.info("-" * 80)
        
        success_count = 0
        error_count = 0
        start_time = time.time()
        
        try:
            response = requests.get(stream_url, params=params, stream=True, timeout=3600)
            response.raise_for_status()
            
            for line in response.iter_lines():
                if line:
                    try:
                        record = json.loads(line)
                        
                        # Check if this is the summary message
                        if record.get('type') == 'summary':
                            logger.info("=" * 80)
                            logger.info("Stream Summary:")
                            logger.info(f"  Total records: {record['total_records']}")
                            logger.info(f"  Real time span: {record['real_time_span_seconds']:.2f}s")
                            logger.info(f"  Stream time: {record['stream_time_seconds']:.2f}s")
                            logger.info(f"  Effective acceleration: {record['effective_acceleration']:.2f}x")
                            continue
                        
                        # Enrich the record
                        enriched_record = self.enrich_tle_record(record)
                        
                        # Send to Kafka
                        if self.send_to_kafka(enriched_record):
                            success_count += 1
                            
                            # Log progress
                            if success_count % 100 == 0:
                                elapsed = time.time() - start_time
                                rate = success_count / elapsed
                                logger.info(
                                    f"[{success_count:6d}] Satellite: {record['satellite_id']:8s} | "
                                    f"Epoch: {record['epoch'][:19]} | "
                                    f"Rate: {rate:.2f} msg/s"
                                )
                        else:
                            error_count += 1
                            
                    except json.JSONDecodeError as e:
                        error_count += 1
                        logger.error(f"Error decoding JSON: {e}")
                        continue
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error processing record: {e}")
                        continue
            
            # Flush any pending messages
            self.producer.flush()
            
            elapsed = time.time() - start_time
            logger.info("=" * 80)
            logger.info("Streaming Complete!")
            logger.info(f"Total records sent: {success_count}")
            logger.info(f"Total errors: {error_count}")
            logger.info(f"Total time: {elapsed:.2f}s")
            logger.info(f"Average rate: {success_count/elapsed:.2f} records/second")
            logger.info("=" * 80)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to API: {e}")
            raise
        except KeyboardInterrupt:
            logger.info("\nStream interrupted by user")
            self.producer.flush()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def get_api_stats(self):
        """Get statistics from the TLE streaming API."""
        try:
            response = requests.get(f"{self.api_base_url}/stats", timeout=30)
            response.raise_for_status()
            
            stats = response.json()
            logger.info("\n=== TLE Dataset Statistics ===")
            logger.info(f"Total Records: {stats['total_records']:,}")
            logger.info(f"Total Satellites: {stats['total_satellites']}")
            logger.info(f"Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
            logger.info(f"Time Span: {stats['date_range']['span_days']} days")
            logger.info(f"Sample Satellites: {', '.join(stats['sample_satellites'][:10])}")
            
            return stats
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting API stats: {e}")
            return None
    
    def close(self):
        """Close Kafka producer."""
        logger.info("Closing Kafka producer...")
        self.producer.close()
        logger.info("Producer closed")


def main():
    """Main function to run the producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream TLE data from API to Kafka')
    parser.add_argument('--api-url', default='http://localhost:5000',
                       help='TLE streaming API base URL (default: http://localhost:5000)')
    parser.add_argument('--kafka-brokers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--kafka-topic', default='space_debris_tle',
                       help='Kafka topic for TLE data (default: space_debris_tle)')
    parser.add_argument('--acceleration', type=int, default=100,
                       help='Streaming acceleration factor (default: 100)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of records to stream (default: no limit)')
    parser.add_argument('--mode', default='adaptive', choices=['proportional', 'fixed', 'adaptive'],
                       help='Streaming mode (default: adaptive)')
    parser.add_argument('--max-delay', type=float, default=5.0,
                       help='Maximum delay between records in seconds (default: 5.0)')
    
    args = parser.parse_args()
    
    # Create producer
    producer = TLEApiToKafkaProducer(
        api_base_url=args.api_url,
        kafka_bootstrap_servers=args.kafka_brokers,
        kafka_topic=args.kafka_topic
    )
    
    try:
        # Get API stats first
        logger.info("=" * 80)
        logger.info("TLE API to Kafka Streaming Producer")
        logger.info("=" * 80)
        producer.get_api_stats()
        
        logger.info("\n" + "=" * 80)
        logger.info("Starting stream...")
        logger.info("=" * 80 + "\n")
        
        # Start streaming
        producer.stream_from_api_to_kafka(
            acceleration=args.acceleration,
            limit=args.limit,
            mode=args.mode,
            max_delay=args.max_delay
        )
        
    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        producer.close()


if __name__ == '__main__':
    main()
