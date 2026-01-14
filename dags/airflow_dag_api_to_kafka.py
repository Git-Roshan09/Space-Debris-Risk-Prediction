"""
Airflow DAG to stream TLE data from the streaming API to Kafka
This DAG consumes real-time TLE data from the Flask API and publishes to Kafka
"""

import uuid
import json
import logging
import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'space-debris-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

# Configuration
TLE_API_BASE_URL = 'http://localhost:5000'  # Update this to your API URL
KAFKA_BOOTSTRAP_SERVERS = ['broker:29092']
KAFKA_TOPIC_TLE = 'space_debris_tle'
STREAM_ACCELERATION = 100  # 100x real-time
STREAM_LIMIT = 1000  # Number of records per DAG run
STREAM_MODE = 'adaptive'
MAX_DELAY = 5.0


def parse_tle_elements(tle_line1, tle_line2):
    """Parse orbital elements from TLE lines."""
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
        }
    except (ValueError, IndexError) as e:
        logger.warning(f"Error parsing TLE elements: {e}")
        return {}


def get_api_stats():
    """Get statistics from the TLE streaming API."""
    try:
        response = requests.get(f"{TLE_API_BASE_URL}/stats", timeout=30)
        response.raise_for_status()
        
        stats = response.json()
        logger.info("\n=== TLE Dataset Statistics ===")
        logger.info(f"Total Records: {stats['total_records']:,}")
        logger.info(f"Total Satellites: {stats['total_satellites']}")
        logger.info(f"Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting API stats: {e}")
        return None


def stream_api_to_kafka():
    """Stream TLE data from API to Kafka."""
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    
    logger.info("Starting TLE API to Kafka streaming")
    
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            max_block_ms=5000,
            acks='all',
            compression_type='gzip'
        )
        logger.info("Kafka producer initialized")
        
        # Build stream URL
        stream_url = f"{TLE_API_BASE_URL}/stream"
        params = {
            'acceleration': STREAM_ACCELERATION,
            'mode': STREAM_MODE,
            'max_delay': MAX_DELAY,
            'limit': STREAM_LIMIT
        }
        
        logger.info(f"Connecting to: {stream_url}")
        logger.info(f"Parameters: {params}")
        
        success_count = 0
        error_count = 0
        start_time = time.time()
        
        # Stream from API
        response = requests.get(stream_url, params=params, stream=True, timeout=3600)
        response.raise_for_status()
        
        for line in response.iter_lines():
            if line:
                try:
                    record = json.loads(line)
                    
                    # Skip summary messages
                    if record.get('type') == 'summary':
                        logger.info(f"Stream summary: {record}")
                        continue
                    
                    # Parse orbital elements
                    orbital_elements = parse_tle_elements(
                        record['tle_line1'],
                        record['tle_line2']
                    )
                    
                    # Enrich record
                    enriched_record = {
                        'message_id': str(uuid.uuid4()),
                        'message_timestamp': datetime.utcnow().isoformat(),
                        'source': 'tle_stream_api',
                        'satellite_id': record['satellite_id'],
                        'epoch': record['epoch'],
                        'tle_line1': record['tle_line1'],
                        'tle_line2': record['tle_line2'],
                        'sequence_number': record.get('sequence_number'),
                        'time_gap_seconds': record.get('time_gap_seconds', 0),
                        **orbital_elements
                    }
                    
                    # Send to Kafka
                    future = producer.send(
                        KAFKA_TOPIC_TLE,
                        key=record['satellite_id'],
                        value=enriched_record
                    )
                    future.get(timeout=10)
                    
                    success_count += 1
                    
                    if success_count % 100 == 0:
                        logger.info(f"Streamed {success_count} records to Kafka")
                        
                except json.JSONDecodeError as e:
                    error_count += 1
                    logger.error(f"JSON decode error: {e}")
                except KafkaError as e:
                    error_count += 1
                    logger.error(f"Kafka error: {e}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing record: {e}")
        
        # Flush and close
        producer.flush()
        producer.close()
        
        elapsed = time.time() - start_time
        logger.info("=" * 80)
        logger.info(f"Streaming complete: {success_count} records sent, {error_count} errors")
        logger.info(f"Time elapsed: {elapsed:.2f}s, Rate: {success_count/elapsed:.2f} msg/s")
        logger.info("=" * 80)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API connection error: {e}")
        raise
    except Exception as e:
        logger.error(f"Streaming error: {e}")
        raise


def monitor_kafka_topic():
    """Monitor Kafka topic health."""
    from kafka import KafkaConsumer
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_TLE,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000
        )
        
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC_TLE)
        logger.info(f"Topic '{KAFKA_TOPIC_TLE}' has {len(partitions) if partitions else 0} partitions")
        
        consumer.close()
        logger.info("Kafka topic health check: OK")
        
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")


# Define the DAG
with DAG(
    'tle_api_to_kafka_streaming',
    default_args=default_args,
    description='Stream TLE data from API to Kafka in real-time',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['space-debris', 'kafka', 'streaming', 'api', 'tle']
) as dag:

    # Task 1: Get API statistics
    stats_task = PythonOperator(
        task_id='get_api_stats',
        python_callable=get_api_stats,
        doc_md="""
        ### Get API Statistics
        Retrieves statistics from the TLE streaming API including
        total records, satellites, and date range.
        """
    )

    # Task 2: Stream from API to Kafka
    stream_task = PythonOperator(
        task_id='stream_api_to_kafka',
        python_callable=stream_api_to_kafka,
        doc_md="""
        ### Stream API to Kafka
        Consumes TLE data from the streaming API and publishes
        enriched records to Kafka topic 'space_debris_tle'.
        Streams up to 1000 records per run at 100x acceleration.
        """
    )

    # Task 3: Monitor Kafka topic
    monitor_task = PythonOperator(
        task_id='monitor_kafka_topic',
        python_callable=monitor_kafka_topic,
        doc_md="""
        ### Monitor Kafka Topic
        Checks Kafka topic health and partition count.
        """
    )

    # Define task dependencies
    stats_task >> stream_task >> monitor_task
