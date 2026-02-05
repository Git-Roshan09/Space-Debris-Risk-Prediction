"""
Airflow DAG for TLE Data Ingestion ONLY
This DAG ONLY handles data ingestion from the TLE API to Kafka.

NOTE: Spark Streaming and Kafka consumers run separately as standalone services.
      Airflow is NOT used to orchestrate Spark or Kafka - only for API ingestion.
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
TLE_API_BASE_URL = 'http://tle-api:5000' 
KAFKA_BOOTSTRAP_SERVERS = ['broker:29092', 'kafka:9093']
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


def check_api_health():
    """Check if the TLE API is healthy and accessible."""
    try:
        response = requests.get(f"{TLE_API_BASE_URL}/health", timeout=30)
        response.raise_for_status()
        
        health = response.json()
        logger.info(f"✓ TLE API is healthy: {health}")
        return True
        
    except Exception as e:
        logger.error(f"✗ TLE API health check failed: {e}")
        return False


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
    """
    Stream TLE data from the Flask API to Kafka.
    This is the ONLY job Airflow should handle - data ingestion.
    """
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    
    logger.info("=" * 60)
    logger.info("Starting TLE API → Kafka Ingestion")
    logger.info("=" * 60)
    
    # Try multiple Kafka servers
    kafka_connected = False
    producer = None
    
    for servers in [['kafka:9093'], ['broker:29092']]:
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                max_block_ms=5000,
                acks='all',
                compression_type='gzip'
            )
            logger.info(f"✓ Connected to Kafka: {servers}")
            kafka_connected = True
            break
        except Exception as e:
            logger.warning(f"Could not connect to {servers}: {e}")
    
    if not kafka_connected or producer is None:
        raise RuntimeError("Could not connect to any Kafka server")
    
    # Build stream URL
    stream_url = f"{TLE_API_BASE_URL}/stream"
    params = {
        'acceleration': STREAM_ACCELERATION,
        'mode': STREAM_MODE,
        'max_delay': MAX_DELAY,
        'limit': STREAM_LIMIT
    }
    
    logger.info(f"Streaming from: {stream_url}")
    logger.info(f"Parameters: {params}")
    
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
                        logger.info(f"Progress: {success_count} records sent to Kafka")
                        
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
        logger.info("=" * 60)
        logger.info(f"✓ Ingestion complete!")
        logger.info(f"  Records sent: {success_count}")
        logger.info(f"  Errors: {error_count}")
        logger.info(f"  Duration: {elapsed:.2f}s")
        logger.info(f"  Rate: {success_count/elapsed:.2f} msg/s")
        logger.info("=" * 60)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API connection error: {e}")
        raise
    except Exception as e:
        logger.error(f"Streaming error: {e}")
        raise


# Define the DAG - ONLY for data ingestion
with DAG(
    'tle_data_ingestion',
    default_args=default_args,
    description='Ingest TLE data from API to Kafka (Airflow only handles ingestion)',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    tags=['space-debris', 'kafka', 'ingestion', 'tle'],
    doc_md="""
    ## TLE Data Ingestion DAG
    
    This DAG handles **ONLY** data ingestion from the TLE API to Kafka.
    
    ### Architecture Note:
    - **Airflow** → Data Ingestion (API → Kafka)
    - **Spark Streaming** → Runs separately as a standalone service
    - **Collision Prediction** → Runs separately (batch or streaming)
    
    Spark and Kafka consumers are NOT orchestrated by Airflow.
    They run as independent Docker services.
    """
) as dag:

    # Task 1: Check API health
    health_check = PythonOperator(
        task_id='check_api_health',
        python_callable=check_api_health,
        doc_md="Check if the TLE streaming API is accessible"
    )

    # Task 2: Get API statistics  
    stats_task = PythonOperator(
        task_id='get_api_stats',
        python_callable=get_api_stats,
        doc_md="Get statistics about the TLE dataset"
    )

    # Task 3: Stream data from API to Kafka
    ingest_task = PythonOperator(
        task_id='ingest_api_to_kafka',
        python_callable=stream_api_to_kafka,
        doc_md="""
        Stream TLE data from the API to Kafka.
        This is the core ingestion task - the ONLY processing Airflow handles.
        """
    )

    # Define task dependencies
    health_check >> stats_task >> ingest_task
