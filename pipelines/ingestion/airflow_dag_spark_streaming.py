"""
Airflow DAG to manage Spark Streaming job for SGP4 vector computation
Submits Spark job that reads from Kafka and writes to HDFS
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'space-debris-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Configuration
KAFKA_SERVERS = 'kafka:9093'


def check_kafka_topic():
    """Verify Kafka topic exists."""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_SERVERS],
            consumer_timeout_ms=5000
        )
        topics = consumer.topics()
        consumer.close()
        
        if 'space_debris_tle' in topics:
            logger.info("✓ Kafka topic 'space_debris_tle' exists")
            return True
        else:
            logger.warning("✗ Kafka topic not found")
            return False
    except Exception as e:
        logger.error(f"Kafka check failed: {e}")
        return False


def check_hdfs_namenode():
    """Check if HDFS NameNode is accessible."""
    try:
        response = requests.get('http://namenode:9870', timeout=5)
        if response.status_code == 200:
            logger.info("✓ HDFS NameNode accessible")
            return True
        else:
            logger.warning(f"✗ HDFS returned {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"HDFS check failed: {e}")
        return False


# Define the DAG
with DAG(
    dag_id='spark_sgp4_streaming',
    default_args=default_args,
    description='Spark Streaming: Kafka TLE → SGP4 Vectors → HDFS',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'streaming', 'sgp4', 'hdfs'],
) as dag:

    check_kafka = PythonOperator(
        task_id='check_kafka_topic',
        python_callable=check_kafka_topic,
    )

    check_hdfs = PythonOperator(
        task_id='check_hdfs_namenode',
        python_callable=check_hdfs_namenode,
    )

    submit_spark_job = BashOperator(
        task_id='submit_spark_streaming_job',
        bash_command='python3 /opt/airflow/config/submit_spark_wrapper.sh ',
        doc_md="""
        Submits Spark Streaming job via HTTP trigger:
        - Reads TLE data from Kafka topic 'space_debris_tle'
        - Computes SGP4 position/velocity vectors
        - Writes to HDFS: hdfs://namenode:9000/space-debris/
        
        Uses HTTP trigger service on Spark master (port 6066).
        """
    )

    [check_kafka, check_hdfs] >> submit_spark_job
