"""
Airflow DAG to manage Spark Streaming job for SGP4 vector computation
Submits a long-running Spark streaming job that reads from Kafka and writes to HDFS
"""

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'space-debris-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Configuration
SPARK_MASTER_URL = 'spark://spark-master:7077'
SPARK_APP_PATH = '/opt/spark-apps/processing/spark_sgp4_to_hdfs.py'
KAFKA_SERVERS = 'kafka:9093'


def check_kafka_topic():
    """Verify Kafka topic exists before starting Spark job."""
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
            logger.warning("✗ Kafka topic 'space_debris_tle' not found")
            return False
    except Exception as e:
        logger.error(f"Error checking Kafka topic: {str(e)}")
        return False


def check_hdfs_namenode():
    """Check if HDFS NameNode is accessible."""
    try:
        response = requests.get('http://namenode:9870', timeout=5)
        if response.status_code == 200:
            logger.info("✓ HDFS NameNode is accessible")
            return True
        else:
            logger.warning(f"✗ HDFS NameNode returned status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error checking HDFS: {str(e)}")
        return False


# Define the DAG
with DAG(
    dag_id='spark_sgp4_streaming',
    default_args=default_args,
    description='Spark Streaming: Kafka TLE → SGP4 Vectors → HDFS',
    schedule_interval=None,  # Manual trigger only (long-running job)
    catchup=False,
    tags=['spark', 'streaming', 'sgp4', 'hdfs'],
) as dag:

    # Task 1: Verify Kafka is ready and topic exists
    check_kafka = PythonOperator(
        task_id='check_kafka_topic',
        python_callable=check_kafka_topic,
    )

    # Task 2: Verify HDFS is ready
    check_hdfs = PythonOperator(
        task_id='check_hdfs_namenode',
        python_callable=check_hdfs_namenode,
    )

    # Task 3: Submit Spark Streaming Job
    spark_submit_cmd = f"""docker exec spark-master /opt/spark/bin/spark-submit \\
        --master {SPARK_MASTER_URL} \\
        --deploy-mode client \\
        --name "SGP4-Vector-Computation" \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        --conf spark.executor.memory=2g \\
        --conf spark.executor.cores=2 \\
        --conf spark.driver.memory=1g \\
        --conf spark.sql.streaming.checkpointLocation=hdfs://namenode:9000/tmp/spark-checkpoint-sgp4 \\
        {SPARK_APP_PATH} \\
        --kafka {KAFKA_SERVERS}"""

    submit_spark_job = BashOperator(
        task_id='submit_spark_streaming_job',
        bash_command=spark_submit_cmd,
        execution_timeout=None,  # Long-running streaming job
    )

    # Set task dependencies
    [check_kafka, check_hdfs] >> submit_spark_job


if __name__ == "__main__":
    dag.test()
