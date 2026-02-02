"""
Airflow DAG for Collision Prediction Pipeline
Orchestrates the Spark job that predicts future positions and detects collisions
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'space-debris-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def check_hdfs_data():
    """Verify that SGP4 vector data exists in HDFS."""
    try:
        import subprocess
        result = subprocess.run(
            ['docker', 'exec', 'spark-master', 
             'hdfs', 'dfs', '-test', '-d', '/space-debris/sgp4_vectors'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("✓ HDFS SGP4 data directory exists")
            return True
        else:
            logger.warning("✗ HDFS SGP4 data directory not found")
            return False
    except Exception as e:
        logger.error(f"HDFS check failed: {e}")
        return False


def check_kafka_connection():
    """Verify Kafka is accessible for publishing collision alerts."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')],
            request_timeout_ms=5000
        )
        producer.close()
        logger.info("✓ Kafka connection successful")
        return True
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        return False


with DAG(
    'collision_prediction_pipeline',
    default_args=default_args,
    description='Predict satellite collisions using SGP4 propagation',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    tags=['collision', 'prediction', 'spark']
) as dag:
    
    # Task 1: Check prerequisites
    check_hdfs = PythonOperator(
        task_id='check_hdfs_data',
        python_callable=check_hdfs_data
    )
    
    check_kafka = PythonOperator(
        task_id='check_kafka_connection',
        python_callable=check_kafka_connection
    )
    
    # Task 2: Submit Spark collision prediction job
    submit_collision_job = BashOperator(
        task_id='submit_collision_prediction',
        bash_command="""
        docker exec spark-master spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            --py-files /opt/spark-apps/pipelines/processing/spark_collision_prediction.py \
            /opt/spark-apps/pipelines/processing/spark_collision_prediction.py
        """
    )
    
    # Task 3: Verify collision data was written
    verify_output = BashOperator(
        task_id='verify_collision_output',
        bash_command="""
        docker exec spark-master hdfs dfs -ls /space-debris/collision_predictions/ | tail -n 5
        """
    )
    
    # Task 4: Log summary statistics
    log_summary = BashOperator(
        task_id='log_summary',
        bash_command="""
        echo "Collision Prediction Pipeline Completed"
        echo "Timestamp: $(date)"
        echo "Check HDFS: hdfs://namenode:9000/space-debris/collision_predictions/"
        echo "Check Kafka topic: space_debris_collisions"
        """
    )
    
    # Define task dependencies
    [check_hdfs, check_kafka] >> submit_collision_job >> verify_output >> log_summary
