#!/bin/bash
# Spark Job 2: Collision Prediction (Batch)
# Reads SGP4 vectors from HDFS, predicts collisions, writes to HDFS/Kafka

echo '=============================================='
echo 'SPARK JOB 2: Collision Prediction (Batch)'
echo '=============================================='
echo 'Input:  HDFS /space-debris/sgp4_vectors'
echo 'Output: HDFS /space-debris/collision_predictions'
echo '        Kafka topic space_debris_collisions (alerts)'
echo ''
echo 'Schedule: Every 10 seconds (DEMO MODE)'
echo ''

# Wait for SGP4 data to be available
echo 'Waiting for initial data (60s)...'
sleep 60

# Run collision prediction periodically
while true; do
  echo ''
  echo ">>> Running collision prediction at $(date)"
  
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --total-executor-cores 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/spark-apps/processing/spark_collision_prediction.py
  
  echo ">>> Collision prediction completed at $(date)"
  echo '>>> Next run in 10 seconds (DEMO)...'
  
  # Sleep for 10 seconds (DEMO MODE)
  sleep 10
done
