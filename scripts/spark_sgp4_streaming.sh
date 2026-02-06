#!/bin/bash
# Spark Job 1: SGP4 Vector Computation (Streaming)
# Consumes TLE from Kafka, computes SGP4 vectors, writes to HDFS

echo '=============================================='
echo 'SPARK JOB 1: SGP4 Vector Computation (Streaming)'
echo '=============================================='
echo 'Input:  Kafka topic space_debris_tle'
echo 'Output: HDFS /space-debris/sgp4_vectors'
echo ''
echo 'Waiting for services to be ready (60s)...'
sleep 60

echo 'Submitting Spark Streaming job...'
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --total-executor-cores 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.streaming.checkpointLocation=hdfs://namenode:9000/tmp/spark-checkpoint-sgp4 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /opt/spark-apps/processing/spark_sgp4_to_hdfs.py \
  --kafka "${KAFKA_SERVERS:-kafka:9093}" \
  --hdfs-path "${HDFS_OUTPUT:-hdfs://namenode:9000/space-debris/sgp4_vectors}" \
  --checkpoint "${CHECKPOINT_PATH:-hdfs://namenode:9000/tmp/spark-checkpoint-sgp4}" \
  --min-altitude "${MIN_ALTITUDE_KM:-150.0}" \
  --max-tle-age "${MAX_TLE_AGE_DAYS:-30}"
