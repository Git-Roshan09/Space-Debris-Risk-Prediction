#!/bin/bash
# Wrapper script to submit Spark job from outside the Spark container
# This script should be run from the host or from Airflow with docker access

SPARK_APP="/opt/spark-apps/processing/spark_sgp4_to_hdfs.py"

echo "Submitting Spark Streaming job..."

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name 'SGP4-Vector-Computation' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.driver.memory=1g \
  $SPARK_APP

echo "Spark job submitted with exit code: $?"
