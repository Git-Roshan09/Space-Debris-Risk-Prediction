"""
Spark Streaming TLE Consumer - Consumes TLE data from Kafka
Reads TLE records from Kafka, converts to state vectors using SGP4, and saves to HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
from dotenv import load_dotenv

# =============================
# CONFIG
# =============================
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tle-raw")

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "namenode")
HDFS_PORT = os.getenv("HDFS_RPC_PORT", "8020")
HDFS_OUTPUT_PATH = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/space-debris/state-vectors-stream"

# Define schema for incoming Kafka messages
tle_schema = StructType([
    StructField("norad_id", StringType(), True),
    StructField("epoch", StringType(), True),
    StructField("tle_line1", StringType(), True),
    StructField("tle_line2", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

def create_spark_session():
    """Create Spark session with Kafka support."""
    return SparkSession.builder \
        .appName("TLE-Kafka-Consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

def calculate_state_vector(tle_line1, tle_line2, epoch_str):
    """
    Calculate state vector from TLE using SGP4.
    Returns: (pos_x, pos_y, pos_z, vel_x, vel_y, vel_z) in km and km/s
    """
    try:
        from sgp4.api import Satrec, jday
        from datetime import datetime
        import numpy as np
        
        if not tle_line1 or not tle_line2 or not epoch_str:
            return (None, None, None, None, None, None)
        
        # Parse the TLE
        satellite = Satrec.twoline2rv(tle_line1, tle_line2)
        
        # Parse epoch
        dt = datetime.fromisoformat(epoch_str.replace('Z', '+00:00').replace('+00:00', ''))
        
        # Calculate Julian date
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, 
                      dt.second + dt.microsecond/1e6)
        
        # Propagate
        error_code, position, velocity = satellite.sgp4(jd, fr)
        
        if error_code != 0:
            return (None, None, None, None, None, None)
        
        return (
            float(position[0]), float(position[1]), float(position[2]),
            float(velocity[0]), float(velocity[1]), float(velocity[2])
        )
    except Exception as e:
        return (None, None, None, None, None, None)

# Output schema for state vectors
state_vector_schema = StructType([
    StructField("pos_x", DoubleType(), True),
    StructField("pos_y", DoubleType(), True),
    StructField("pos_z", DoubleType(), True),
    StructField("vel_x", DoubleType(), True),
    StructField("vel_y", DoubleType(), True),
    StructField("vel_z", DoubleType(), True)
])

def main():
    print("🚀 Starting Spark Streaming TLE Consumer")
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📂 Topic: {KAFKA_TOPIC}")
    print(f"💾 Output: {HDFS_OUTPUT_PATH}\n")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Register UDF for state vector calculation
    calculate_sv_udf = udf(calculate_state_vector, state_vector_schema)
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON messages
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), tle_schema).alias("data")) \
        .select("data.*")
    
    # Calculate state vectors
    processed_df = parsed_df \
        .withColumn("state_vector", calculate_sv_udf(
            col("tle_line1"), col("tle_line2"), col("epoch")
        )) \
        .select(
            col("norad_id"),
            col("epoch"),
            col("state_vector.pos_x").alias("pos_x"),
            col("state_vector.pos_y").alias("pos_y"),
            col("state_vector.pos_z").alias("pos_z"),
            col("state_vector.vel_x").alias("vel_x"),
            col("state_vector.vel_y").alias("vel_y"),
            col("state_vector.vel_z").alias("vel_z"),
            current_timestamp().alias("processed_at")
        ) \
        .filter(col("pos_x").isNotNull())  # Filter out failed propagations
    
    # Write to console for debugging
    console_query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("✅ Streaming started. Press Ctrl+C to stop.\n")
    
    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping stream...")
        console_query.stop()
        spark.stop()
        print("✅ Stream stopped")

if __name__ == "__main__":
    main()
