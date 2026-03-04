"""
TLE Stream Processor - Spark Structured Streaming
Consumes TLE data from Kafka, converts to State Vectors using SGP4, writes to HDFS.

Flow: Kafka (tle-raw) → Spark Streaming → HDFS (state-vectors)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)
import os
from dotenv import load_dotenv

# =============================
# CONFIG
# =============================
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tle-raw")

# HDFS Configuration (WebHDFS for output path reference)
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_OUTPUT_PATH = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/space-debris-webhdfs/state-vectors-stream"

# For local testing, use local path
LOCAL_OUTPUT_PATH = "Output/state_vectors_stream"
USE_HDFS = os.getenv("USE_HDFS", "false").lower() == "true"

# Checkpoint location for streaming
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/spark-checkpoints/tle-processor")

# Define schema for Kafka messages
kafka_schema = StructType([
    StructField("norad_id", StringType(), True),
    StructField("epoch", StringType(), True),
    StructField("tle_line1", StringType(), True),
    StructField("tle_line2", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

def create_spark_session():
    """Create Spark session with Kafka support."""
    return SparkSession.builder \
        .appName("TLE-Stream-Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

def sgp4_propagate(tle_line1, tle_line2):
    """
    Calculate state vector from TLE using SGP4.
    Returns [pos_x, pos_y, pos_z, vel_x, vel_y, vel_z] in km and km/s
    """
    try:
        from sgp4.api import Satrec
        
        if not tle_line1 or not tle_line2:
            return None
        
        # Parse TLE
        satellite = Satrec.twoline2rv(tle_line1, tle_line2)
        
        # Get TLE epoch (Julian date)
        jd = satellite.jdsatepoch
        fr = satellite.jdsatepochF
        
        # Propagate at TLE epoch
        error_code, position, velocity = satellite.sgp4(jd, fr)
        
        if error_code != 0:
            return None
        
        return [
            round(position[0], 6),  # pos_x (km)
            round(position[1], 6),  # pos_y (km)
            round(position[2], 6),  # pos_z (km)
            round(velocity[0], 9),  # vel_x (km/s)
            round(velocity[1], 9),  # vel_y (km/s)
            round(velocity[2], 9)   # vel_z (km/s)
        ]
    except Exception as e:
        return None

# Register UDF
sgp4_udf = udf(sgp4_propagate, ArrayType(DoubleType()))

def process_batch(batch_df, batch_id):
    """Process each micro-batch: calculate state vectors and write to storage."""
    if batch_df.isEmpty():
        return
    
    print(f"\n📦 Processing batch {batch_id} with {batch_df.count()} records...")
    
    # Calculate state vectors
    processed = batch_df \
        .withColumn("state_vector", sgp4_udf(col("tle_line1"), col("tle_line2"))) \
        .filter(col("state_vector").isNotNull()) \
        .select(
            col("norad_id"),
            col("epoch"),
            col("state_vector")[0].alias("pos_x"),
            col("state_vector")[1].alias("pos_y"),
            col("state_vector")[2].alias("pos_z"),
            col("state_vector")[3].alias("vel_x"),
            col("state_vector")[4].alias("vel_y"),
            col("state_vector")[5].alias("vel_z"),
            current_timestamp().alias("processed_at")
        )
    
    count = processed.count()
    
    if count > 0:
        # Write to output
        output_path = HDFS_OUTPUT_PATH if USE_HDFS else LOCAL_OUTPUT_PATH
        
        processed.write \
            .mode("append") \
            .option("header", "true") \
            .partitionBy("norad_id") \
            .csv(output_path)
        
        print(f"   ✅ Wrote {count} state vectors to {output_path}")
    else:
        print(f"   ⚠️ No valid state vectors in this batch")

def main():
    print("=" * 70)
    print("🚀 TLE STREAM PROCESSOR (Spark Structured Streaming)")
    print("=" * 70)
    print(f"📥 Source:      Kafka ({KAFKA_BOOTSTRAP_SERVERS})")
    print(f"📂 Topic:       {KAFKA_TOPIC}")
    print(f"📤 Output:      {'HDFS: ' + HDFS_OUTPUT_PATH if USE_HDFS else 'Local: ' + LOCAL_OUTPUT_PATH}")
    print(f"📍 Checkpoint:  {CHECKPOINT_PATH}")
    print("=" * 70 + "\n")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created")
    
    # Read from Kafka
    print("📡 Connecting to Kafka...")
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON messages
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), kafka_schema).alias("data")) \
        .select("data.*")
    
    print("✅ Connected to Kafka")
    print("\n⏳ Waiting for TLE data... (Press Ctrl+C to stop)\n")
    print("-" * 70)
    
    # Process stream using foreachBatch
    query = parsed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping stream...")
        query.stop()
        spark.stop()
        print("✅ Stream stopped")

if __name__ == "__main__":
    main()
