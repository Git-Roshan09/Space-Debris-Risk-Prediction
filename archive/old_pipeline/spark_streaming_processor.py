"""
Spark Streaming TLE Processor - Layer 3: Processing
Real-time conjunction candidate calculation using Spark Structured Streaming.

Architecture: Kafka → Spark Streaming → HDFS/Alerts
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, current_timestamp, window,
    sqrt, pow as spark_pow, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, FloatType
)
import os
from dotenv import load_dotenv

# =============================
# CONFIG
# =============================
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC_TLE = os.getenv("KAFKA_TOPIC_TLE", "tle-raw")

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_RPC_PORT", "9000")
HDFS_OUTPUT_PATH = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/space-debris/spark-processed"

# Alert Configuration
CONJUNCTION_THRESHOLD_KM = float(os.getenv("CONJUNCTION_THRESHOLD_KM", 10.0))

# Define schema for incoming Kafka messages
tle_schema = StructType([
    StructField("type", StringType(), True),
    StructField("norad_id", StringType(), True),
    StructField("epoch", StringType(), True),
    StructField("tle_line1", StringType(), True),
    StructField("tle_line2", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

def create_spark_session():
    """Create Spark session with Kafka and HDFS support."""
    return SparkSession.builder \
        .appName("SpaceDebris-RealTime-Processor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .getOrCreate()

def calculate_state_vector(tle_line1, tle_line2, epoch_str):
    """Calculate state vector from TLE using SGP4."""
    try:
        from sgp4.api import Satrec, jday
        from datetime import datetime
        
        if not tle_line1 or not tle_line2 or not epoch_str:
            return (None, None, None, None, None, None)
        
        satellite = Satrec.twoline2rv(tle_line1, tle_line2)
        
        epoch_clean = epoch_str.replace('Z', '').split('+')[0]
        dt = datetime.fromisoformat(epoch_clean)
        
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                      dt.second + dt.microsecond/1e6)
        
        error_code, position, velocity = satellite.sgp4(jd, fr)
        
        if error_code != 0:
            return (None, None, None, None, None, None)
        
        return (
            float(position[0]), float(position[1]), float(position[2]),
            float(velocity[0]), float(velocity[1]), float(velocity[2])
        )
    except Exception:
        return (None, None, None, None, None, None)

# Register UDF
state_vector_schema = StructType([
    StructField("pos_x", DoubleType(), True),
    StructField("pos_y", DoubleType(), True),
    StructField("pos_z", DoubleType(), True),
    StructField("vel_x", DoubleType(), True),
    StructField("vel_y", DoubleType(), True),
    StructField("vel_z", DoubleType(), True)
])

def main():
    print("=" * 60)
    print("🚀 SPARK STREAMING PROCESSOR (Layer 3: Processing)")
    print("=" * 60)
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📂 Topic: {KAFKA_TOPIC_TLE}")
    print(f"💾 Output: Console (for demo)")
    print(f"⚠️  Conjunction threshold: {CONJUNCTION_THRESHOLD_KM} km")
    print("=" * 60 + "\n")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Register UDF
    calculate_sv_udf = udf(calculate_state_vector, state_vector_schema)
    
    print("✅ Spark session created\n")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_TLE) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and calculate state vectors
    processed_df = kafka_df \
        .select(from_json(col("value").cast("string"), tle_schema).alias("data")) \
        .select("data.*") \
        .filter(col("type") == "tle") \
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
        .filter(col("pos_x").isNotNull())
    
    # Calculate orbital radius (distance from Earth center)
    with_radius_df = processed_df.withColumn(
        "orbital_radius_km",
        sqrt(
            spark_pow(col("pos_x"), 2) + 
            spark_pow(col("pos_y"), 2) + 
            spark_pow(col("pos_z"), 2)
        )
    ).withColumn(
        "altitude_km",
        col("orbital_radius_km") - lit(6371.0)  # Earth radius
    )
    
    # Write to console for real-time monitoring
    console_query = with_radius_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("✅ Streaming started!")
    print("📊 Real-time state vectors will appear below...\n")
    print("Press Ctrl+C to stop.\n")
    print("-" * 80)
    
    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping stream...")
        console_query.stop()
        spark.stop()
        print("✅ Stream stopped")

if __name__ == "__main__":
    main()
