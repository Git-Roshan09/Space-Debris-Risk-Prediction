"""
Simplified Spark Streaming: TLE → SGP4 Vectors → HDFS
Focus: Calculate position/velocity vectors and store for time-series analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, current_timestamp, to_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)
from sgp4.api import Satrec, jday
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TLE_to_SGP4_HDFS:
    """
    Streaming Pipeline: Kafka TLE → SGP4 Vectors → HDFS Storage
    Purpose: Build time-series dataset for future analysis
    """
    
    def __init__(self, 
                 kafka_servers='localhost:9092',
                 hdfs_output_path='hdfs://namenode:9000/space-debris/sgp4_vectors',
                 checkpoint_path='hdfs://namenode:9000/tmp/spark-checkpoint-sgp4'):
        """Initialize Spark with Kafka and HDFS configs."""
        
        self.spark = SparkSession.builder \
            .appName("TLE-to-SGP4-HDFS") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", checkpoint_path) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.kafka_servers = kafka_servers
        self.hdfs_output = hdfs_output_path
        self.checkpoint_path = checkpoint_path
        
        logger.info(f"=== SGP4 Vector Computation Pipeline ===")
        logger.info(f"Kafka: {kafka_servers}")
        logger.info(f"HDFS Output: {hdfs_output_path}")
        logger.info(f"Checkpoint: {checkpoint_path}")
    
    def get_tle_schema(self):
        """Schema for TLE data from Kafka topic."""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("satellite_id", StringType(), True),
            StructField("epoch", StringType(), True),
            StructField("tle_line1", StringType(), True),
            StructField("tle_line2", StringType(), True),
            StructField("inclination", DoubleType(), True),
            StructField("raan", DoubleType(), True),
            StructField("eccentricity", StringType(), True),
            StructField("argument_of_perigee", DoubleType(), True),
            StructField("mean_anomaly", DoubleType(), True),
            StructField("mean_motion", DoubleType(), True),
            StructField("revolution_number", IntegerType(), True),
        ])
    
    @staticmethod
    def compute_sgp4_vectors(tle_line1, tle_line2, epoch_str):
        """
        Compute SGP4 position and velocity vectors at epoch time.
        
        Args:
            tle_line1: First line of TLE
            tle_line2: Second line of TLE
            epoch_str: Epoch timestamp string
            
        Returns:
            Tuple: (pos_x, pos_y, pos_z, vel_x, vel_y, vel_z, 
                   altitude_km, velocity_magnitude, error_code)
        """
        try:
            # Initialize satellite from TLE
            satellite = Satrec.twoline2rv(tle_line1, tle_line2)
            
            # Parse epoch time
            epoch = datetime.fromisoformat(epoch_str.replace('+00:00', ''))
            if epoch.tzinfo is None:
                epoch = epoch.replace(tzinfo=timezone.utc)
            
            # Convert to Julian date
            jd, fr = jday(
                epoch.year, epoch.month, epoch.day,
                epoch.hour, epoch.minute, 
                epoch.second + epoch.microsecond / 1e6
            )
            
            # Propagate satellite position
            error_code, position, velocity = satellite.sgp4(jd, fr)
            
            if error_code != 0:
                logger.warning(f"SGP4 error code {error_code} for satellite")
                return (None, None, None, None, None, None, None, None, error_code)
            
            # Extract vectors
            pos_x, pos_y, pos_z = position
            vel_x, vel_y, vel_z = velocity
            
            # Calculate derived metrics
            # Altitude: distance from Earth's center - Earth's radius (6371 km)
            distance_from_center = (pos_x**2 + pos_y**2 + pos_z**2)**0.5
            altitude_km = distance_from_center - 6371.0
            
            # Velocity magnitude (km/s)
            velocity_magnitude = (vel_x**2 + vel_y**2 + vel_z**2)**0.5
            
            return (
                float(pos_x), float(pos_y), float(pos_z),
                float(vel_x), float(vel_y), float(vel_z),
                float(altitude_km),
                float(velocity_magnitude),
                int(error_code)
            )
            
        except Exception as e:
            logger.error(f"SGP4 computation error: {str(e)}")
            return (None, None, None, None, None, None, None, None, -1)
    
    def start_streaming(self, output_mode="append"):
        """
        Start the streaming pipeline.
        
        Args:
            output_mode: "append" for continuous writes, "complete" for aggregations
        """
        
        logger.info("Starting Kafka consumer...")
        
        # Step 1: Read from Kafka
        kafka_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "space_debris_tle") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("✓ Connected to Kafka topic: space_debris_tle")
        
        # Step 2: Parse JSON from Kafka
        tle_data = kafka_stream.select(
            from_json(
                col("value").cast("string"), 
                self.get_tle_schema()
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        logger.info("✓ Parsing TLE data from Kafka messages")
        
        # Step 3: Register SGP4 UDF
        sgp4_udf = udf(
            self.compute_sgp4_vectors,
            StructType([
                StructField("position_x", DoubleType(), True),
                StructField("position_y", DoubleType(), True),
                StructField("position_z", DoubleType(), True),
                StructField("velocity_x", DoubleType(), True),
                StructField("velocity_y", DoubleType(), True),
                StructField("velocity_z", DoubleType(), True),
                StructField("altitude_km", DoubleType(), True),
                StructField("velocity_magnitude_kms", DoubleType(), True),
                StructField("sgp4_error_code", IntegerType(), True),
            ])
        )
        
        # Step 4: Compute SGP4 vectors
        vectors_df = tle_data.withColumn(
            "sgp4_result",
            sgp4_udf(col("tle_line1"), col("tle_line2"), col("epoch"))
        ).select(
            # Original TLE metadata
            col("message_id"),
            col("satellite_id"),
            to_timestamp(col("epoch")).alias("epoch_time"),
            col("kafka_timestamp"),
            current_timestamp().alias("processing_time"),
            
            # Orbital elements from TLE
            col("inclination"),
            col("raan"),
            col("eccentricity"),
            col("argument_of_perigee"),
            col("mean_anomaly"),
            col("mean_motion"),
            col("revolution_number"),
            
            # SGP4 computed vectors
            col("sgp4_result.position_x"),
            col("sgp4_result.position_y"),
            col("sgp4_result.position_z"),
            col("sgp4_result.velocity_x"),
            col("sgp4_result.velocity_y"),
            col("sgp4_result.velocity_z"),
            col("sgp4_result.altitude_km"),
            col("sgp4_result.velocity_magnitude_kms"),
            col("sgp4_result.sgp4_error_code")
        ).filter(col("sgp4_error_code") == 0)  # Only successful calculations
        
        logger.info("✓ Configured SGP4 vector computation")
        
        # Step 5: Write to HDFS in Parquet format (columnar, compressed)
        # Partition by date for efficient time-series queries
        hdfs_query = vectors_df \
            .writeStream \
            .outputMode(output_mode) \
            .format("parquet") \
            .option("path", self.hdfs_output) \
            .option("checkpointLocation", f"{self.checkpoint_path}/hdfs") \
            .partitionBy("epoch_time") \
            .start()
        
        logger.info(f"✓ Writing SGP4 vectors to HDFS: {self.hdfs_output}")
        logger.info("  Format: Parquet (columnar, compressed)")
        logger.info("  Partitioning: By epoch_time for efficient queries")
        
        # Step 6: Console output for monitoring
        console_query = vectors_df \
            .select(
                "satellite_id", "epoch_time", "altitude_km", 
                "velocity_magnitude_kms", "position_x", "position_y", "position_z"
            ) \
            .writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 3) \
            .start()
        
        logger.info("✓ Console monitoring enabled")
        
        # Display streaming stats
        logger.info("\n" + "="*60)
        logger.info("STREAMING PIPELINE ACTIVE")
        logger.info("="*60)
        logger.info(f"Input:  Kafka topic 'space_debris_tle'")
        logger.info(f"Output: {self.hdfs_output}")
        logger.info(f"Status: Processing TLE → SGP4 vectors → HDFS")
        logger.info("="*60 + "\n")
        
        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("\nStopping streaming pipeline...")
            self.spark.streams.active[0].stop()
            logger.info("✓ Pipeline stopped gracefully")


def main():
    """Run the streaming pipeline with command-line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Stream TLE data from Kafka, compute SGP4 vectors, store in HDFS'
    )
    parser.add_argument(
        '--kafka',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--hdfs-path',
        default='/space-debris/sgp4_vectors',
        help='HDFS output path (default: /space-debris/sgp4_vectors)'
    )
    parser.add_argument(
        '--checkpoint',
        default='/tmp/spark-checkpoint-sgp4',
        help='Checkpoint directory (default: /tmp/spark-checkpoint-sgp4)'
    )
    
    args = parser.parse_args()
    
    # Initialize and start pipeline
    pipeline = TLE_to_SGP4_HDFS(
        kafka_servers=args.kafka,
        hdfs_output_path=args.hdfs_path,
        checkpoint_path=args.checkpoint
    )
    
    pipeline.start_streaming()


if __name__ == "__main__":
    main()
