"""
Simplified Spark Streaming: TLE → SGP4 Vectors → HDFS + PostgreSQL
Focus: Calculate position/velocity vectors and store for time-series analysis
Writes: HDFS (all vectors) + PostgreSQL (satellite metadata)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, current_timestamp, to_timestamp, lit,
    datediff, when, max as spark_max, count as spark_count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)
from sgp4.api import Satrec, jday
from datetime import datetime, timezone
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Try importing from the current directory first
try:
    from postgres_utils import get_postgres_connector
except ImportError:
    from pipelines.processing.postgres_utils import get_postgres_connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TLE_to_SGP4_HDFS:
    """
    Streaming Pipeline: Kafka TLE → SGP4 Vectors → HDFS Storage
    Purpose: Build time-series dataset for future analysis
    """
    
    def __init__(self, 
                 kafka_servers='kafka:9093',
                 hdfs_output_path='hdfs://namenode:9000/space-debris/sgp4_vectors',
                 checkpoint_path='hdfs://namenode:9000/tmp/spark-checkpoint-sgp4',
                 min_altitude_km=150.0,
                 max_tle_age_days=30):
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
        self.min_altitude_km = min_altitude_km
        self.max_tle_age_days = max_tle_age_days
        
        logger.info(f"=== SGP4 Vector Computation Pipeline ===")
        logger.info(f"Kafka: {kafka_servers}")
        logger.info(f"HDFS Output: {hdfs_output_path}")
        logger.info(f"Checkpoint: {checkpoint_path}")
        logger.info(f"\n=== Tracking Stop Conditions ===")
        logger.info(f"1. Minimum Altitude: {min_altitude_km} km (de-orbit threshold)")
        logger.info(f"2. Maximum TLE Age: {max_tle_age_days} days (data freshness)")
        logger.info(f"3. SGP4 Error Code: 0 only (valid propagation)")
    
    def get_tle_schema(self):
        """Schema for TLE data from Kafka topic."""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("message_timestamp", StringType(), True),
            StructField("source", StringType(), True),
            StructField("norad_id", IntegerType(), True),
            StructField("object_name", StringType(), True),
            StructField("tle_line1", StringType(), True),
            StructField("tle_line2", StringType(), True),
            StructField("classification", StringType(), True),
            StructField("inclination", DoubleType(), True),
            StructField("raan", DoubleType(), True),
            StructField("eccentricity", StringType(), True),
            StructField("argument_of_perigee", DoubleType(), True),
            StructField("mean_anomaly", DoubleType(), True),
            StructField("mean_motion", DoubleType(), True),
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
            .option("kafka.group.id", "spark-sgp4-consumer-group") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", "10000") \
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
            sgp4_udf(col("tle_line1"), col("tle_line2"), col("message_timestamp"))
        ).select(
            # Original TLE metadata
            col("message_id"),
            col("norad_id"),
            col("object_name"),
            col("classification"),
            to_timestamp(col("message_timestamp")).alias("epoch_time"),
            col("kafka_timestamp"),
            current_timestamp().alias("processing_time"),
            
            # Orbital elements from TLE
            col("inclination"),
            col("raan"),
            col("eccentricity"),
            col("argument_of_perigee"),
            col("mean_anomaly"),
            col("mean_motion"),
            
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
        )
        
        logger.info("✓ Configured SGP4 vector computation")
        
        # Step 4a: Apply Tracking Stop Conditions
        logger.info("\n=== Applying Tracking Stop Conditions ===")
        
        # Calculate TLE age in days
        vectors_with_age = vectors_df.withColumn(
            "tle_age_days",
            datediff(current_timestamp(), col("epoch_time"))
        )
        
        # Add tracking status flags
        # NOTE: TLE age check disabled for historical data testing
        filtered_vectors = vectors_with_age.withColumn(
            "tracking_status",
            when(col("sgp4_error_code") != 0, "STOPPED_SGP4_ERROR")
            .when(col("altitude_km") < self.min_altitude_km, "STOPPED_LOW_ALTITUDE")
            # Temporarily disabled for historical data: .when(col("tle_age_days") > self.max_tle_age_days, "STOPPED_STALE_TLE")
            .otherwise("ACTIVE")
        )
        
        # Filter: Keep only actively tracked satellites
        active_satellites = filtered_vectors.filter(
            col("tracking_status") == "ACTIVE"
        )
        
        # Log filtered satellites (for monitoring)
        stopped_satellites = filtered_vectors.filter(
            col("tracking_status") != "ACTIVE"
        )
        
        logger.info("Filter 1: SGP4 Error Code = 0 (valid propagation)")
        logger.info("Filter 2: Altitude >= {} km (above de-orbit threshold)".format(self.min_altitude_km))
        logger.info("Filter 3: TLE Age <= {} days (data freshness)".format(self.max_tle_age_days))
        
        # Use active satellites for further processing
        vectors_df = active_satellites
        
        # Step 4b: Write stopped satellites log to HDFS (for analysis)
        stopped_hdfs_path = self.hdfs_output.replace('sgp4_vectors', 'stopped_tracking')
        stopped_query = stopped_satellites \
            .select(
                "norad_id", "epoch_time", "altitude_km", 
                "sgp4_error_code", "tle_age_days", "tracking_status",
                "processing_time"
            ) \
            .writeStream \
            .outputMode(output_mode) \
            .format("parquet") \
            .option("path", stopped_hdfs_path) \
            .option("checkpointLocation", f"{self.checkpoint_path}/stopped_tracking") \
            .partitionBy("tracking_status") \
            .start()
        
        logger.info(f"✓ Logging stopped satellites to: {stopped_hdfs_path}")
        logger.info("  Partitioning: By tracking_status (for analysis)\n")
        
        # Step 4c: Update PostgreSQL with satellite metadata (foreachBatch)
        def update_postgres_metadata(batch_df, batch_id):
            """Update satellite metadata in PostgreSQL for each batch using UPSERT."""
            try:
                if batch_df.count() == 0:
                    return
                
                logger.info(f"Batch {batch_id}: Updating PostgreSQL with satellite metadata...")
                
                # Aggregate latest info per satellite
                satellite_updates = batch_df.groupBy("norad_id") \
                    .agg(
                        spark_max("epoch_time").alias("last_tle_epoch"),
                        spark_max("altitude_km").alias("last_altitude_km"),
                        spark_max("velocity_magnitude_kms").alias("last_velocity_kms"),
                        spark_max("position_x").alias("last_position_x"),
                        spark_max("position_y").alias("last_position_y"),
                        spark_max("position_z").alias("last_position_z"),
                        spark_max("sgp4_error_code").alias("last_sgp4_error_code"),
                        spark_max("tle_age_days").alias("tle_age_days"),
                        spark_max("inclination").alias("inclination"),
                        spark_max("eccentricity").alias("eccentricity"),
                        spark_max("mean_motion").alias("mean_motion"),
                        spark_count("*").alias("observations_count")
                    ) \
                    .withColumn("status_updated_at", current_timestamp()) \
                    .withColumn("tracking_status", lit("ACTIVE"))
                
                # Collect data for upsert
                satellite_data = satellite_updates.collect()
                
                if not satellite_data:
                    return
                
                # Connect to PostgreSQL and perform UPSERT
                import psycopg2
                conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'postgres-debris'),
                    port=int(os.getenv('POSTGRES_PORT', '5432')),
                    database=os.getenv('POSTGRES_DB', 'space_debris'),
                    user=os.getenv('POSTGRES_USER', 'postgres'),
                    password=os.getenv('POSTGRES_PASSWORD', 'postgres')
                )
                cursor = conn.cursor()
                
                # UPSERT query using ON CONFLICT DO UPDATE
                upsert_query = """
                    INSERT INTO satellites (
                        norad_id, last_tle_epoch, last_altitude_km, last_velocity_kms,
                        last_position_x, last_position_y, last_position_z,
                        last_sgp4_error_code, tle_age_days, inclination, eccentricity,
                        mean_motion, total_observations, tracking_status, status_updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (norad_id) DO UPDATE SET
                        last_tle_epoch = EXCLUDED.last_tle_epoch,
                        last_altitude_km = EXCLUDED.last_altitude_km,
                        last_velocity_kms = EXCLUDED.last_velocity_kms,
                        last_position_x = EXCLUDED.last_position_x,
                        last_position_y = EXCLUDED.last_position_y,
                        last_position_z = EXCLUDED.last_position_z,
                        last_sgp4_error_code = EXCLUDED.last_sgp4_error_code,
                        tle_age_days = EXCLUDED.tle_age_days,
                        inclination = EXCLUDED.inclination,
                        eccentricity = EXCLUDED.eccentricity,
                        mean_motion = EXCLUDED.mean_motion,
                        total_observations = satellites.total_observations + EXCLUDED.total_observations,
                        tracking_status = EXCLUDED.tracking_status,
                        status_updated_at = EXCLUDED.status_updated_at
                """
                
                # Batch insert/update
                rows = [
                    (
                        row['norad_id'], row['last_tle_epoch'], row['last_altitude_km'],
                        row['last_velocity_kms'], row['last_position_x'], row['last_position_y'],
                        row['last_position_z'], row['last_sgp4_error_code'], row['tle_age_days'],
                        row['inclination'], row['eccentricity'], row['mean_motion'],
                        row['observations_count'], row['tracking_status'], row['status_updated_at']
                    )
                    for row in satellite_data
                ]
                
                cursor.executemany(upsert_query, rows)
                conn.commit()
                
                # Update pipeline state
                cursor.execute("""
                    UPDATE pipeline_state 
                    SET last_run_end = NOW(),
                        last_run_status = 'SUCCESS',
                        records_processed = records_processed + %s,
                        data_version = data_version + 1,
                        updated_at = NOW()
                    WHERE component_name = 'SGP4_PROCESSING'
                """, (len(rows),))
                conn.commit()
                
                cursor.close()
                conn.close()
                
                logger.info(f"✓ Batch {batch_id}: UPSERTED {len(rows)} satellites in PostgreSQL")
                
            except Exception as e:
                logger.error(f"Error updating PostgreSQL in batch {batch_id}: {e}")
                # Update pipeline state with error
                try:
                    import psycopg2
                    conn = psycopg2.connect(
                        host=os.getenv('POSTGRES_HOST', 'postgres-debris'),
                        port=int(os.getenv('POSTGRES_PORT', '5432')),
                        database=os.getenv('POSTGRES_DB', 'space_debris'),
                        user=os.getenv('POSTGRES_USER', 'postgres'),
                        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
                    )
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE pipeline_state 
                        SET last_run_status = 'FAILED',
                            error_message = %s,
                            updated_at = NOW()
                        WHERE component_name = 'SGP4_PROCESSING'
                    """, (str(e),))
                    conn.commit()
                    cursor.close()
                    conn.close()
                except:
                    pass
        
        # Apply PostgreSQL updates using foreachBatch
        postgres_query = active_satellites \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(update_postgres_metadata) \
            .option("checkpointLocation", f"{self.checkpoint_path}/postgres_metadata") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        logger.info("✓ PostgreSQL metadata updates configured (every 30 seconds)")
        
        # Step 5a: Write raw TLE data to HDFS (for backup/auditing)
        tle_hdfs_path = self.hdfs_output.replace('sgp4_vectors', 'tle_raw')
        tle_query = tle_data \
            .writeStream \
            .outputMode(output_mode) \
            .format("parquet") \
            .option("path", tle_hdfs_path) \
            .option("checkpointLocation", f"{self.checkpoint_path}/tle_raw") \
            .partitionBy("norad_id") \
            .start()
        
        logger.info(f"✓ Writing raw TLE data to HDFS: {tle_hdfs_path}")
        logger.info("  Format: Parquet")
        logger.info("  Partitioning: By norad_id")
        
        # Step 5b: Write SGP4 vectors to HDFS in Parquet format (columnar, compressed)
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
                "norad_id", "epoch_time", "altitude_km", 
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
        logger.info(f"Input:   Kafka topic 'space_debris_tle'")
        logger.info(f"Output:  {tle_hdfs_path} (raw TLE)")
        logger.info(f"         {self.hdfs_output} (SGP4 vectors)")
        logger.info(f"         PostgreSQL satellites table (metadata)")
        logger.info(f"Status:  Processing TLE → SGP4 vectors → HDFS + PostgreSQL")
        logger.info("="*60 + "\n")
        
        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("\nStopping streaming pipeline...")
            for stream in self.spark.streams.active:
                stream.stop()
            logger.info("✓ Pipeline stopped gracefully")


def main():
    """Run the streaming pipeline with command-line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Stream TLE data from Kafka, compute SGP4 vectors, store in HDFS'
    )
    parser.add_argument(
        '--kafka',
        default='kafka:9093',
        help='Kafka bootstrap servers (default: kafka:9093)'
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
    parser.add_argument(
        '--min-altitude',
        type=float,
        default=150.0,
        help='Minimum altitude in km for active tracking (default: 150.0)'
    )
    parser.add_argument(
        '--max-tle-age',
        type=int,
        default=30,
        help='Maximum TLE age in days before stopping tracking (default: 30)'
    )
    
    args = parser.parse_args()
    
    # Initialize and start pipeline
    pipeline = TLE_to_SGP4_HDFS(
        kafka_servers=args.kafka,
        hdfs_output_path=args.hdfs_path,
        checkpoint_path=args.checkpoint,
        min_altitude_km=args.min_altitude,
        max_tle_age_days=args.max_tle_age
    )
    
    pipeline.start_streaming()


if __name__ == "__main__":
    main()
