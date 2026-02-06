"""
Spark Job: Collision Prediction System
Reads SGP4 vectors from HDFS and detects potential collisions based on position proximity.
Writes results to HDFS (historical), Kafka (streaming), and PostgreSQL (dashboard).
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, sqrt, pow as spark_pow,
    when, broadcast, max as spark_max, count, coalesce
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from datetime import datetime, timedelta, timezone
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres-debris'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'space_debris'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}


class CollisionPredictionEngine:
    """
    Simplified Collision Detection Pipeline:
    Read SGP4 vectors → Detect close approaches → Output collision alerts
    """
    
    def __init__(self):
        """Initialize Spark with configuration from environment."""
        
        # Load configuration from environment
        self.collision_threshold_km = float(os.getenv('COLLISION_THRESHOLD_KM', '10.0'))
        self.time_window_days = int(os.getenv('TIME_WINDOW_DAYS', '7'))
        
        self.hdfs_input = os.getenv('HDFS_SGP4_VECTORS_PATH', 
                                     'hdfs://namenode:9000/space-debris/sgp4_vectors')
        self.hdfs_output = os.getenv('HDFS_COLLISION_PREDICTIONS_PATH',
                                      'hdfs://namenode:9000/space-debris/collision_predictions')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
        self.kafka_topic = os.getenv('KAFKA_COLLISION_TOPIC', 'space_debris_collisions')
        
        self.spark = SparkSession.builder \
            .appName("Collision-Prediction-Engine") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # PostgreSQL JDBC URL
        self.postgres_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        self.postgres_properties = {
            "user": POSTGRES_CONFIG['user'],
            "password": POSTGRES_CONFIG['password'],
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("=" * 60)
        logger.info("=== Collision Prediction Engine Initialized ===")
        logger.info(f"Collision Threshold: {self.collision_threshold_km} km")
        logger.info(f"Time Window: {self.time_window_days} days")
        logger.info(f"Input: {self.hdfs_input}")
        logger.info(f"Output: {self.hdfs_output}")
        logger.info(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
        logger.info("=" * 60)
    
    def read_latest_sgp4_data(self):
        """
        Read latest SGP4 vector data from HDFS within time window.
        """
        try:
            # Read parquet data from HDFS
            df = self.spark.read.parquet(self.hdfs_input)
            
            logger.info(f"Available columns: {df.columns}")
            
            # Get latest record for each satellite
            window_spec = Window.partitionBy("satellite_id")
            
            # Use kafka_timestamp or epoch_time for ordering
            if "kafka_timestamp" in df.columns:
                timestamp_col = "kafka_timestamp"
            elif "epoch_time" in df.columns:
                timestamp_col = "epoch_time"
            else:
                timestamp_col = None
            
            if timestamp_col:
                df_latest = df.withColumn("max_ts", spark_max(col(timestamp_col)).over(window_spec)) \
                              .filter(col(timestamp_col) == col("max_ts")) \
                              .drop("max_ts")
            else:
                df_latest = df.dropDuplicates(["satellite_id"])
            
            satellite_count = df_latest.select("satellite_id").distinct().count()
            logger.info(f"Loaded {satellite_count} satellites with latest position data")
            return df_latest
            
        except Exception as e:
            logger.error(f"Error reading SGP4 data: {e}")
            raise
    
    def detect_collisions(self, df_positions):
        """
        Detect potential collisions by comparing satellite positions.
        Uses Euclidean distance in 3D space.
        """
        try:
            # Select relevant columns for comparison
            df_sat = df_positions.select(
                col("satellite_id"),
                col("position_x"),
                col("position_y"),
                col("position_z"),
                col("altitude_km"),
                col("velocity_magnitude_kms").alias("velocity")
            ).cache()  # Cache for join performance
            
            satellite_count = df_sat.count()
            logger.info(f"Comparing {satellite_count} satellites for potential collisions...")
            
            if satellite_count < 2:
                logger.warning("Not enough satellites for collision detection")
                return self.spark.createDataFrame([], schema="satellite_1 string, satellite_2 string")
            
            # Self-join to compare all pairs (avoid comparing satellite with itself)
            # Use satellite_id < satellite_id to avoid duplicate pairs
            df_pairs = df_sat.alias("sat1").crossJoin(
                broadcast(df_sat.alias("sat2"))
            ).filter(
                col("sat1.satellite_id") < col("sat2.satellite_id")
            )
            
            # Calculate 3D Euclidean distance
            df_distances = df_pairs.withColumn(
                "distance_km",
                sqrt(
                    spark_pow(col("sat2.position_x") - col("sat1.position_x"), 2) +
                    spark_pow(col("sat2.position_y") - col("sat1.position_y"), 2) +
                    spark_pow(col("sat2.position_z") - col("sat1.position_z"), 2)
                )
            )
            
            # Filter pairs below collision threshold
            df_collisions = df_distances.filter(
                col("distance_km") <= self.collision_threshold_km
            ).select(
                col("sat1.satellite_id").alias("satellite_1"),
                col("sat2.satellite_id").alias("satellite_2"),
                col("distance_km"),
                col("sat1.position_x").alias("sat1_x"),
                col("sat1.position_y").alias("sat1_y"),
                col("sat1.position_z").alias("sat1_z"),
                col("sat1.altitude_km").alias("sat1_altitude"),
                col("sat2.position_x").alias("sat2_x"),
                col("sat2.position_y").alias("sat2_y"),
                col("sat2.position_z").alias("sat2_z"),
                col("sat2.altitude_km").alias("sat2_altitude"),
                current_timestamp().alias("detection_timestamp")
            )
            
            # Add risk classification based on distance
            df_collisions = df_collisions.withColumn(
                "risk_level",
                when(col("distance_km") <= 1.0, "CRITICAL")
                .when(col("distance_km") <= 5.0, "HIGH")
                .when(col("distance_km") <= 10.0, "MEDIUM")
                .otherwise("LOW")
            )
            
            collision_count = df_collisions.count()
            logger.info(f"Detected {collision_count} potential collision pairs within {self.collision_threshold_km} km")
            
            # Log risk breakdown
            if collision_count > 0:
                risk_counts = df_collisions.groupBy("risk_level").agg(count("*").alias("count")).collect()
                for row in risk_counts:
                    logger.info(f"  {row['risk_level']}: {row['count']} pairs")
            
            df_sat.unpersist()  # Release cache
            return df_collisions
            
        except Exception as e:
            logger.error(f"Error detecting collisions: {e}")
            raise
    
    def save_to_hdfs(self, df_collisions):
        """Save collision predictions to HDFS."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"{self.hdfs_output}/batch_{timestamp}"
            
            df_collisions.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            logger.info(f"✓ Saved collision predictions to: {output_path}")
        except Exception as e:
            logger.error(f"Error saving to HDFS: {e}")
            raise
    
    def publish_to_kafka(self, df_collisions):
        """Publish high-risk collision alerts to Kafka."""
        try:
            # Filter only high risk and above
            df_alerts = df_collisions.filter(
                col("risk_level").isin(["CRITICAL", "HIGH", "MEDIUM"])
            )
            
            alert_count = df_alerts.count()
            if alert_count > 0:
                # Convert to JSON for Kafka
                df_kafka = df_alerts.selectExpr(
                    "CAST(satellite_1 AS STRING) as key",
                    "to_json(struct(*)) as value"
                )
                
                # Write to Kafka
                df_kafka.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.kafka_servers) \
                    .option("topic", self.kafka_topic) \
                    .save()
                
                logger.info(f"✓ Published {alert_count} alerts to Kafka topic: {self.kafka_topic}")
            else:
                logger.info("No high-risk alerts to publish")
                
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
    
    def save_satellites_to_postgres(self, df_positions):
        """
        Save/update satellite records to PostgreSQL using Spark JDBC.
        Must be called before collision alerts (due to FK constraint).
        """
        try:
            # Prepare satellite data for PostgreSQL
            df_satellites = df_positions.select(
                col("satellite_id").cast(IntegerType()).alias("norad_id"),
                lit("Unknown").alias("name"),
                lit("SATELLITE").alias("object_type"),
                lit("Unknown").alias("country"),
                lit("ACTIVE").alias("tracking_status"),
                col("epoch_time").alias("last_tle_epoch"),
                col("altitude_km").alias("last_altitude_km"),
                col("velocity_magnitude_kms").alias("last_velocity_kms"),
                col("position_x").alias("last_position_x"),
                col("position_y").alias("last_position_y"),
                col("position_z").alias("last_position_z"),
                coalesce(col("sgp4_error_code"), lit(0)).alias("last_sgp4_error_code"),
                lit(1).alias("total_observations"),
                current_timestamp().alias("status_updated_at")
            ).dropDuplicates(["norad_id"])
            
            satellite_count = df_satellites.count()
            logger.info(f"Writing {satellite_count} satellites to PostgreSQL...")
            
            # Write to temp table then merge (Spark JDBC doesn't support upsert directly)
            # Using overwrite mode - this will replace all satellite data
            df_satellites.write \
                .jdbc(
                    url=self.postgres_url,
                    table="satellites",
                    mode="overwrite",
                    properties=self.postgres_properties
                )
            
            logger.info(f"✓ Saved {satellite_count} satellites to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error saving satellites to PostgreSQL: {e}")
            # Don't raise - continue with other operations
    
    def save_collisions_to_postgres(self, df_collisions):
        """
        Save collision alerts to PostgreSQL for dashboard queries using Spark JDBC.
        """
        try:
            collision_count = df_collisions.count()
            if collision_count == 0:
                logger.info("No collisions to save to PostgreSQL")
                return
            
            # Prepare collision data for PostgreSQL schema
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            df_alerts = df_collisions.select(
                col("satellite_1").cast(IntegerType()).alias("satellite_1_id"),
                col("satellite_2").cast(IntegerType()).alias("satellite_2_id"),
                lit(None).cast(StringType()).alias("satellite_1_name"),
                lit(None).cast(StringType()).alias("satellite_2_name"),
                col("detection_timestamp").alias("predicted_time"),
                col("distance_km").alias("miss_distance_km"),
                lit(None).cast(DoubleType()).alias("relative_velocity_kms"),
                col("sat1_x").alias("approach_position_x"),
                col("sat1_y").alias("approach_position_y"),
                col("sat1_z").alias("approach_position_z"),
                col("risk_level"),
                lit(None).cast(DoubleType()).alias("collision_probability"),
                current_timestamp().alias("detected_at"),
                lit(batch_id).alias("batch_id"),
                lit(True).alias("is_active")
            )
            
            logger.info(f"Writing {collision_count} collision alerts to PostgreSQL...")
            
            # Use append mode - new collision alerts are added
            df_alerts.write \
                .jdbc(
                    url=self.postgres_url,
                    table="collision_alerts",
                    mode="append",
                    properties=self.postgres_properties
                )
            
            logger.info(f"✓ Saved {collision_count} collision alerts to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error saving collisions to PostgreSQL: {e}")
            
        except Exception as e:
            logger.error(f"Error saving collisions to PostgreSQL: {e}")
    
    def run(self):
        """Execute the complete collision prediction pipeline."""
        try:
            logger.info("Starting collision prediction pipeline...")
            
            # Step 1: Read latest SGP4 position data
            df_positions = self.read_latest_sgp4_data()
            
            # Step 2: Save satellites to PostgreSQL (required for FK constraint)
            self.save_satellites_to_postgres(df_positions)
            
            # Step 3: Detect collisions based on position proximity
            df_collisions = self.detect_collisions(df_positions)
            
            # Step 4: Save and publish results
            if df_collisions.count() > 0:
                self.save_to_hdfs(df_collisions)
                self.save_collisions_to_postgres(df_collisions)
                self.publish_to_kafka(df_collisions)
            else:
                logger.info("No collisions detected within threshold")
            
            logger.info("✓ Collision prediction pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    engine = CollisionPredictionEngine()
    engine.run()
