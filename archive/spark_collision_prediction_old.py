"""
Spark Streaming Job: Collision Prediction System
Reads SGP4 vectors from HDFS, predicts future positions, and detects potential collisions
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, udf, explode, array, struct, lit, current_timestamp,
    unix_timestamp, from_unixtime, expr, max as spark_max, 
    min as spark_min, count, avg, when, sqrt, pow
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, ArrayType, BooleanType
)
from sgp4.api import Satrec, jday
from datetime import datetime, timedelta, timezone
import logging
import os
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CollisionPredictionEngine:
    """
    Collision Prediction Pipeline: Read SGP4 vectors → Predict future positions → Detect collisions
    """
    
    def __init__(self):
        """Initialize Spark with configuration from environment."""
        
        # Load configuration from environment
        self.prediction_days = int(os.getenv('PREDICTION_DAYS', '7'))
        self.collision_threshold_km = float(os.getenv('COLLISION_THRESHOLD_KM', '10.0'))
        self.time_window_days = int(os.getenv('TIME_WINDOW_DAYS', '7'))
        self.propagation_step_hours = int(os.getenv('SGP4_PROPAGATION_STEP_HOURS', '6'))
        
        self.hdfs_input = os.getenv('HDFS_SGP4_VECTORS_PATH', 
                                     'hdfs://namenode:9000/space-debris/sgp4_vectors')
        self.hdfs_output = os.getenv('HDFS_COLLISION_PREDICTIONS_PATH',
                                      'hdfs://namenode:9000/space-debris/collision_predictions')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
        self.kafka_topic = os.getenv('KAFKA_COLLISION_TOPIC', 'space_debris_collisions')
        
        self.spark = SparkSession.builder \
            .appName("Collision-Prediction-Engine") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.sql.streaming.checkpointLocation", 
                   "hdfs://namenode:9000/tmp/checkpoint-collision") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("=" * 60)
        logger.info("=== Collision Prediction Engine Initialized ===")
        logger.info(f"Prediction Days: {self.prediction_days}")
        logger.info(f"Collision Threshold: {self.collision_threshold_km} km")
        logger.info(f"Time Window: {self.time_window_days} days")
        logger.info(f"Propagation Step: {self.propagation_step_hours} hours")
        logger.info(f"Input: {self.hdfs_input}")
        logger.info(f"Output: {self.hdfs_output}")
        logger.info("=" * 60)
    
    @staticmethod
    def propagate_sgp4(tle_line1, tle_line2, days_ahead, step_hours=6):
        """
        Propagate satellite position for next N days using SGP4.
        
        Args:
            tle_line1: First line of TLE
            tle_line2: Second line of TLE
            days_ahead: Number of days to propagate
            step_hours: Time step in hours between predictions
            
        Returns:
            List of tuples: [(timestamp, pos_x, pos_y, pos_z, vel_x, vel_y, vel_z)]
        """
        try:
            satellite = Satrec.twoline2rv(tle_line1, tle_line2)
            predictions = []
            
            now = datetime.now(timezone.utc)
            steps = int((days_ahead * 24) / step_hours)
            
            for i in range(steps):
                future_time = now + timedelta(hours=i * step_hours)
                jd, fr = jday(
                    future_time.year, future_time.month, future_time.day,
                    future_time.hour, future_time.minute, 
                    future_time.second + future_time.microsecond / 1e6
                )
                
                error_code, position, velocity = satellite.sgp4(jd, fr)
                
                if error_code == 0:
                    predictions.append((
                        future_time.isoformat(),
                        float(position[0]),
                        float(position[1]),
                        float(position[2]),
                        float(velocity[0]),
                        float(velocity[1]),
                        float(velocity[2])
                    ))
            
            return predictions
        except Exception as e:
            logger.error(f"SGP4 propagation error: {e}")
            return []
    
    @staticmethod
    def calculate_distance(x1, y1, z1, x2, y2, z2):
        """Calculate Euclidean distance between two 3D points in kilometers."""
        return math.sqrt((x2 - x1)**2 + (y2 - y1)**2 + (z2 - z1)**2)
    
    def read_latest_sgp4_data(self):
        """
        Read latest SGP4 vector data from HDFS within time window.
        Uses time window to determine if SGP4 propagation is needed.
        """
        try:
            # Read parquet data from HDFS
            df = self.spark.read.parquet(self.hdfs_input)
            
            # Filter data within time window - use epoch_time or kafka_timestamp
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.time_window_days)
            cutoff_timestamp = cutoff_time.isoformat()
            
            # Try epoch_time first, fall back to kafka_timestamp
            if "epoch_time" in df.columns:
                timestamp_col = "epoch_time"
            elif "kafka_timestamp" in df.columns:
                timestamp_col = "kafka_timestamp"
            else:
                # No timestamp filter if column not found
                logger.warning("No timestamp column found, using all data")
                timestamp_col = None
            
            if timestamp_col:
                df_recent = df.filter(col(timestamp_col) >= lit(cutoff_timestamp))
            else:
                df_recent = df
            
            # Get latest record for each satellite
            if timestamp_col:
                window_spec = Window.partitionBy("satellite_id").orderBy(col(timestamp_col).desc())
                df_latest = df_recent.withColumn("row_num", expr(f"row_number() over (partition by satellite_id order by {timestamp_col} desc)")) \
                                     .filter(col("row_num") == 1) \
                                     .drop("row_num")
            else:
                # Just deduplicate by satellite_id
                df_latest = df_recent.dropDuplicates(["satellite_id"])
            
            logger.info(f"Loaded {df_latest.count()} satellites with recent data")
            return df_latest
            
        except Exception as e:
            logger.error(f"Error reading SGP4 data: {e}")
            raise
    
    def predict_future_positions(self, df_sgp4):
        """
        Generate future position predictions for all satellites.
        Uses SGP4 propagation if data is older than time window.
        """
        # Register UDF for propagation
        propagate_udf = udf(
            lambda tle1, tle2, days, step: self.propagate_sgp4(tle1, tle2, days, step),
            ArrayType(StructType([
                StructField("timestamp", StringType()),
                StructField("pos_x", DoubleType()),
                StructField("pos_y", DoubleType()),
                StructField("pos_z", DoubleType()),
                StructField("vel_x", DoubleType()),
                StructField("vel_y", DoubleType()),
                StructField("vel_z", DoubleType())
            ]))
        )
        
        # Propagate each satellite's position
        df_predictions = df_sgp4.withColumn(
            "future_positions",
            propagate_udf(
                col("tle_line1"),
                col("tle_line2"),
                lit(self.prediction_days),
                lit(self.propagation_step_hours)
            )
        )
        
        # Explode predictions into separate rows
        df_exploded = df_predictions.select(
            col("satellite_id"),
            col("tle_line1"),
            col("tle_line2"),
            explode("future_positions").alias("prediction")
        ).select(
            col("satellite_id"),
            col("tle_line1"),
            col("tle_line2"),
            col("prediction.timestamp").alias("prediction_time"),
            col("prediction.pos_x"),
            col("prediction.pos_y"),
            col("prediction.pos_z"),
            col("prediction.vel_x"),
            col("prediction.vel_y"),
            col("prediction.vel_z")
        )
        
        logger.info(f"Generated {df_exploded.count()} position predictions")
        return df_exploded
    
    def detect_collisions(self, df_predictions):
        """
        Detect potential collisions by comparing all satellite pairs at each time step.
        """
        # Self-join to compare all pairs
        df_pairs = df_predictions.alias("sat1").join(
            df_predictions.alias("sat2"),
            (col("sat1.prediction_time") == col("sat2.prediction_time")) &
            (col("sat1.satellite_id") < col("sat2.satellite_id"))  # Avoid duplicate pairs
        )
        
        # Calculate distance between each pair
        distance_udf = udf(self.calculate_distance, DoubleType())
        
        df_distances = df_pairs.withColumn(
            "distance_km",
            distance_udf(
                col("sat1.pos_x"), col("sat1.pos_y"), col("sat1.pos_z"),
                col("sat2.pos_x"), col("sat2.pos_y"), col("sat2.pos_z")
            )
        )
        
        # Filter collisions below threshold
        df_collisions = df_distances.filter(
            col("distance_km") <= self.collision_threshold_km
        ).select(
            col("sat1.satellite_id").alias("satellite_1"),
            col("sat2.satellite_id").alias("satellite_2"),
            col("sat1.prediction_time").alias("collision_time"),
            col("distance_km"),
            col("sat1.pos_x").alias("sat1_pos_x"),
            col("sat1.pos_y").alias("sat1_pos_y"),
            col("sat1.pos_z").alias("sat1_pos_z"),
            col("sat2.pos_x").alias("sat2_pos_x"),
            col("sat2.pos_y").alias("sat2_pos_y"),
            col("sat2.pos_z").alias("sat2_pos_z"),
            current_timestamp().alias("detection_timestamp")
        )
        
        # Add risk classification
        df_collisions = df_collisions.withColumn(
            "risk_level",
            when(col("distance_km") <= float(os.getenv('HIGH_RISK_THRESHOLD_KM', '5.0')), "HIGH")
            .when(col("distance_km") <= float(os.getenv('MEDIUM_RISK_THRESHOLD_KM', '10.0')), "MEDIUM")
            .otherwise("LOW")
        )
        
        collision_count = df_collisions.count()
        logger.info(f"Detected {collision_count} potential collisions")
        
        if collision_count > 0:
            df_collisions.groupBy("risk_level").count().show()
        
        return df_collisions
    
    def save_to_hdfs(self, df_collisions):
        """Save collision predictions to HDFS in parquet format."""
        try:
            df_collisions.write \
                .mode("append") \
                .partitionBy("risk_level") \
                .parquet(self.hdfs_output)
            
            logger.info(f"✓ Saved collision predictions to {self.hdfs_output}")
        except Exception as e:
            logger.error(f"Error saving to HDFS: {e}")
            raise
    
    def publish_to_kafka(self, df_collisions):
        """Publish high-risk collisions to Kafka for real-time alerting."""
        try:
            # Filter only high and medium risk
            df_alerts = df_collisions.filter(
                col("risk_level").isin(["HIGH", "MEDIUM"])
            )
            
            if df_alerts.count() > 0:
                # Convert to JSON
                df_kafka = df_alerts.selectExpr(
                    "satellite_1 as key",
                    "to_json(struct(*)) as value"
                )
                
                # Write to Kafka
                df_kafka.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.kafka_servers) \
                    .option("topic", self.kafka_topic) \
                    .save()
                
                logger.info(f"✓ Published {df_alerts.count()} alerts to Kafka topic: {self.kafka_topic}")
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
    
    def run(self):
        """Execute the complete collision prediction pipeline."""
        try:
            logger.info("Starting collision prediction pipeline...")
            
            # Step 1: Read latest SGP4 data
            df_sgp4 = self.read_latest_sgp4_data()
            
            # Step 2: Predict future positions
            df_predictions = self.predict_future_positions(df_sgp4)
            
            # Step 3: Detect collisions
            df_collisions = self.detect_collisions(df_predictions)
            
            # Step 4: Save to HDFS
            if df_collisions.count() > 0:
                self.save_to_hdfs(df_collisions)
                
                # Step 5: Publish high-risk alerts to Kafka
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
