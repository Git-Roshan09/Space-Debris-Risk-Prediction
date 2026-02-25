"""
Spark Job: Optimized Collision Prediction System
Reads classified SGP4 vectors from HDFS and detects potential collisions.
ONLY processes SAT-SAT and SAT-DEB collision pairs (excludes DEB-DEB as requested).
Writes results to HDFS (historical), Kafka (streaming), and PostgreSQL (dashboard).
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, sqrt, pow as spark_pow,
    when, broadcast, max as spark_max, count, coalesce, 
    desc, asc, size, array_contains
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType, StructType, StructField
from datetime import datetime, timedelta, timezone
import logging
import os
import csv

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres-debris'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'space_debris'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

CATALOG_DIR = os.getenv('CATALOG_DIR', 'data/raw')


class CollisionPredictionEngine:
    """
    Optimized Collision Detection Pipeline for Space Debris Risk Assessment.
    
    Processes SGP4 orbital vectors to identify potential satellite-satellite and
    satellite-debris collisions. Excludes debris-debris pairs as requested.
    Outputs predictions to HDFS for historical analysis, Kafka for streaming,
    and PostgreSQL for dashboard queries.
    """
    
    def __init__(self):
        """
        Initialize Spark session and load configuration from environment variables.
        
        Sets up collision detection thresholds, HDFS paths, Kafka configuration,
        and PostgreSQL connection parameters. Loads object classification catalogs
        for satellite and debris identification.
        """
        
        self.collision_threshold_km = float(os.getenv('COLLISION_THRESHOLD_KM', '50.0'))
        self.time_window_days = int(os.getenv('TIME_WINDOW_DAYS', '7'))
        
        # Risk threshold configuration from environment variables
        self.high_risk_threshold = float(os.getenv('HIGH_RISK_THRESHOLD_KM', '20.0'))
        self.medium_risk_threshold = float(os.getenv('MEDIUM_RISK_THRESHOLD_KM', '35.0'))
        self.low_risk_threshold = float(os.getenv('LOW_RISK_THRESHOLD_KM', '25.0'))
        
        self.hdfs_input = os.getenv('HDFS_SGP4_VECTORS_PATH', 
                                     'hdfs://namenode:9000/space-debris/sgp4_vectors')
        self.hdfs_output = os.getenv('HDFS_COLLISION_PREDICTIONS_PATH',
                                      'hdfs://namenode:9000/space-debris/collision_predictions')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
        self.kafka_topic = os.getenv('KAFKA_COLLISION_TOPIC', 'space_debris_collisions')
        
        self.spark = SparkSession.builder \
            .appName("Optimized-Collision-Prediction-Engine") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.postgres_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        self.postgres_properties = {
            "user": POSTGRES_CONFIG['user'],
            "password": POSTGRES_CONFIG['password'],
            "driver": "org.postgresql.Driver"
        }
        
        self.satellite_ids = set()
        self.debris_ids = set()
        self._load_object_classifications()
        
        logger.info("=" * 70)
        logger.info("=== Optimized Collision Prediction Engine Initialized ===")
        logger.info(f"Collision Threshold: {self.collision_threshold_km} km")
        logger.info(f"High Risk Threshold: {self.high_risk_threshold} km")
        logger.info(f"Medium Risk Threshold: {self.medium_risk_threshold} km")
        logger.info(f"Low Risk Threshold: {self.low_risk_threshold} km")
        logger.info(f"Time Window: {self.time_window_days} days")
        logger.info(f"Satellites classified: {len(self.satellite_ids):,}")
        logger.info(f"Debris classified: {len(self.debris_ids):,}")
        logger.info(f"Collision types: SAT-SAT, SAT-DEB (DEB-DEB excluded)")
        logger.info(f"Input: {self.hdfs_input}")
        logger.info(f"Output: {self.hdfs_output}")
        logger.info(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
        logger.info("=" * 70)
    
    def _load_object_classifications(self):
        """
        Load satellite and debris classifications from CSV catalog files.
        
        Reads satellites_and_objects_catalog.csv and space_debris_catalog.csv
        to create classification lookup tables. Creates Spark DataFrames for
        efficient joining with SGP4 vector data.
        """
        try:
            sat_catalog_path = os.path.join(CATALOG_DIR, 'satellites_and_objects_catalog.csv')
            if os.path.exists(sat_catalog_path):
                with open(sat_catalog_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            norad_id = int(row['NORAD_CAT_ID'])
                            self.satellite_ids.add(norad_id)
                        except (ValueError, KeyError):
                            continue
                logger.info(f"✅ Loaded {len(self.satellite_ids):,} satellite classifications")
            debris_catalog_path = os.path.join(CATALOG_DIR, 'space_debris_catalog.csv')
            if os.path.exists(debris_catalog_path):
                with open(debris_catalog_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            norad_id = int(row['NORAD_CAT_ID'])
                            self.debris_ids.add(norad_id)
                        except (ValueError, KeyError):
                            continue
                logger.info(f"✅ Loaded {len(self.debris_ids):,} debris classifications")
            satellite_ids_list = list(self.satellite_ids)
            debris_ids_list = list(self.debris_ids)
            
            satellite_df = self.spark.createDataFrame(
                [(sat_id, 'SATELLITE') for sat_id in satellite_ids_list],
                ['norad_id', 'classification']
            ).cache()
            
            debris_df = self.spark.createDataFrame(
                [(deb_id, 'DEBRIS') for deb_id in debris_ids_list],
                ['norad_id', 'classification']
            ).cache()
            
            self.classification_df = satellite_df.union(debris_df)
            logger.info(f"📊 Classification lookup table: {self.classification_df.count():,} objects")
            
        except Exception as e:
            logger.error(f"❌ Error loading classifications: {e}")
            self.satellite_ids = set()
            self.debris_ids = set()
            self.classification_df = self.spark.createDataFrame([], 'norad_id int, classification string')
    
    def get_simulation_time(self):
        """
        Get the current simulation time from the dashboard API.
        
        Returns:
            datetime: Current simulation time, or current time if API unavailable
        """
        if not HAS_REQUESTS:
            logger.warning("⚠️  requests module not available, using current time instead of simulation time")
            return datetime.now(timezone.utc)
            
        try:
            # Try to get simulation time from dashboard API
            dashboard_url = os.getenv('DASHBOARD_API_URL', 'http://dashboard-api:5001')
            response = requests.get(f"{dashboard_url}/api/simulation/time", timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                sim_time = datetime.fromisoformat(data['current_simulated_time'].replace('Z', '+00:00'))
                logger.info(f"🕐 Using simulation time: {sim_time.isoformat()}")
                return sim_time
            else:
                logger.warning(f"⚠️  Dashboard API returned {response.status_code}, using current time")
                return datetime.now(timezone.utc)
                
        except Exception as e:
            logger.warning(f"⚠️  Could not get simulation time from API ({e}), using current time")
            return datetime.now(timezone.utc)
    
    def read_latest_sgp4_data(self):
        """
        Read and classify the latest SGP4 vector data from HDFS storage.
        
        Returns:
            DataFrame: Classified SGP4 vectors with satellite and debris objects
                       including positions, velocities, and timestamps
        """
        try:
            df = self.spark.read \
                .option("basePath", self.hdfs_input) \
                .option("mergeSchema", "true") \
                .parquet(self.hdfs_input + "/epoch_time=*")
            
            logger.info(f"Available columns: {df.columns}")
            
            window_spec = Window.partitionBy("norad_id")
            
            if "message_timestamp" in df.columns:
                timestamp_col = "message_timestamp"
            elif "kafka_timestamp" in df.columns:
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
                df_latest = df.dropDuplicates(["norad_id"])
            
            df_latest = df_latest.withColumn("norad_id", col("norad_id").cast(IntegerType()))
            
            if "classification" in df_latest.columns:
                df_latest = df_latest.drop("classification")
            
            df_classified = df_latest.join(
                broadcast(self.classification_df),
                on="norad_id",
                how="left"
            ).fillna("UNKNOWN", ["classification"])
            
            classification_counts = df_classified.groupBy("classification").count().collect()
            for row in classification_counts:
                logger.info(f"  {row['classification']}: {row['count']:,} objects")
            
            total_objects = df_classified.count()
            logger.info(f"✅ Loaded {total_objects:,} objects with classifications")
            
            return df_classified
            
        except Exception as e:
            logger.error(f"Error reading SGP4 data: {e}")
            raise

    def detect_optimized_collisions(self, df_positions):
        """
        Detect potential collisions between satellite-satellite and satellite-debris pairs.
        
        Explicitly excludes debris-debris collisions as requested. Uses efficient
        cross-join with distance calculations and risk level classification.
        
        Args:
            df_positions (DataFrame): Classified SGP4 position vectors
            
        Returns:
            DataFrame: Collision predictions with distances, velocities, and risk levels
        """
        try:
            df_objects = df_positions.select(
                col("norad_id"),
                col("object_name"),
                col("classification"),
                col("position_x"),
                col("position_y"),
                col("position_z"),
                col("altitude_km"),
                col("velocity_magnitude_kms").alias("velocity")
            ).cache()
            
            df_satellites = df_objects.filter(col("classification") == "SATELLITE").cache()
            df_debris = df_objects.filter(col("classification") == "DEBRIS").cache()
            
            satellite_count = df_satellites.count()
            debris_count = df_debris.count()
            
            logger.info(f"🛰️  Satellites for collision detection: {satellite_count:,}")
            logger.info(f"🗑️  Debris for collision detection: {debris_count:,}")
            
            collision_pairs = []
            
            if satellite_count >= 2:
                logger.info("🔍 Detecting SAT-SAT collision pairs...")
                df_sat_sat = self._detect_pairs(
                    df_satellites.alias("sat1"),
                    df_satellites.alias("sat2"),
                    "SAT-SAT"
                )
                collision_pairs.append(df_sat_sat)
            
            if satellite_count > 0 and debris_count > 0:
                logger.info("🔍 Detecting SAT-DEB collision pairs...")
                df_sat_deb = self._detect_pairs(
                    df_satellites.alias("sat1"),
                    df_debris.alias("deb1"),
                    "SAT-DEB"
                )
                collision_pairs.append(df_sat_deb)
            
            if collision_pairs:
                df_all_collisions = collision_pairs[0]
                for df_collision in collision_pairs[1:]:
                    df_all_collisions = df_all_collisions.union(df_collision)
            else:
                schema = StructType([
                    StructField("object_1", StringType(), True),
                    StructField("object_2", StringType(), True),
                    StructField("collision_type", StringType(), True)
                ])
                df_all_collisions = self.spark.createDataFrame([], schema)
            
            df_objects.unpersist()
            df_satellites.unpersist()
            df_debris.unpersist()
            
            total_collisions = df_all_collisions.count()
            logger.info(f"📊 Total collision pairs detected: {total_collisions:,}")
            
            if total_collisions > 0:
                collision_type_counts = df_all_collisions.groupBy("collision_type").count().collect()
                for row in collision_type_counts:
                    logger.info(f"  {row['collision_type']}: {row['count']:,} pairs")
            
            return df_all_collisions
            
        except Exception as e:
            logger.error(f"Error detecting collisions: {e}")
            raise

    def _detect_pairs(self, df1, df2, collision_type):
        """
        Perform pairwise collision detection between two object groups.
        
        Uses cross-join to compare all object pairs and calculates 3D Euclidean
        distances between their positions. Filters pairs within collision threshold
        and classifies risk levels based on distance.
        
        Args:
            df1 (DataFrame): First set of objects (e.g., satellites)
            df2 (DataFrame): Second set of objects (e.g., debris or satellites)
            collision_type (str): Type of collision ('SAT-SAT' or 'SAT-DEB')
            
        Returns:
            DataFrame: Collision pairs with distances, positions, and risk classifications

        Helper method to detect collision pairs between two object groups.
        """
        # Create pairs (avoid self-comparison for SAT-SAT)
        if collision_type == "SAT-SAT":
            # For SAT-SAT, use norad_id comparison to avoid duplicates
            df_pairs = df1.crossJoin(broadcast(df2)).filter(
                col("sat1.norad_id") < col("sat2.norad_id"))
            
            df_distances = df_pairs.withColumn(
                "distance_km",
                sqrt(
                    spark_pow(col("sat2.position_x") - col("sat1.position_x"), 2) +
                    spark_pow(col("sat2.position_y") - col("sat1.position_y"), 2) +
                    spark_pow(col("sat2.position_z") - col("sat1.position_z"), 2)
                )
            )
            
        else:
            df_pairs = df1.crossJoin(broadcast(df2))
            
            df_distances = df_pairs.withColumn(
                "distance_km",
                sqrt(
                    spark_pow(col("deb1.position_x") - col("sat1.position_x"), 2) +
                    spark_pow(col("deb1.position_y") - col("sat1.position_y"), 2) +
                    spark_pow(col("deb1.position_z") - col("sat1.position_z"), 2)
                )
            )
        df_collisions = df_distances.filter(
            col("distance_km") <= self.collision_threshold_km
        )
        if collision_type == "SAT-DEB":
            df_result = df_collisions.select(
                col("sat1.norad_id").alias("object_1"),
                col("deb1.norad_id").alias("object_2"),
                col("sat1.norad_id").alias("norad_1"),
                col("deb1.norad_id").alias("norad_2"),
                coalesce(col("sat1.object_name"), lit("Unknown")).alias("object_1_name"),
                coalesce(col("deb1.object_name"), lit("Unknown")).alias("object_2_name"),
                col("sat1.classification").alias("classification_1"),
                col("deb1.classification").alias("classification_2"),
                col("distance_km"),
                lit(collision_type).alias("collision_type"),
                col("sat1.position_x").alias("obj1_x"),
                col("sat1.position_y").alias("obj1_y"),
                col("sat1.position_z").alias("obj1_z"),
                col("sat1.altitude_km").alias("obj1_altitude"),
                col("sat1.velocity").alias("obj1_velocity"),
                col("deb1.position_x").alias("obj2_x"),
                col("deb1.position_y").alias("obj2_y"),
                col("deb1.position_z").alias("obj2_z"),
                col("deb1.altitude_km").alias("obj2_altitude"),
                col("deb1.velocity").alias("obj2_velocity"),
                lit(self.get_simulation_time()).cast(TimestampType()).alias("detection_timestamp")
            )
        else:
            df_result = df_collisions.select(
                col("sat1.norad_id").alias("object_1"),
                col("sat2.norad_id").alias("object_2"),
                col("sat1.norad_id").alias("norad_1"),
                col("sat2.norad_id").alias("norad_2"),
                coalesce(col("sat1.object_name"), lit("Unknown")).alias("object_1_name"),
                coalesce(col("sat2.object_name"), lit("Unknown")).alias("object_2_name"),
                col("sat1.classification").alias("classification_1"),
                col("sat2.classification").alias("classification_2"),
                col("distance_km"),
                lit(collision_type).alias("collision_type"),
                col("sat1.position_x").alias("obj1_x"),
                col("sat1.position_y").alias("obj1_y"),
                col("sat1.position_z").alias("obj1_z"),
                col("sat1.altitude_km").alias("obj1_altitude"),
                col("sat1.velocity").alias("obj1_velocity"),
                col("sat2.position_x").alias("obj2_x"),
                col("sat2.position_y").alias("obj2_y"),
                col("sat2.position_z").alias("obj2_z"),
                col("sat2.altitude_km").alias("obj2_altitude"),
                col("sat2.velocity").alias("obj2_velocity"),
                lit(self.get_simulation_time()).cast(TimestampType()).alias("detection_timestamp")
            )

        # Compute relative velocity (scalar approximation)
        df_result = df_result.withColumn(
            "relative_velocity_kms",
            when(
                col("obj1_velocity").isNotNull() & col("obj2_velocity").isNotNull(),
                (col("obj1_velocity") + col("obj2_velocity"))
            ).otherwise(lit(None).cast(DoubleType()))
        )

        # Risk classification
        df_result = df_result.withColumn(
            "risk_level",
            when(col("distance_km") <= 1.0, "CRITICAL")
            .when(col("distance_km") <= self.high_risk_threshold, "HIGH")
            .when(col("distance_km") <= self.medium_risk_threshold, "MEDIUM")
            .otherwise("LOW")
        )

        # Collision probability estimate (simple inverse-distance model)
        df_result = df_result.withColumn(
            "collision_probability",
            when(col("distance_km") <= 0.01, lit(1.0))
            .otherwise(lit(1.0) / (lit(1.0) + col("distance_km") * col("distance_km")))
        )
        
        pair_count = df_result.count()
        logger.info(f"  {collision_type}: {pair_count:,} close approach pairs detected within {self.collision_threshold_km} km")
        
        return df_result
    
    def save_to_hdfs(self, df_collisions):
        """
        Save collision predictions to HDFS in Parquet format.
        
        Creates timestamped batch directories for historical analysis
        and efficient querying of collision prediction data.
        
        Args:
            df_collisions (DataFrame): Collision predictions to save
        """
        try:
            timestamp = self.get_simulation_time().strftime("%Y%m%d_%H%M%S")
            output_path = f"{self.hdfs_output}/batch_{timestamp}"
            
            df_collisions.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            logger.info(f"✓ Saved collision predictions to: {output_path}")
        except Exception as e:
            logger.error(f"Error saving to HDFS: {e}")
            raise
    
    def publish_to_kafka(self, df_collisions):
        """
        Publish collision alerts to Kafka for real-time processing.
        
        Includes all risk levels (CRITICAL, HIGH, MEDIUM, LOW) for
        comprehensive monitoring. Converts DataFrame to JSON format
        for Kafka message consumption.
        
        Args:
            df_collisions (DataFrame): Collision predictions to publish
        """
        try:
            df_alerts = df_collisions.filter(
                col("risk_level").isin(["CRITICAL", "HIGH", "MEDIUM", "LOW"])
            )
            
            alert_count = df_alerts.count()
            if alert_count > 0:
                df_kafka = df_alerts.selectExpr(
                    "CAST(object_1 AS STRING) as key",
                    "to_json(struct(*)) as value"
                )
                
                def _publish():
                    df_kafka.write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", self.kafka_servers) \
                        .option("topic", self.kafka_topic) \
                        .save()
                
                self._retry(_publish, "Kafka publish")
                
                logger.info(f"✓ Published {alert_count} alerts to Kafka topic: {self.kafka_topic}")
            else:
                logger.info("No high-risk alerts to publish")
                
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
    
    def save_satellites_to_postgres(self, df_positions):
        """
        Save or update satellite tracking records in PostgreSQL database.
        
        Must be called before collision alerts due to foreign key constraints.
        Filters out invalid data including SGP4 errors and unrealistic altitudes.
        Updates satellite tracking status and last known positions.
        
        Args:
            df_positions (DataFrame): Classified position data with satellites
        """
        try:
            MAX_REASONABLE_ALTITUDE_KM = 100000.0
            
            df_satellites = df_positions.select(
                col("norad_id").cast(IntegerType()).alias("norad_id"),
                coalesce(col("object_name"), lit("Unknown")).alias("name"),
                coalesce(col("classification"), lit("SATELLITE")).alias("object_type"),
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
                lit(self.get_simulation_time()).cast(TimestampType()).alias("status_updated_at")
            ).dropDuplicates(["norad_id"])
            
            # Filter out invalid data:
            # - SGP4 error code must be 0 (successful propagation)
            # - Altitude must be positive and within reasonable bounds
            # - Altitude must not be NULL/NaN
            initial_count = df_satellites.count()
            df_satellites = df_satellites.filter(
                (col("last_sgp4_error_code") == 0) &
                (col("last_altitude_km").isNotNull()) &
                (col("last_altitude_km") > 0) &
                (col("last_altitude_km") < MAX_REASONABLE_ALTITUDE_KM)
            )
            
            satellite_count = df_satellites.count()
            filtered_out = initial_count - satellite_count
            if filtered_out > 0:
                logger.info(f"📊 Data quality filter: removed {filtered_out} records with invalid data")
            logger.info(f"Writing {satellite_count} valid satellites to PostgreSQL...")
            
            # Use psycopg2 for direct database insert (Spark JDBC has issues with FK constraints)
            import psycopg2
            from psycopg2.extras import execute_batch
            
            try:
                logger.info("Step 1: Connecting to PostgreSQL...")
                conn = psycopg2.connect(
                    host=POSTGRES_CONFIG['host'],
                    port=POSTGRES_CONFIG['port'],
                    database=POSTGRES_CONFIG['database'],
                    user=POSTGRES_CONFIG['user'],
                    password=POSTGRES_CONFIG['password']
                )
                cursor = conn.cursor()
                logger.info("✓ Connected to PostgreSQL")
                
                # Collect satellite data
                logger.info("Step 2: Collecting satellite data from Spark DataFrame...")
                satellite_data = df_satellites.collect()
                logger.info(f"✓ Collected {len(satellite_data)} satellite records")
                
                # UPSERT satellites using ON CONFLICT DO UPDATE
                logger.info("Step 3: Executing UPSERT for satellites...")
                upsert_query = """
                    INSERT INTO satellites (
                        norad_id, name, object_type, country, tracking_status,
                        last_tle_epoch, last_altitude_km, last_velocity_kms,
                        last_position_x, last_position_y, last_position_z,
                        last_sgp4_error_code, total_observations, status_updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (norad_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        object_type = EXCLUDED.object_type,
                        tracking_status = EXCLUDED.tracking_status,
                        last_tle_epoch = EXCLUDED.last_tle_epoch,
                        last_altitude_km = EXCLUDED.last_altitude_km,
                        last_velocity_kms = EXCLUDED.last_velocity_kms,
                        last_position_x = EXCLUDED.last_position_x,
                        last_position_y = EXCLUDED.last_position_y,
                        last_position_z = EXCLUDED.last_position_z,
                        last_sgp4_error_code = EXCLUDED.last_sgp4_error_code,
                        total_observations = satellites.total_observations + EXCLUDED.total_observations,
                        status_updated_at = EXCLUDED.status_updated_at
                """
                
                rows = [
                    (
                        row['norad_id'], row['name'], row['object_type'], row['country'],
                        row['tracking_status'], row['last_tle_epoch'], row['last_altitude_km'],
                        row['last_velocity_kms'], row['last_position_x'], row['last_position_y'],
                        row['last_position_z'], row['last_sgp4_error_code'],
                        row['total_observations'], row['status_updated_at']
                    )
                    for row in satellite_data
                ]
                logger.info(f"✓ Prepared {len(rows)} rows for UPSERT")
                
                logger.info("Step 4: Executing batch UPSERT...")
                execute_batch(cursor, upsert_query, rows, page_size=1000)
                logger.info("✓ Batch UPSERT executed")
                
                logger.info("Step 5: Committing transaction...")
                conn.commit()
                logger.info("✓ Transaction committed")
                
                cursor.close()
                conn.close()
                
                logger.info(f"✅ Successfully UPSERTED {satellite_count} satellites to PostgreSQL")
                
            except Exception as psycopg2_err:
                logger.error(f"❌ psycopg2 error: {psycopg2_err}")
                logger.error(f"Error type: {type(psycopg2_err).__name__}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise  # Re-raise to trigger outer exception handler
            
        except Exception as e:
            logger.error(f"Error saving satellites to PostgreSQL: {e}")
            # Don't raise - continue with other operations
    
    def _retry(self, func, description, max_retries=3):
        """
        Retry wrapper with exponential backoff for resilient I/O operations.
        
        Args:
            func (callable): Function to execute
            description (str): Human-readable operation name for logging
            max_retries (int): Maximum number of retry attempts
        """
        import time as _time
        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except Exception as e:
                if attempt == max_retries:
                    logger.error(f"❌ {description} failed after {max_retries} attempts: {e}")
                    raise
                wait = 2 ** attempt
                logger.warning(f"⚠ {description} attempt {attempt}/{max_retries} failed: {e}. Retrying in {wait}s...")
                _time.sleep(wait)

    def save_collisions_to_postgres(self, df_collisions):
        """
        Save collision alerts to PostgreSQL using UPSERT to prevent duplicates.
        Uses ON CONFLICT DO UPDATE for atomic updates without race conditions.
        """
        try:
            collision_count = df_collisions.count()
            if collision_count == 0:
                logger.info("No close approaches to save to PostgreSQL")
                return
            
            batch_id = self.get_simulation_time().strftime("%Y%m%d_%H%M%S")
            
            df_alerts = df_collisions.select(
                col("object_1").cast(IntegerType()).alias("satellite_1_id"),
                col("object_2").cast(IntegerType()).alias("satellite_2_id"),
                col("object_1_name").alias("satellite_1_name"),
                col("object_2_name").alias("satellite_2_name"),
                col("detection_timestamp").alias("predicted_time"),
                col("distance_km").alias("miss_distance_km"),
                col("relative_velocity_kms"),
                col("obj1_x").alias("approach_position_x"),
                col("obj1_y").alias("approach_position_y"),
                col("obj1_z").alias("approach_position_z"),
                col("risk_level"),
                col("collision_probability"),
                lit(self.get_simulation_time()).cast(TimestampType()).alias("detected_at"),
                lit(batch_id).alias("batch_id"),
                lit(True).alias("is_active")
            )
            
            logger.info(f"Writing {collision_count} close approach alerts to PostgreSQL using UPSERT...")
            
            # Use psycopg2 for UPSERT to avoid race conditions
            import psycopg2
            from psycopg2.extras import execute_batch
            
            conn = psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                port=POSTGRES_CONFIG['port'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password']
            )
            cursor = conn.cursor()
            
            # Collect collision data
            collision_data = df_alerts.collect()
            
            # UPSERT query with ON CONFLICT on unique constraint
            upsert_query = """
                INSERT INTO collision_alerts (
                    satellite_1_id, satellite_2_id, satellite_1_name, satellite_2_name,
                    predicted_time, miss_distance_km, relative_velocity_kms,
                    approach_position_x, approach_position_y, approach_position_z,
                    risk_level, collision_probability, detected_at, batch_id, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (satellite_1_id, satellite_2_id, predicted_time) DO UPDATE SET
                    satellite_1_name = EXCLUDED.satellite_1_name,
                    satellite_2_name = EXCLUDED.satellite_2_name,
                    miss_distance_km = EXCLUDED.miss_distance_km,
                    relative_velocity_kms = EXCLUDED.relative_velocity_kms,
                    approach_position_x = EXCLUDED.approach_position_x,
                    approach_position_y = EXCLUDED.approach_position_y,
                    approach_position_z = EXCLUDED.approach_position_z,
                    risk_level = EXCLUDED.risk_level,
                    collision_probability = EXCLUDED.collision_probability,
                    detected_at = EXCLUDED.detected_at,
                    batch_id = EXCLUDED.batch_id,
                    is_active = EXCLUDED.is_active
            """
            
            rows = [
                (
                    row['satellite_1_id'], row['satellite_2_id'],
                    row['satellite_1_name'], row['satellite_2_name'],
                    row['predicted_time'], row['miss_distance_km'],
                    row['relative_velocity_kms'],
                    row['approach_position_x'], row['approach_position_y'], row['approach_position_z'],
                    row['risk_level'], row['collision_probability'],
                    row['detected_at'], row['batch_id'], row['is_active']
                )
                for row in collision_data
            ]
            
            execute_batch(cursor, upsert_query, rows, page_size=1000)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            logger.info(f"✅ UPSERTED {collision_count} close approach alerts to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error saving close approaches to PostgreSQL: {e}")
    
    def run(self):
        """Execute the optimized collision prediction pipeline with state coordination."""
        try:
            logger.info("🚀 Starting optimized collision prediction pipeline...")
            logger.info("📋 Pipeline scope: SAT-SAT and SAT-DEB collisions only (DEB-DEB excluded)")
            
            # Step 0: Check if new SGP4 data is available
            if not self._should_run_collision_prediction():
                logger.info("⏸️  No new SGP4 data available - skipping collision prediction")
                logger.info("✅ Pipeline check completed - no work needed")
                return
            
            # Mark pipeline as running
            self._update_pipeline_state('RUNNING', 0, None)
            
            # Step 1: Read latest SGP4 position data with classifications
            df_positions = self.read_latest_sgp4_data()
            
            # Step 2: Save satellites to PostgreSQL (required for FK constraint)
            self.save_satellites_to_postgres(df_positions)
            
            # Step 3: Detect optimized collisions (SAT-SAT and SAT-DEB only)
            df_collisions = self.detect_optimized_collisions(df_positions)
            
            # Step 4: Save and publish results
            collision_count = df_collisions.count()
            if collision_count > 0:
                logger.info(f"💾 Saving {collision_count:,} collision predictions...")
                self.save_to_hdfs(df_collisions)
                self.save_collisions_to_postgres(df_collisions)
                self.publish_to_kafka(df_collisions)
            else:
                logger.info("✅ No collisions detected within threshold - system safe")
            
            # Update pipeline state as successful
            self._update_pipeline_state('SUCCESS', collision_count, None)
            
            logger.info("🎉 Optimized collision prediction pipeline completed successfully")
            logger.info(f"📊 Final results: {collision_count:,} collision pairs identified (SAT-SAT + SAT-DEB)")
            
        except Exception as e:
            logger.error(f"❌ Pipeline execution failed: {e}")
            self._update_pipeline_state('FAILED', 0, str(e))
            raise
        finally:
            self.spark.stop()
    
    def _should_run_collision_prediction(self):
        """
        Check if new SGP4 data is available for collision prediction.
        Coordinates with SGP4 processing pipeline via pipeline_state table.
        
        Returns:
            bool: True if new data available and collision prediction should run
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                port=POSTGRES_CONFIG['port'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password']
            )
            cursor = conn.cursor()
            
            # Get last data version processed by SGP4 and Collision Prediction
            cursor.execute("""
                SELECT component_name, data_version, last_run_status, last_run_end
                FROM pipeline_state
                WHERE component_name IN ('SGP4_PROCESSING', 'COLLISION_PREDICTION')
            """)
            
            results = {row[0]: {'version': row[1], 'status': row[2], 'end': row[3]} 
                      for row in cursor.fetchall()}
            
            cursor.close()
            conn.close()
            
            sgp4_version = results.get('SGP4_PROCESSING', {}).get('version', 0)
            collision_version = results.get('COLLISION_PREDICTION', {}).get('version', 0)
            sgp4_status = results.get('SGP4_PROCESSING', {}).get('status', 'IDLE')
            
            logger.info(f"📊 Data versions - SGP4: {sgp4_version}, Collision: {collision_version}")
            logger.info(f"📊 SGP4 status: {sgp4_status}")
            
            # Run if SGP4 has processed new data
            if sgp4_version > collision_version and sgp4_status == 'SUCCESS':
                logger.info(f"✅ New SGP4 data available (version {sgp4_version}) - proceeding with collision prediction")
                return True
            else:
                logger.info(f"⏸️  No new data - SGP4 version {sgp4_version} already processed")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️  Could not check pipeline state: {e} - running collision prediction anyway")
            return True  # Default to running if state check fails
    
    def _update_pipeline_state(self, status, records_processed, error_message):
        """
        Update pipeline state in PostgreSQL to coordinate with other components.
        
        Args:
            status (str): Pipeline status - RUNNING, SUCCESS, FAILED
            records_processed (int): Number of collision pairs detected
            error_message (str): Error message if failed, None otherwise
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                port=POSTGRES_CONFIG['port'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password']
            )
            cursor = conn.cursor()
            
            if status == 'RUNNING':
                cursor.execute("""
                    UPDATE pipeline_state 
                    SET last_run_start = NOW(),
                        last_run_status = 'RUNNING',
                        updated_at = NOW()
                    WHERE component_name = 'COLLISION_PREDICTION'
                """)
            elif status == 'SUCCESS':
                # Get SGP4 version to sync with
                cursor.execute("""
                    SELECT data_version FROM pipeline_state 
                    WHERE component_name = 'SGP4_PROCESSING'
                """)
                sgp4_version = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
                
                cursor.execute("""
                    UPDATE pipeline_state 
                    SET last_run_end = NOW(),
                        last_run_status = 'SUCCESS',
                        records_processed = records_processed + %s,
                        data_version = %s,
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE component_name = 'COLLISION_PREDICTION'
                """, (records_processed, sgp4_version))
            elif status == 'FAILED':
                cursor.execute("""
                    UPDATE pipeline_state 
                    SET last_run_end = NOW(),
                        last_run_status = 'FAILED',
                        error_message = %s,
                        updated_at = NOW()
                    WHERE component_name = 'COLLISION_PREDICTION'
                """, (error_message,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Updated pipeline state: {status}")
            
        except Exception as e:
            logger.warning(f"⚠️  Could not update pipeline state: {e}")


if __name__ == "__main__":
    engine = CollisionPredictionEngine()
    engine.run()
