"""
Enhanced Spark Streaming: TLE → SGP4 Vectors → Risk Analysis
Architecture: Kafka → Spark → (HDFS + Cassandra)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, struct, current_timestamp,
    window, avg, max as spark_max, min as spark_min, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, ArrayType
)
from sgp4.api import Satrec, jday
from datetime import datetime, timezone
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SGP4StreamProcessor:
    """
    End-to-End Streaming Pipeline:
    1. Read TLE from Kafka
    2. Calculate SGP4 position/velocity vectors
    3. Compute risk scores
    4. Write to HDFS (vectors) and Cassandra (risk scores)
    """
    
    def __init__(self, 
                 kafka_servers='localhost:9092',
                 hdfs_path='hdfs://localhost:9000/space-debris',
                 cassandra_host='localhost'):
        """Initialize Spark with Kafka, HDFS, and Cassandra configs."""
        
        self.spark = SparkSession.builder \
            .appName("SGP4-Risk-Streaming") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.cassandra.connection.host", cassandra_host) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .getOrCreate()
        
        self.kafka_servers = kafka_servers
        self.hdfs_path = hdfs_path
        
        logger.info(f"Initialized Spark with Kafka: {kafka_servers}, HDFS: {hdfs_path}")
    
    def get_tle_schema(self):
        """Schema for TLE data from Kafka."""
        return StructType([
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
        ])
    
    @staticmethod
    def calculate_sgp4_vectors(tle_line1, tle_line2, epoch_str):
        """
        Calculate position and velocity vectors using SGP4.
        
        Returns: (position_x, position_y, position_z, velocity_x, velocity_y, velocity_z, error_code)
        """
        try:
            # Parse TLE
            satellite = Satrec.twoline2rv(tle_line1, tle_line2)
            
            # Parse epoch time
            epoch = datetime.fromisoformat(epoch_str.replace('+00:00', '')).replace(tzinfo=timezone.utc)
            jd, fr = jday(epoch.year, epoch.month, epoch.day, 
                         epoch.hour, epoch.minute, epoch.second + epoch.microsecond/1e6)
            
            # Propagate to current time
            error_code, position, velocity = satellite.sgp4(jd, fr)
            
            if error_code == 0:
                return (
                    float(position[0]), float(position[1]), float(position[2]),
                    float(velocity[0]), float(velocity[1]), float(velocity[2]),
                    error_code
                )
            else:
                return (None, None, None, None, None, None, error_code)
                
        except Exception as e:
            logger.error(f"SGP4 calculation error: {e}")
            return (None, None, None, None, None, None, -1)
    
    @staticmethod
    def calculate_collision_risk(pos_x, pos_y, pos_z, vel_x, vel_y, vel_z, 
                                 inclination, eccentricity, mean_motion):
        """
        Calculate collision risk score based on orbital parameters and vectors.
        
        Risk factors:
        - Altitude (LEO is more crowded)
        - Velocity magnitude (higher velocity = higher impact energy)
        - Inclination (polar orbits cross more paths)
        - Eccentricity (highly elliptical orbits are unpredictable)
        """
        try:
            # Calculate altitude from position vector (km)
            altitude = np.sqrt(pos_x**2 + pos_y**2 + pos_z**2) - 6371.0  # Earth radius
            
            # Velocity magnitude (km/s)
            velocity_mag = np.sqrt(vel_x**2 + vel_y**2 + vel_z**2)
            
            # Risk components (normalized 0-1)
            altitude_risk = 1.0 / (1.0 + np.exp((altitude - 2000) / 500))  # Higher risk in LEO
            velocity_risk = min(velocity_mag / 15.0, 1.0)  # Cap at 15 km/s
            inclination_risk = abs(np.sin(np.radians(inclination)))  # Polar orbits = higher risk
            eccentricity_risk = min(float(eccentricity), 1.0)
            
            # Weighted risk score
            risk_score = (
                0.35 * altitude_risk +
                0.25 * velocity_risk +
                0.25 * inclination_risk +
                0.15 * eccentricity_risk
            )
            
            # Classify risk level
            if risk_score >= 0.7:
                risk_level = "HIGH"
            elif risk_score >= 0.4:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"
            
            return float(risk_score), risk_level, float(altitude), float(velocity_mag)
            
        except Exception as e:
            logger.error(f"Risk calculation error: {e}")
            return (0.0, "UNKNOWN", 0.0, 0.0)
    
    def start_streaming(self):
        """Main streaming pipeline."""
        
        # 1. Read from Kafka
        tle_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "space_debris_tle") \
            .option("startingOffsets", "latest") \
            .load()
        
        # 2. Parse JSON
        tle_data = tle_stream.select(
            from_json(col("value").cast("string"), self.get_tle_schema()).alias("data")
        ).select("data.*")
        
        # 3. Register UDFs for SGP4 and Risk calculations
        sgp4_udf = udf(self.calculate_sgp4_vectors, 
                       StructType([
                           StructField("pos_x", DoubleType()),
                           StructField("pos_y", DoubleType()),
                           StructField("pos_z", DoubleType()),
                           StructField("vel_x", DoubleType()),
                           StructField("vel_y", DoubleType()),
                           StructField("vel_z", DoubleType()),
                           StructField("error_code", IntegerType()),
                       ]))
        
        risk_udf = udf(self.calculate_collision_risk,
                      StructType([
                          StructField("risk_score", DoubleType()),
                          StructField("risk_level", StringType()),
                          StructField("altitude_km", DoubleType()),
                          StructField("velocity_kms", DoubleType()),
                      ]))
        
        # 4. Calculate SGP4 vectors
        vectors_df = tle_data.withColumn(
            "vectors",
            sgp4_udf(col("tle_line1"), col("tle_line2"), col("epoch"))
        ).select(
            col("satellite_id"),
            col("epoch"),
            col("vectors.pos_x").alias("position_x"),
            col("vectors.pos_y").alias("position_y"),
            col("vectors.pos_z").alias("position_z"),
            col("vectors.vel_x").alias("velocity_x"),
            col("vectors.vel_y").alias("velocity_y"),
            col("vectors.vel_z").alias("velocity_z"),
            col("vectors.error_code"),
            col("inclination"),
            col("eccentricity"),
            col("mean_motion"),
            current_timestamp().alias("processing_time")
        ).filter(col("error_code") == 0)  # Only successful calculations
        
        # 5. Calculate risk scores
        risk_df = vectors_df.withColumn(
            "risk_metrics",
            risk_udf(
                col("position_x"), col("position_y"), col("position_z"),
                col("velocity_x"), col("velocity_y"), col("velocity_z"),
                col("inclination"), col("eccentricity"), col("mean_motion")
            )
        ).select(
            col("satellite_id"),
            col("epoch"),
            col("position_x"), col("position_y"), col("position_z"),
            col("velocity_x"), col("velocity_y"), col("velocity_z"),
            col("risk_metrics.risk_score"),
            col("risk_metrics.risk_level"),
            col("risk_metrics.altitude_km"),
            col("risk_metrics.velocity_kms"),
            col("processing_time")
        )
        
        # 6. Dual Write: HDFS (vectors) + Cassandra (risk scores)
        
        # Write vectors to HDFS (Parquet format for ML)
        hdfs_query = vectors_df \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"{self.hdfs_path}/sgp4_vectors") \
            .option("checkpointLocation", "/tmp/spark-checkpoint/hdfs") \
            .start()
        
        # Write risk scores to Cassandra (for real-time queries)
        cassandra_query = risk_df \
            .writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "space_debris") \
            .option("table", "risk_scores") \
            .option("checkpointLocation", "/tmp/spark-checkpoint/cassandra") \
            .start()
        
        # Console output for monitoring (optional)
        console_query = risk_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 5) \
            .start()
        
        logger.info("Streaming pipeline started...")
        logger.info(f"  → Writing vectors to HDFS: {self.hdfs_path}/sgp4_vectors")
        logger.info(f"  → Writing risk scores to Cassandra: space_debris.risk_scores")
        
        # Wait for termination
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='SGP4 + Risk Analysis Streaming')
    parser.add_argument('--kafka', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--hdfs', default='hdfs://localhost:9000/space-debris', help='HDFS path')
    parser.add_argument('--cassandra', default='localhost', help='Cassandra host')
    
    args = parser.parse_args()
    
    processor = SGP4StreamProcessor(
        kafka_servers=args.kafka,
        hdfs_path=args.hdfs,
        cassandra_host=args.cassandra
    )
    
    processor.start_streaming()
