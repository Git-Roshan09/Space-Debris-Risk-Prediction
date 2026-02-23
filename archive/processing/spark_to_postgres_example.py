"""
Example: PySpark writing to PostgreSQL
Demonstrates best practices for Spark → PostgreSQL integration
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, count, current_timestamp
from postgres_utils import get_postgres_connector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def update_satellite_metadata(spark, pg):
    """
    Example: Update satellite tracking status in PostgreSQL
    Reads SGP4 vectors from HDFS, aggregates metadata, writes to PostgreSQL.
    """
    logger.info("Updating satellite metadata in PostgreSQL...")
    
    # Read latest SGP4 data from HDFS
    sgp4_df = spark.read.parquet("hdfs://namenode:9000/space-debris/sgp4_vectors")
    
    # Aggregate latest status per satellite
    satellite_metadata = sgp4_df.groupBy("satellite_id") \
        .agg(
            spark_max("epoch_time").alias("last_tle_epoch"),
            spark_max("altitude_km").alias("last_altitude_km"),
            spark_max("sgp4_error_code").alias("last_sgp4_error_code"),
            count("*").alias("total_observations")
        ) \
        .withColumn("status_updated_at", current_timestamp())
    
    # Determine tracking status
    satellite_metadata = satellite_metadata.withColumn(
        "tracking_status",
        when(col("last_sgp4_error_code") != 0, "STOPPED_SGP4_ERROR")
        .when(col("last_altitude_km") < 150, "STOPPED_LOW_ALTITUDE")
        .otherwise("ACTIVE")
    )
    
    # Write to PostgreSQL (upsert/overwrite mode)
    logger.info(f"Writing {satellite_metadata.count()} satellite records to PostgreSQL")
    pg.write_table(
        satellite_metadata,
        table_name="satellite_status",
        mode="overwrite",
        batch_size=1000
    )
    
    logger.info("✓ Satellite metadata updated in PostgreSQL")


def write_collision_alerts(spark, pg):
    """
    Example: Write high-priority collision alerts to PostgreSQL
    Only writes HIGH and MEDIUM risk collisions for dashboard.
    """
    logger.info("Writing collision alerts to PostgreSQL...")
    
    # Read collision predictions from HDFS
    collisions_df = spark.read.parquet(
        "hdfs://namenode:9000/space-debris/collision_predictions"
    )
    
    # Filter only recent high/medium risk collisions
    from pyspark.sql.functions import lit, datediff
    
    alerts_df = collisions_df \
        .filter(col("risk_level").isin(["HIGH", "MEDIUM"])) \
        .filter(datediff(col("predicted_time"), current_timestamp()) <= 7) \
        .withColumn("is_active", lit(True)) \
        .withColumn("detected_at", current_timestamp())
    
    # Write to PostgreSQL (append new alerts)
    logger.info(f"Writing {alerts_df.count()} collision alerts to PostgreSQL")
    pg.write_table(
        alerts_df,
        table_name="collision_alerts",
        mode="append",
        batch_size=500
    )
    
    logger.info("✓ Collision alerts written to PostgreSQL")


def read_from_postgres_example(spark, pg):
    """
    Example: Read data from PostgreSQL into Spark for processing
    """
    logger.info("Reading active satellites from PostgreSQL...")
    
    # Read active satellites
    active_satellites = pg.read_query(
        spark,
        query="""
            SELECT norad_id, name, last_altitude_km 
            FROM satellites 
            WHERE tracking_status = 'ACTIVE'
        """
    )
    
    logger.info(f"Found {active_satellites.count()} active satellites")
    active_satellites.show(10)
    
    return active_satellites


def main():
    """
    Main job: Process HDFS data and update PostgreSQL metadata.
    This is a BATCH job, not streaming.
    """
    # Initialize Spark with PostgreSQL JDBC driver
    spark = SparkSession.builder \
        .appName("Spark-PostgreSQL-Integration") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized with PostgreSQL support")
    
    # Initialize PostgreSQL connector
    pg = get_postgres_connector()
    
    try:
        # Example 1: Update satellite metadata
        update_satellite_metadata(spark, pg)
        
        # Example 2: Write collision alerts
        write_collision_alerts(spark, pg)
        
        # Example 3: Read from PostgreSQL
        read_from_postgres_example(spark, pg)
        
        logger.info("✓ All PostgreSQL operations completed successfully")
        
    except Exception as e:
        logger.error(f"Error during PostgreSQL operations: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
