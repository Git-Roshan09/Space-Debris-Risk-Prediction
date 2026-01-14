"""
Spark Streaming Consumer for Space Debris TLE Data
Consumes TLE and debris catalog data from Kafka and performs real-time analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min,
    count, to_timestamp, current_timestamp, expr, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpaceDebrisStreamProcessor:
    """Process space debris TLE data from Kafka using Spark Structured Streaming."""
    
    def __init__(self, kafka_bootstrap_servers='broker:29092', 
                 checkpoint_location='/tmp/spark-checkpoint'):
        """
        Initialize Spark session and configuration.
        
        Args:
            kafka_bootstrap_servers: Kafka broker address
            checkpoint_location: Directory for Spark checkpointing
        """
        self.kafka_servers = kafka_bootstrap_servers
        self.checkpoint_location = checkpoint_location
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("SpaceDebrisRiskStreaming") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized for space debris streaming")
    
    def get_tle_schema(self):
        """Define schema for TLE data from Kafka."""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("message_timestamp", StringType(), True),
            StructField("satellite_id", StringType(), True),
            StructField("epoch", StringType(), True),
            StructField("tle_line1", StringType(), True),
            StructField("tle_line2", StringType(), True),
            StructField("inclination", DoubleType(), True),
            StructField("eccentricity", StringType(), True),
            StructField("mean_motion", DoubleType(), True)
        ])
    
    def get_debris_catalog_schema(self):
        """Define schema for debris catalog data from Kafka."""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("message_timestamp", StringType(), True),
            StructField("data", StructType([
                StructField("norad_cat_id", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("launch_site", StringType(), True),
                StructField("decay_date", StringType(), True),
                StructField("period", StringType(), True),
                StructField("inclination", StringType(), True),
                StructField("apogee", StringType(), True),
                StructField("perigee", StringType(), True),
                StructField("rcs_size", StringType(), True)
            ]), True)
        ])
    
    def read_tle_stream(self):
        """Read TLE data stream from Kafka."""
        logger.info("Starting TLE stream consumption from Kafka")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "space_debris_tle") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        tle_schema = self.get_tle_schema()
        tle_df = kafka_df.select(
            from_json(col("value").cast("string"), tle_schema).alias("data")
        ).select("data.*")
        
        # Convert timestamps
        tle_df = tle_df.withColumn(
            "epoch_timestamp", 
            to_timestamp(col("epoch"))
        ).withColumn(
            "message_timestamp_parsed",
            to_timestamp(col("message_timestamp"))
        )
        
        return tle_df
    
    def read_debris_catalog_stream(self):
        """Read debris catalog stream from Kafka."""
        logger.info("Starting debris catalog stream consumption from Kafka")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "space_debris_catalog") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        catalog_schema = self.get_debris_catalog_schema()
        catalog_df = kafka_df.select(
            from_json(col("value").cast("string"), catalog_schema).alias("parsed")
        ).select("parsed.*", "parsed.data.*")
        
        return catalog_df
    
    def calculate_risk_metrics(self, tle_df):
        """
        Calculate risk metrics from TLE data.
        
        Risk factors considered:
        - Low altitude (higher decay risk)
        - High eccentricity (unstable orbit)
        - High inclination (more collision opportunities)
        - Orbital decay rate
        """
        
        # Add risk score calculation
        risk_df = tle_df.withColumn(
            "risk_score",
            expr("""
                CASE 
                    WHEN inclination > 80 THEN 0.4
                    WHEN inclination > 60 THEN 0.3
                    WHEN inclination > 40 THEN 0.2
                    ELSE 0.1
                END +
                CASE
                    WHEN mean_motion > 14 THEN 0.3  -- Lower orbit, higher risk
                    WHEN mean_motion > 12 THEN 0.2
                    ELSE 0.1
                END +
                CASE
                    WHEN CAST(SUBSTRING(eccentricity, 3, 7) AS DOUBLE) / 10000000 > 0.1 THEN 0.3
                    ELSE 0.1
                END
            """)
        )
        
        # Classify risk level
        risk_df = risk_df.withColumn(
            "risk_level",
            when(col("risk_score") >= 0.7, "HIGH")
            .when(col("risk_score") >= 0.5, "MEDIUM")
            .otherwise("LOW")
        )
        
        return risk_df
    
    def aggregate_risk_statistics(self, risk_df):
        """
        Aggregate risk statistics over time windows.
        """
        
        # Aggregate by 5-minute windows
        windowed_stats = risk_df \
            .withWatermark("message_timestamp_parsed", "10 minutes") \
            .groupBy(
                window(col("message_timestamp_parsed"), "5 minutes"),
                col("risk_level")
            ).agg(
                count("*").alias("object_count"),
                avg("risk_score").alias("avg_risk_score"),
                avg("inclination").alias("avg_inclination"),
                avg("mean_motion").alias("avg_mean_motion")
            )
        
        return windowed_stats
    
    def write_to_console(self, df, output_mode="append", query_name="console_output"):
        """Write streaming results to console for debugging."""
        query = df.writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", "false") \
            .queryName(query_name) \
            .start()
        
        return query
    
    def write_to_parquet(self, df, output_path, checkpoint_path, query_name="parquet_output"):
        """Write streaming results to Parquet files."""
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .queryName(query_name) \
            .start()
        
        return query
    
    def run_tle_analysis(self):
        """Run complete TLE analysis pipeline."""
        logger.info("Starting TLE analysis pipeline")
        
        # Read TLE stream
        tle_df = self.read_tle_stream()
        
        # Calculate risk metrics
        risk_df = self.calculate_risk_metrics(tle_df)
        
        # Aggregate statistics
        stats_df = self.aggregate_risk_statistics(risk_df)
        
        # Write detailed risk data to console
        query1 = self.write_to_console(
            risk_df.select(
                "satellite_id", "epoch", "inclination", 
                "mean_motion", "risk_score", "risk_level"
            ),
            query_name="tle_risk_analysis"
        )
        
        # Write aggregated statistics
        query2 = self.write_to_console(
            stats_df,
            output_mode="complete",
            query_name="risk_statistics"
        )
        
        # Optional: Write to parquet for persistence
        # query3 = self.write_to_parquet(
        #     risk_df,
        #     output_path="/tmp/space_debris_risk",
        #     checkpoint_path="/tmp/space_debris_risk_checkpoint",
        #     query_name="parquet_risk_data"
        # )
        
        return [query1, query2]
    
    def run_catalog_analysis(self):
        """Run debris catalog analysis."""
        logger.info("Starting debris catalog analysis pipeline")
        
        # Read catalog stream
        catalog_df = self.read_debris_catalog_stream()
        
        # Analyze by object type and country
        type_stats = catalog_df \
            .groupBy("object_type", "country") \
            .agg(
                count("*").alias("object_count")
            ) \
            .orderBy(col("object_count").desc())
        
        # Write to console
        query = self.write_to_console(
            type_stats,
            output_mode="complete",
            query_name="catalog_analysis"
        )
        
        return [query]
    
    def stop(self):
        """Stop Spark session."""
        logger.info("Stopping Spark session")
        self.spark.stop()


def main():
    """Main function to run the streaming application."""
    processor = SpaceDebrisStreamProcessor()
    
    try:
        # Start TLE analysis
        tle_queries = processor.run_tle_analysis()
        
        # Start catalog analysis
        # catalog_queries = processor.run_catalog_analysis()
        
        # Wait for termination
        logger.info("Streaming queries running. Press Ctrl+C to stop.")
        for query in tle_queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Stopping streaming application")
    except Exception as e:
        logger.error(f"Error in streaming application: {e}")
        raise
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
