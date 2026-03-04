"""
Space Debris Catalog Fetcher - PySpark + HDFS Direct Ingestion
Fetches debris catalog from Space-Track.org and writes directly to HDFS using PySpark.
"""

import os
import json
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# =============================
# CONFIG
# =============================
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

# HDFS Configuration
# Use localhost:9000 when running PySpark on host (outside Docker)
# Port 9000 is exposed from Docker's namenode container
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_RPC_PORT", "9000")
HDFS_BASE_PATH = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/space-debris"

def create_spark_session():
    """
    Create a Spark session configured for HDFS.
    """
    spark = SparkSession.builder \
        .appName("SpaceDebris-Catalog-Ingestion") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}") \
        .getOrCreate()
    
    # Reduce logging noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def main():
    print("🚀 Initializing Spark Session...")
    spark = create_spark_session()
    
    # Initialize Space-Track client
    st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)
    
    print("📡 Requesting debris catalog from Space-Track.org...")
    
    try:
        # Fetch debris objects from SATCAT
        response = st.satcat(
            object_type="DEBRIS",
            current="Y",
            orderby="launch asc",
            format="json"
        )
        
        # Parse response
        data = json.loads(response) if isinstance(response, str) else response
        
        if not data:
            print("❌ No debris records found.")
            spark.stop()
            return
        
        print(f"📦 Retrieved {len(data)} debris objects from Space-Track")
        
        # Define schema for the catalog
        schema = StructType([
            StructField("NORAD_CAT_ID", StringType(), True),
            StructField("OBJECT_NAME", StringType(), True),
            StructField("OBJECT_TYPE", StringType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("LAUNCH", StringType(), True),
            StructField("SITE", StringType(), True),
            StructField("DECAY", StringType(), True),
            StructField("PERIOD", StringType(), True),
            StructField("INCLINATION", StringType(), True),
            StructField("APOGEE", StringType(), True),
            StructField("PERIGEE", StringType(), True),
            StructField("RCS_SIZE", StringType(), True),
        ])
        
        # Filter to keep only required columns
        keep_columns = [
            "NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "COUNTRY",
            "LAUNCH", "SITE", "DECAY", "PERIOD", "INCLINATION",
            "APOGEE", "PERIGEE", "RCS_SIZE"
        ]
        
        filtered_data = []
        for record in data:
            filtered_record = {col: record.get(col) for col in keep_columns}
            filtered_data.append(filtered_record)
        
        # Create Spark DataFrame
        df = spark.createDataFrame(filtered_data, schema=schema)
        
        # Cast numeric columns
        df = df.withColumn("PERIOD", df["PERIOD"].cast(DoubleType())) \
               .withColumn("INCLINATION", df["INCLINATION"].cast(DoubleType())) \
               .withColumn("APOGEE", df["APOGEE"].cast(DoubleType())) \
               .withColumn("PERIGEE", df["PERIGEE"].cast(DoubleType()))
        
        # Show sample
        print("\n📋 Sample data:")
        df.show(5, truncate=False)
        
        # Write to HDFS
        hdfs_path = f"{HDFS_BASE_PATH}/catalog"
        print(f"\n💾 Writing to HDFS: {hdfs_path}")
        
        df.coalesce(1) \
          .write \
          .mode("overwrite") \
          .option("header", "true") \
          .csv(hdfs_path)
        
        print(f"✅ Successfully wrote {df.count()} records to HDFS!")
        print(f"📁 Location: {hdfs_path}")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        raise
    
    finally:
        spark.stop()
        print("🛑 Spark session stopped")

if __name__ == "__main__":
    main()
