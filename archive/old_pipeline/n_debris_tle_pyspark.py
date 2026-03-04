"""
TLE History Fetcher - PySpark + HDFS Direct Ingestion
Fetches TLE history from Space-Track.org and writes directly to HDFS using PySpark.
"""

import os
import json
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp
from time import sleep

# =============================
# CONFIG
# =============================
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

N_DEBRIS = int(os.getenv("N_DEBRIS", 10))
SLEEP_SEC = 2  # Space-Track rate limit

# HDFS Configuration
# Use localhost:9000 when running PySpark on host (outside Docker)
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_RPC_PORT", "9000")
HDFS_BASE_PATH = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/space-debris"

def create_spark_session():
    """
    Create a Spark session configured for HDFS.
    """
    spark = SparkSession.builder \
        .appName("SpaceDebris-TLE-Ingestion") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def check_path_exists(spark, hdfs_path):
    """Check if a path exists in HDFS using Spark's Hadoop FileSystem."""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI.create(hdfs_path),
            hadoop_conf
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        return fs.exists(path)
    except Exception:
        return False

def load_catalog_from_hdfs(spark):
    """Load the debris catalog from HDFS."""
    catalog_path = f"{HDFS_BASE_PATH}/catalog"
    
    try:
        df = spark.read.option("header", "true").csv(catalog_path)
        return df.toPandas()
    except Exception as e:
        raise FileNotFoundError(f"Catalog not found in HDFS. Run debris_pyspark.py first! Error: {e}")

def main():
    print("🚀 Initializing Spark Session...")
    spark = create_spark_session()
    
    # Initialize Space-Track client
    st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)
    
    print("📦 Loading debris catalog from HDFS...")
    
    try:
        df_catalog = load_catalog_from_hdfs(spark)
    except FileNotFoundError as e:
        print(f"❌ {e}")
        spark.stop()
        return
    
    # Get NORAD IDs
    norad_ids = df_catalog["NORAD_CAT_ID"].dropna().astype(int).unique()[:N_DEBRIS]
    
    print(f"🛰️ Fetching TLE history for {len(norad_ids)} debris objects → HDFS\n")
    
    # Schema for TLE data
    tle_schema = StructType([
        StructField("EPOCH", StringType(), True),
        StructField("TLE_LINE1", StringType(), True),
        StructField("TLE_LINE2", StringType(), True),
    ])
    
    for i, norad_id in enumerate(norad_ids, start=1):
        tle_hdfs_path = f"{HDFS_BASE_PATH}/tle-history/{norad_id}"
        
        # Skip if already exists
        if check_path_exists(spark, tle_hdfs_path):
            print(f"[{i}/{len(norad_ids)}] ⏭️ NORAD {norad_id} — already in HDFS, skipping")
            continue
        
        print(f"[{i}/{len(norad_ids)}] 📡 Fetching NORAD {norad_id}...", end=" ")
        
        try:
            response = st.gp_history(
                norad_cat_id=int(norad_id),
                orderby="epoch asc",
                format="json"
            )
            
            data = json.loads(response) if isinstance(response, str) else response
            
            if not data:
                print("⚠️ No TLEs found")
                continue
            
            # Extract required fields
            tle_records = []
            for record in data:
                if all(k in record for k in ["EPOCH", "TLE_LINE1", "TLE_LINE2"]):
                    tle_records.append({
                        "EPOCH": record["EPOCH"],
                        "TLE_LINE1": record["TLE_LINE1"],
                        "TLE_LINE2": record["TLE_LINE2"]
                    })
            
            if not tle_records:
                print("⚠️ No valid TLE records")
                continue
            
            # Create Spark DataFrame
            df_tle = spark.createDataFrame(tle_records, schema=tle_schema)
            
            # Remove duplicates
            df_tle = df_tle.dropDuplicates()
            
            # Write to HDFS
            df_tle.coalesce(1) \
                  .write \
                  .mode("overwrite") \
                  .option("header", "true") \
                  .csv(tle_hdfs_path)
            
            print(f"✅ {df_tle.count()} TLEs → HDFS")
            
            sleep(SLEEP_SEC)
            
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n🎯 Finished TLE ingestion to HDFS!")
    spark.stop()
    print("🛑 Spark session stopped")

if __name__ == "__main__":
    main()
