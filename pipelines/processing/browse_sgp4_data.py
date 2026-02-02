#!/usr/bin/env python3
"""
Quick script to browse SGP4 vectors stored in parquet format
Run: docker cp this file into spark container and execute
"""

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("Browse_SGP4_Data") \
    .getOrCreate()

# Read the parquet data
df = spark.read.parquet("/space-debris/sgp4_vectors/")

print("=" * 80)
print("SGP4 VECTORS DATA SUMMARY")
print("=" * 80)

# Show schema
print("\n📊 Schema:")
df.printSchema()

# Count records
total = df.count()
print(f"\n📈 Total Records: {total:,}")

# Show sample data
print("\n📝 Sample Data (10 rows):")
df.show(10, truncate=False)

# Statistics
print("\n📐 Statistics:")
df.select('altitude_km', 'velocity_magnitude_kms').describe().show()

# Group by epoch time
print("\n⏰ Records by Epoch Time:")
df.groupBy('epoch_time').count().orderBy('count', ascending=False).show(20)

# Altitude distribution
print("\n🛰️ Altitude Distribution:")
df.groupBy('altitude_km').count().orderBy('altitude_km').show(20)

spark.stop()
