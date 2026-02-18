"""
Dashboard API - Serves collision prediction and space debris monitoring data
Provides real-time data for the visualization dashboard
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, avg, min as spark_min, max as spark_max
from datetime import datetime, timedelta
import os
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Configuration
HDFS_COLLISION_PATH = os.getenv('HDFS_COLLISION_PREDICTIONS_PATH', 
                                 'hdfs://namenode:9000/space-debris/collision_predictions')
HDFS_SGP4_PATH = os.getenv('HDFS_SGP4_VECTORS_PATH',
                           'hdfs://namenode:9000/space-debris/sgp4_vectors')


class DashboardDataProvider:
    """Provides data for the dashboard by querying HDFS."""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Dashboard-API") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized for Dashboard API")
    
    def get_collision_alerts(self, limit=100):
        """Get recent collision predictions."""
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            # Get most recent collisions
            df_recent = df.orderBy(desc("detection_timestamp")) \
                         .limit(limit)
            
            return [row.asDict() for row in df_recent.collect()]
        except Exception as e:
            logger.error(f"Error reading collision data: {e}")
            return []
    
    def get_collision_stats(self):
        """Get statistical summary of collision predictions."""
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            # Overall statistics
            total_collisions = df.count()
            
            # By risk level
            risk_counts = df.groupBy("risk_level").count().collect()
            risk_stats = {row['risk_level']: row['count'] for row in risk_counts}
            
            # Distance statistics
            distance_stats = df.select(
                spark_min("distance_km").alias("min_distance"),
                spark_max("distance_km").alias("max_distance"),
                avg("distance_km").alias("avg_distance")
            ).first()
            
            # Time range
            time_range = df.select(
                spark_min("detection_timestamp").alias("earliest"),
                spark_max("detection_timestamp").alias("latest")
            ).first()
            
            return {
                'total_collisions': total_collisions,
                'risk_distribution': risk_stats,
                'distance_stats': {
                    'min_km': float(distance_stats['min_distance']) if distance_stats['min_distance'] else 0,
                    'max_km': float(distance_stats['max_distance']) if distance_stats['max_distance'] else 0,
                    'avg_km': float(distance_stats['avg_distance']) if distance_stats['avg_distance'] else 0
                },
                'time_range': {
                    'earliest': str(time_range['earliest']) if time_range['earliest'] else None,
                    'latest': str(time_range['latest']) if time_range['latest'] else None
                }
            }
        except Exception as e:
            logger.error(f"Error computing collision stats: {e}")
            return {}
    
    def get_high_risk_collisions(self):
        """Get only high-risk collision alerts."""
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            df_high_risk = df.filter(col("risk_level") == "HIGH") \
                            .orderBy(desc("detection_timestamp")) \
                            .limit(50)
            
            return [row.asDict() for row in df_high_risk.collect()]
        except Exception as e:
            logger.error(f"Error reading high-risk collisions: {e}")
            return []
    
    def get_satellite_tracking(self, norad_id=None):
        """Get tracking data for satellites."""
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_SGP4_PATH}/batch_*")
            
            if norad_id:
                df = df.filter(col("norad_id") == norad_id)
            
            df_recent = df.orderBy(desc("timestamp")).limit(1000)
            
            return [row.asDict() for row in df_recent.collect()]
        except Exception as e:
            logger.error(f"Error reading satellite tracking data: {e}")
            return []
    
    def get_collision_timeline(self, days=7):
        """Get collision predictions grouped by time."""
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            # Filter recent predictions
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            df_recent = df.filter(col("detection_timestamp") >= cutoff)
            
            # Group by hour
            timeline = df_recent.groupBy("detection_timestamp", "risk_level") \
                               .agg(count("*").alias("collision_count")) \
                               .orderBy("detection_timestamp") \
                               .collect()
            
            return [row.asDict() for row in timeline]
        except Exception as e:
            logger.error(f"Error generating collision timeline: {e}")
            return []
    
    def get_satellite_pairs(self):
        """Get most frequently colliding satellite pairs."""
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            pairs = df.groupBy("satellite_1", "satellite_2") \
                     .agg(
                         count("*").alias("collision_count"),
                         spark_min("distance_km").alias("min_distance"),
                         avg("distance_km").alias("avg_distance")
                     ) \
                     .orderBy(desc("collision_count")) \
                     .limit(20) \
                     .collect()
            
            return [row.asDict() for row in pairs]
        except Exception as e:
            logger.error(f"Error reading satellite pairs: {e}")
            return []


# Initialize data provider
data_provider = DashboardDataProvider()


# API Endpoints
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'collision-dashboard-api'
    })


@app.route('/api/collisions', methods=['GET'])
def get_collisions():
    """Get collision predictions."""
    limit = request.args.get('limit', 100, type=int)
    collisions = data_provider.get_collision_alerts(limit)
    return jsonify({
        'count': len(collisions),
        'data': collisions
    })


@app.route('/api/collisions/stats', methods=['GET'])
def get_stats():
    """Get collision statistics."""
    stats = data_provider.get_collision_stats()
    return jsonify(stats)


@app.route('/api/collisions/high-risk', methods=['GET'])
def get_high_risk():
    """Get high-risk collision alerts."""
    collisions = data_provider.get_high_risk_collisions()
    return jsonify({
        'count': len(collisions),
        'data': collisions
    })


@app.route('/api/collisions/timeline', methods=['GET'])
def get_timeline():
    """Get collision timeline."""
    days = request.args.get('days', 7, type=int)
    timeline = data_provider.get_collision_timeline(days)
    return jsonify({
        'count': len(timeline),
        'data': timeline
    })


@app.route('/api/satellites/tracking', methods=['GET'])
def get_tracking():
    """Get satellite tracking data."""
    norad_id = request.args.get('norad_id', None)
    tracking = data_provider.get_satellite_tracking(norad_id)
    return jsonify({
        'count': len(tracking),
        'data': tracking
    })


@app.route('/api/satellites/pairs', methods=['GET'])
def get_pairs():
    """Get frequently colliding satellite pairs."""
    pairs = data_provider.get_satellite_pairs()
    return jsonify({
        'count': len(pairs),
        'data': pairs
    })


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get dashboard configuration."""
    return jsonify({
        'prediction_days': int(os.getenv('PREDICTION_DAYS', '7')),
        'collision_threshold_km': float(os.getenv('COLLISION_THRESHOLD_KM', '10.0')),
        'time_window_days': int(os.getenv('TIME_WINDOW_DAYS', '7')),
        'update_interval_seconds': int(os.getenv('DASHBOARD_UPDATE_INTERVAL_SECONDS', '30')),
        'high_risk_threshold_km': float(os.getenv('HIGH_RISK_THRESHOLD_KM', '5.0')),
        'medium_risk_threshold_km': float(os.getenv('MEDIUM_RISK_THRESHOLD_KM', '10.0')),
        'low_risk_threshold_km': float(os.getenv('LOW_RISK_THRESHOLD_KM', '50.0'))
    })


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', '5001'))
    logger.info(f"Starting Dashboard API on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
