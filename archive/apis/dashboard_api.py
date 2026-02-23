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
CORS(app)

HDFS_COLLISION_PATH = os.getenv('HDFS_COLLISION_PREDICTIONS_PATH', 
                                 'hdfs://namenode:9000/space-debris/collision_predictions')
HDFS_SGP4_PATH = os.getenv('HDFS_SGP4_VECTORS_PATH',
                           'hdfs://namenode:9000/space-debris/sgp4_vectors')


class DashboardDataProvider:
    """
    Provides collision prediction and satellite tracking data for dashboard visualization.
    
    Queries HDFS to retrieve collision predictions, SGP4 vectors, and statistical summaries
    for real-time space debris monitoring dashboard.
    """
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Dashboard-API") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized for Dashboard API")
    
    def get_collision_alerts(self, limit=100):
        """
        Retrieve recent collision predictions from HDFS storage.
        
        Args:
            limit (int): Maximum number of collision records to return (default: 100)
            
        Returns:
            list: Array of collision prediction dictionaries with detection timestamps,
                  satellite pairs, distances, and risk levels
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            df_recent = df.orderBy(desc("detection_timestamp")) \
                         .limit(limit)
            
            return [row.asDict() for row in df_recent.collect()]
        except Exception as e:
            logger.error(f"Error reading collision data: {e}")
            return []
    
    def get_collision_stats(self):
        """
        Generate comprehensive statistical summary of collision predictions.
        
        Returns:
            dict: Statistics including total collision count, risk level distribution,
                  distance statistics (min/max/avg), and time range of predictions
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            total_collisions = df.count()
            
            risk_counts = df.groupBy("risk_level").count().collect()
            risk_stats = {row['risk_level']: row['count'] for row in risk_counts}
            
            distance_stats = df.select(
                spark_min("distance_km").alias("min_distance"),
                spark_max("distance_km").alias("max_distance"),
                avg("distance_km").alias("avg_distance")
            ).first()
            
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
        """
        Retrieve only high-risk collision alerts for priority monitoring.
        
        Returns:
            list: Array of HIGH risk collision predictions ordered by detection time
        """
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
        """
        Retrieve satellite tracking data and SGP4 propagation vectors.
        
        Args:
            norad_id (int, optional): Filter by specific NORAD catalog ID
            
        Returns:
            list: Array of satellite tracking records with positions, velocities, and timestamps
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_SGP4_PATH}/batch_*")
            
            if norad_id:
                df = df.filter(col("norad_id") == norad_id)
            
            df_recent = df.orderBy(desc("timestamp")).limit(1000)
            
            return [row.asDict() for row in df_recent.collect()]
        except Exception as e:
            logger.error(f"Error reading satellite tracking data: {e}")
            return []
    
    def get_collision_timeline(self, days=7):
        """
        Generate collision prediction timeline grouped by time periods.
        
        Args:
            days (int): Number of days to include in timeline (default: 7)
            
        Returns:
            list: Array of time-grouped collision counts with timestamps and risk levels
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            df_recent = df.filter(col("detection_timestamp") >= cutoff)
            
            timeline = df_recent.groupBy("detection_timestamp", "risk_level") \
                               .agg(count("*").alias("collision_count")) \
                               .orderBy("detection_timestamp") \
                               .collect()
            
            return [row.asDict() for row in timeline]
        except Exception as e:
            logger.error(f"Error generating collision timeline: {e}")
            return []
    
    def get_satellite_pairs(self):
        """
        Identify satellite pairs with the highest collision frequency.
        
        Returns:
            list: Array of satellite pairs with collision counts, minimum distances,
                  and average distances, ordered by collision frequency
        """
        try:
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


data_provider = DashboardDataProvider()


@app.route('/api/health', methods=['GET'])
def health_check():
    """
    Health check endpoint to verify API and Spark connectivity.
    
    Returns:
        JSON response with service status and timestamp
    """
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'collision-dashboard-api'
    })


@app.route('/api/collisions', methods=['GET'])
def get_collisions():
    """
    Get collision predictions with optional limit parameter.
    
    Query Parameters:
        limit (int): Maximum number of collision records to return (default: 100)
    
    Returns:
        JSON response with collision count and array of collision data
    """
    limit = request.args.get('limit', 100, type=int)
    collisions = data_provider.get_collision_alerts(limit)
    return jsonify({
        'count': len(collisions),
        'data': collisions
    })


@app.route('/api/collisions/stats', methods=['GET'])
def get_stats():
    """
    Get comprehensive collision prediction statistics.
    
    Returns:
        JSON response with collision statistics including counts by risk level,
        distance statistics, and time range coverage
    """
    stats = data_provider.get_collision_stats()
    return jsonify(stats)


@app.route('/api/collisions/high-risk', methods=['GET'])
def get_high_risk():
    """
    Get high-risk collision alerts for priority monitoring.
    
    Returns:
        JSON response with count and array of high-risk collision predictions
    """
    collisions = data_provider.get_high_risk_collisions()
    return jsonify({
        'count': len(collisions),
        'data': collisions
    })


@app.route('/api/collisions/timeline', methods=['GET'])
def get_timeline():
    """
    Get collision prediction timeline for trend analysis.
    
    Query Parameters:
        days (int): Number of days to include in timeline (default: 7)
    
    Returns:
        JSON response with timeline data grouped by time periods and risk levels
    """
    days = request.args.get('days', 7, type=int)
    timeline = data_provider.get_collision_timeline(days)
    return jsonify({
        'count': len(timeline),
        'data': timeline
    })


@app.route('/api/satellites/tracking', methods=['GET'])
def get_tracking():
    """
    Get satellite tracking and propagation data.
    
    Query Parameters:
        norad_id (int, optional): Filter by specific NORAD catalog ID
    
    Returns:
        JSON response with tracking data count and array of satellite records
    """
    norad_id = request.args.get('norad_id', None)
    tracking = data_provider.get_satellite_tracking(norad_id)
    return jsonify({
        'count': len(tracking),
        'data': tracking
    })


@app.route('/api/satellites/pairs', methods=['GET'])
def get_pairs():
    """
    Get satellite pairs with highest collision frequencies.
    
    Returns:
        JSON response with satellite pair count and array of collision-prone pairs
    """
    pairs = data_provider.get_satellite_pairs()
    return jsonify({
        'count': len(pairs),
        'data': pairs
    })


@app.route('/api/config', methods=['GET'])
def get_config():
    """
    Get dashboard configuration parameters from environment variables.
    
    Returns:
        JSON response with current system configuration including thresholds,
        time windows, and update intervals for dashboard functionality
    """
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
