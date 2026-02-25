"""
Dashboard API - Serves collision prediction and space debris monitoring data
Provides real-time data for the visualization dashboard
DEMO MODE: Simulates time progression at accelerated rate (10 days per minute)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, avg, min as spark_min, max as spark_max
from datetime import datetime, timedelta
import os
import logging
import json
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

HDFS_COLLISION_PATH = os.getenv('HDFS_COLLISION_PREDICTIONS_PATH', 
                                 'hdfs://namenode:9000/space-debris/collision_predictions')
HDFS_SGP4_PATH = os.getenv('HDFS_SGP4_VECTORS_PATH',
                           'hdfs://namenode:9000/space-debris/sgp4_vectors')

# Event-Driven Simulation Configuration
# Simulation advances manually when processing for a day completes
SIMULATION_CURRENT_DATE = datetime(2004, 1, 1, 0, 0, 0)  # Current simulated datetime
SIMULATION_EPOCH = datetime(2004, 1, 1, 0, 0, 0)  # Starting datetime for simulation
SIMULATION_LOCKED = False  # Prevents concurrent simulation updates


def get_simulated_time():
    """
    Get the current simulated time.
    
    This is event-driven - time only advances when explicitly updated
    via advance_simulation_time() after processing completes for a day.
    
    Returns:
        datetime: Current simulated datetime
    """
    return SIMULATION_CURRENT_DATE


def set_simulation_time(new_time):
    """
    Set the simulation to a specific datetime.
    
    Args:
        new_time (datetime): New simulated datetime
    """
    global SIMULATION_CURRENT_DATE
    SIMULATION_CURRENT_DATE = new_time
    logger.info(f"Simulation time set to {new_time.isoformat()}")


def advance_simulation_time(days=1, hours=0, minutes=0):
    """
    Advance the simulation time by specified amount.
    
    This should be called after processing completes for a time period.
    For example, call advance_simulation_time(days=1) after all data
    processing for the current day finishes.
    
    Args:
        days (int): Number of days to advance (default: 1)
        hours (int): Number of hours to advance (default: 0)
        minutes (int): Number of minutes to advance (default: 0)
        
    Returns:
        datetime: New simulated datetime after advancement
    """
    global SIMULATION_CURRENT_DATE, SIMULATION_LOCKED
    
    if SIMULATION_LOCKED:
        logger.warning("Simulation is locked - another process is updating time")
        return SIMULATION_CURRENT_DATE
    
    SIMULATION_LOCKED = True
    try:
        old_time = SIMULATION_CURRENT_DATE
        SIMULATION_CURRENT_DATE = SIMULATION_CURRENT_DATE + timedelta(days=days, hours=hours, minutes=minutes)
        logger.info(f"Simulation advanced from {old_time.isoformat()} to {SIMULATION_CURRENT_DATE.isoformat()}")
        return SIMULATION_CURRENT_DATE
    finally:
        SIMULATION_LOCKED = False


def reset_simulation(new_epoch=None):
    """
    Reset simulation to epoch or specified datetime.
    
    Args:
        new_epoch (datetime, optional): New starting datetime. If None, uses original SIMULATION_EPOCH
    """
    global SIMULATION_CURRENT_DATE, SIMULATION_EPOCH
    
    if new_epoch:
        SIMULATION_EPOCH = new_epoch
    
    SIMULATION_CURRENT_DATE = SIMULATION_EPOCH
    logger.info(f"Simulation reset to {SIMULATION_CURRENT_DATE.isoformat()}")


class DashboardDataProvider:
    """
    Provides collision prediction and satellite tracking data for dashboard visualization.
    
    Queries HDFS to retrieve collision predictions, SGP4 vectors, and statistical summaries
    for real-time space debris monitoring dashboard.
    
    DEMO MODE: Uses simulated time progression for realistic demo scenarios.
    """
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Dashboard-API") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized for Dashboard API")
        logger.info(f"Event-driven simulation enabled: Current={SIMULATION_CURRENT_DATE.isoformat()}, Epoch={SIMULATION_EPOCH.isoformat()}")
    
    def get_collision_alerts(self, limit=100, use_simulation=True):
        """
        Retrieve recent collision predictions from HDFS storage.
        
        Args:
            limit (int): Maximum number of collision records to return (default: 100)
            use_simulation (bool): If True, filter data based on simulated time (default: True)
            
        Returns:
            list: Array of collision prediction dictionaries with detection timestamps,
                  satellite pairs, distances, and risk levels
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            if use_simulation:
                sim_time = get_simulated_time()
                # Show data from the past 24 simulated hours
                sim_start = (sim_time - timedelta(hours=24)).isoformat()
                sim_end = sim_time.isoformat()
                df = df.filter((col("detection_timestamp") >= sim_start) & 
                             (col("detection_timestamp") <= sim_end))
                logger.debug(f"Filtering collisions for simulated time range: {sim_start} to {sim_end}")
            
            df_recent = df.orderBy(desc("detection_timestamp")) \
                         .limit(limit)
            
            results = [row.asDict() for row in df_recent.collect()]
            
            if use_simulation and results:
                logger.info(f"Retrieved {len(results)} collisions for simulated time {get_simulated_time().isoformat()}")
            
            return results
        except Exception as e:
            logger.error(f"Error reading collision data: {e}")
            return []
    
    def get_collision_stats(self, use_simulation=True):
        """
        Generate comprehensive statistical summary of collision predictions.
        
        Args:
            use_simulation (bool): If True, compute stats for data up to simulated time
        
        Returns:
            dict: Statistics including total collision count, risk level distribution,
                  distance statistics (min/max/avg), and time range of predictions
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            if use_simulation:
                sim_time = get_simulated_time()
                sim_end = sim_time.isoformat()
                df = df.filter(col("detection_timestamp") <= sim_end)
            
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
            
            stats = {
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
            
            if use_simulation:
                stats['simulated_time'] = get_simulated_time().isoformat()
            
            return stats
        except Exception as e:
            logger.error(f"Error computing collision stats: {e}")
            return {}
    
    def get_high_risk_collisions(self, use_simulation=True):
        """
        Retrieve only high-risk collision alerts for priority monitoring.
        
        Args:
            use_simulation (bool): If True, filter based on simulated time
        
        Returns:
            list: Array of HIGH risk collision predictions ordered by detection time
        """
        try:
            # Read from all batch subdirectories
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            if use_simulation:
                sim_time = get_simulated_time()
                sim_start = (sim_time - timedelta(hours=48)).isoformat()  # Last 48 simulated hours
                sim_end = sim_time.isoformat()
                df = df.filter((col("detection_timestamp") >= sim_start) & 
                             (col("detection_timestamp") <= sim_end))
            
            df_high_risk = df.filter(col("risk_level") == "HIGH") \
                            .orderBy(desc("detection_timestamp")) \
                            .limit(50)
            
            return [row.asDict() for row in df_high_risk.collect()]
        except Exception as e:
            logger.error(f"Error reading high-risk collisions: {e}")
            return []
    
    def get_satellite_tracking(self, norad_id=None, use_simulation=True):
        """
        Retrieve satellite tracking data and SGP4 propagation vectors.
        
        Args:
            norad_id (int, optional): Filter by specific NORAD catalog ID
            use_simulation (bool): If True, filter based on simulated time
            
        Returns:
            list: Array of satellite tracking records with positions, velocities, and timestamps
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_SGP4_PATH}/batch_*")
            
            if use_simulation:
                sim_time = get_simulated_time()
                sim_start = (sim_time - timedelta(hours=1)).isoformat()  # Last simulated hour
                sim_end = sim_time.isoformat()
                df = df.filter((col("timestamp") >= sim_start) & 
                             (col("timestamp") <= sim_end))
            
            if norad_id:
                df = df.filter(col("norad_id") == norad_id)
            
            df_recent = df.orderBy(desc("timestamp")).limit(1000)
            
            return [row.asDict() for row in df_recent.collect()]
        except Exception as e:
            logger.error(f"Error reading satellite tracking data: {e}")
            return []
    
    def get_collision_timeline(self, days=7, use_simulation=True):
        """
        Generate collision prediction timeline grouped by time periods.
        
        Args:
            days (int): Number of days to include in timeline (default: 7)
            use_simulation (bool): If True, use simulated time for filtering
            
        Returns:
            list: Array of time-grouped collision counts with timestamps and risk levels
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            if use_simulation:
                sim_time = get_simulated_time()
                cutoff = (sim_time - timedelta(days=days)).isoformat()
                sim_end = sim_time.isoformat()
                df_recent = df.filter((col("detection_timestamp") >= cutoff) & 
                                    (col("detection_timestamp") <= sim_end))
            else:
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
    
    def get_satellite_pairs(self, use_simulation=True):
        """
        Identify satellite pairs with the highest collision frequency.
        
        Args:
            use_simulation (bool): If True, filter based on simulated time
        
        Returns:
            list: Array of satellite pairs with collision counts, minimum distances,
                  and average distances, ordered by collision frequency
        """
        try:
            df = self.spark.read.parquet(f"{HDFS_COLLISION_PATH}/batch_*")
            
            if use_simulation:
                sim_time = get_simulated_time()
                sim_end = sim_time.isoformat()
                df = df.filter(col("detection_timestamp") <= sim_end)
            
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
        JSON response with service status, timestamp, and simulation info
    """
    return jsonify({
        'status': 'healthy',
        'real_time': datetime.now().isoformat(),  # Server's actual time (for logging)
        'simulated_time': get_simulated_time().isoformat(),  # Event-driven simulated time
        'simulation_epoch': SIMULATION_EPOCH.isoformat(),
        'simulation_mode': 'event-driven',
        'simulation_description': 'Time advances when processing completes',
        'service': 'collision-dashboard-api'
    })


@app.route('/api/simulation/time', methods=['GET'])
def get_simulation_time_endpoint():
    """
    Get current simulated time and simulation parameters.
    
    Returns:
        JSON response with current simulated time, epoch, and elapsed days
    """
    sim_time = get_simulated_time()
    elapsed_sim = sim_time - SIMULATION_EPOCH
    
    return jsonify({
        'current_simulated_time': sim_time.isoformat(),
        'simulation_epoch': SIMULATION_EPOCH.isoformat(),
        'simulation_mode': 'event-driven',
        'simulation_description': 'Time advances when processing completes for a day',
        'elapsed_simulated_days': elapsed_sim.total_seconds() / 86400,
        'simulation_locked': SIMULATION_LOCKED
    })


@app.route('/api/simulation/advance', methods=['POST'])
def advance_simulation():
    """
    Advance the simulation time (call after processing completes).
    
    Request Body (JSON):
        days (int, optional): Number of days to advance (default: 1)
        hours (int, optional): Number of hours to advance (default: 0)
        minutes (int, optional): Number of minutes to advance (default: 0)
        
    Returns:
        JSON response with updated simulation time
    """
    try:
        data = request.get_json() or {}
        
        days = data.get('days', 1)
        hours = data.get('hours', 0)
        minutes = data.get('minutes', 0)
        
        new_time = advance_simulation_time(days=days, hours=hours, minutes=minutes)
        
        return jsonify({
            'status': 'advanced',
            'current_simulated_time': new_time.isoformat(),
            'simulation_epoch': SIMULATION_EPOCH.isoformat(),
            'elapsed_days': (new_time - SIMULATION_EPOCH).total_seconds() / 86400
        })
    except Exception as e:
        logger.error(f"Error advancing simulation: {e}")
        return jsonify({'error': str(e)}), 400


@app.route('/api/simulation/set', methods=['POST'])
def set_simulation():
    """
    Set simulation to a specific datetime or reset to epoch.
    
    Request Body (JSON):
        time (str, optional): Specific datetime in ISO format
        reset (bool, optional): If true, reset to epoch
        epoch (str, optional): New epoch datetime in ISO format
        
    Returns:
        JSON response with updated simulation configuration
    """
    try:
        data = request.get_json() or {}
        
        if data.get('reset'):
            new_epoch = None
            if 'epoch' in data:
                new_epoch = datetime.fromisoformat(data['epoch'])
            reset_simulation(new_epoch)
        elif 'time' in data:
            new_time = datetime.fromisoformat(data['time'])
            set_simulation_time(new_time)
        else:
            return jsonify({'error': 'Must provide either "time", "reset":true, or "epoch"'}), 400
        
        return jsonify({
            'status': 'updated',
            'current_simulated_time': get_simulated_time().isoformat(),
            'simulation_epoch': SIMULATION_EPOCH.isoformat()
        })
    except Exception as e:
        logger.error(f"Error setting simulation: {e}")
        return jsonify({'error': str(e)}), 400


@app.route('/api/collisions', methods=['GET'])
def get_collisions():
    """
    Get collision predictions with optional limit parameter.
    
    Query Parameters:
        limit (int): Maximum number of collision records to return (default: 100)
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with collision count, simulated time, and array of collision data
    """
    limit = request.args.get('limit', 100, type=int)
    use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
    
    collisions = data_provider.get_collision_alerts(limit, use_simulation=use_sim)
    
    response = {
        'count': len(collisions),
        'data': collisions
    }
    
    if use_sim:
        response['simulated_time'] = get_simulated_time().isoformat()
    
    return jsonify(response)


@app.route('/api/collisions/stats', methods=['GET'])
def get_stats():
    """
    Get comprehensive collision prediction statistics.
    
    Query Parameters:
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with collision statistics including counts by risk level,
        distance statistics, time range coverage, and simulated time
    """
    use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
    stats = data_provider.get_collision_stats(use_simulation=use_sim)
    return jsonify(stats)


@app.route('/api/collisions/high-risk', methods=['GET'])
def get_high_risk():
    """
    Get high-risk collision alerts for priority monitoring.
    
    Query Parameters:
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with count, simulated time, and array of high-risk collision predictions
    """
    use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
    collisions = data_provider.get_high_risk_collisions(use_simulation=use_sim)
    
    response = {
        'count': len(collisions),
        'data': collisions
    }
    
    if use_sim:
        response['simulated_time'] = get_simulated_time().isoformat()
    
    return jsonify(response)


@app.route('/api/collisions/timeline', methods=['GET'])
def get_timeline():
    """
    Get collision prediction timeline for trend analysis.
    
    Query Parameters:
        days (int): Number of days to include in timeline (default: 7)
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with timeline data grouped by time periods and risk levels,
        plus simulated time information
    """
    days = request.args.get('days', 7, type=int)
    use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
    
    timeline = data_provider.get_collision_timeline(days, use_simulation=use_sim)
    
    response = {
        'count': len(timeline),
        'data': timeline,
        'days_requested': days
    }
    
    if use_sim:
        response['simulated_time'] = get_simulated_time().isoformat()
    
    return jsonify(response)


@app.route('/api/satellites/tracking', methods=['GET'])
def get_tracking():
    """
    Get satellite tracking and propagation data.
    
    Query Parameters:
        norad_id (int, optional): Filter by specific NORAD catalog ID
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with tracking data count, simulated time, and array of satellite records
    """
    norad_id = request.args.get('norad_id', None)
    use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
    
    tracking = data_provider.get_satellite_tracking(norad_id, use_simulation=use_sim)
    
    response = {
        'count': len(tracking),
        'data': tracking
    }
    
    if use_sim:
        response['simulated_time'] = get_simulated_time().isoformat()
    
    return jsonify(response)


@app.route('/api/satellites/pairs', methods=['GET'])
def get_pairs():
    """
    Get satellite pairs with highest collision frequencies.
    
    Query Parameters:
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with satellite pair count and array of collision-prone pairs
    """
    use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
    pairs = data_provider.get_satellite_pairs(use_simulation=use_sim)
    
    response = {
        'count': len(pairs),
        'data': pairs
    }
    
    if use_sim:
        response['simulated_time'] = get_simulated_time().isoformat()
    
    return jsonify(response)


@app.route('/api/config', methods=['GET'])
def get_config():
    """
    Get dashboard configuration parameters from environment variables.
    
    Returns:
        JSON response with current system configuration including thresholds,
        time windows, update intervals, and simulation settings
    """
    return jsonify({
        'prediction_days': int(os.getenv('PREDICTION_DAYS', '7')),
        'collision_threshold_km': float(os.getenv('COLLISION_THRESHOLD_KM', '10.0')),
        'time_window_days': int(os.getenv('TIME_WINDOW_DAYS', '7')),
        'update_interval_seconds': int(os.getenv('DASHBOARD_UPDATE_INTERVAL_SECONDS', '30')),
        'high_risk_threshold_km': float(os.getenv('HIGH_RISK_THRESHOLD_KM', '5.0')),
        'medium_risk_threshold_km': float(os.getenv('MEDIUM_RISK_THRESHOLD_KM', '10.0')),
        'low_risk_threshold_km': float(os.getenv('LOW_RISK_THRESHOLD_KM', '50.0')),
        'simulation': {
            'enabled': True,
            'mode': 'event-driven',
            'description': 'Time advances when processing completes',
            'epoch': SIMULATION_EPOCH.isoformat(),
            'current_simulated_time': get_simulated_time().isoformat(),
            'elapsed_days': (get_simulated_time() - SIMULATION_EPOCH).total_seconds() / 86400
        }
    })


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', '5001'))
    logger.info("="*60)
    logger.info("Starting Dashboard API with EVENT-DRIVEN SIMULATION")
    logger.info("="*60)
    logger.info(f"Simulation Epoch: {SIMULATION_EPOCH.isoformat()}")
    logger.info(f"Current Simulated Time: {SIMULATION_CURRENT_DATE.isoformat()}")
    logger.info(f"Simulation Mode: Event-driven (advances when processing completes)")
    logger.info(f"API Port: {port}")
    logger.info("="*60)
    logger.info("Simulation Endpoints:")
    logger.info("  GET  /api/simulation/time    - Get current simulated time")
    logger.info("  POST /api/simulation/advance - Advance time after processing")
    logger.info("  POST /api/simulation/set     - Set time or reset to epoch")
    logger.info("="*60)
    app.run(host='0.0.0.0', port=port, debug=False)
