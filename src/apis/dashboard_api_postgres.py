"""
Enhanced Dashboard API - PostgreSQL + HDFS Hybrid
Fast queries from PostgreSQL, historical data from HDFS
DEMO MODE: Simulates time progression at accelerated rate (10 days per minute)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.pool
from datetime import datetime, timedelta
import os
import logging
import json
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Event-Driven Simulation Configuration
# Simulation advances manually when processing for a day completes
SIMULATION_CURRENT_DATE = datetime(2004, 1, 1, 0, 0, 0)  # Current simulated datetime
SIMULATION_EPOCH = datetime(2004, 1, 1, 0, 0, 0)  # Starting datetime for simulation
SIMULATION_LOCKED = False  # Prevents concurrent simulation updates

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres-debris'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'space_debris'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        **POSTGRES_CONFIG
    )
    logger.info(f"✓ PostgreSQL connection pool initialized: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
except Exception as e:
    logger.error(f"❌ Failed to initialize PostgreSQL pool: {e}")
    db_pool = None


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


def get_db_connection():
    """
    Get a connection from the PostgreSQL connection pool.
    
    Returns:
        psycopg2.connection: Database connection from the pool
        
    Raises:
        Exception: If database pool is not initialized
    """
    if db_pool:
        return db_pool.getconn()
    raise Exception("Database pool not initialized")


def release_db_connection(conn):
    """
    Return a database connection to the connection pool.
    
    Args:
        conn: Database connection to return to pool
    """
    if db_pool and conn:
        db_pool.putconn(conn)

@app.route('/api/health', methods=['GET'])
def health_check():
    """
    Health check endpoint to verify API and database connectivity.
    
    Returns:
        JSON response with health status, database connection status, and timestamp
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'real_time': datetime.now().isoformat(),  # Server's actual time (for logging)
            'simulated_time': get_simulated_time().isoformat(),  # Event-driven simulated time
            'simulation_epoch': SIMULATION_EPOCH.isoformat(),
            'simulation_mode': 'event-driven',
            'simulation_description': 'Time advances when processing completes'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/satellites', methods=['GET'])
def get_satellites():
    """
    Get all satellites with current tracking status and optional filtering.
    
    Query Parameters:
        status (str): Filter by tracking status (e.g., 'ACTIVE', 'STOPPED_*')
        limit (int): Maximum number of results to return (default: 1000)
    
    Returns:
        JSON response with satellite count and array of satellite objects
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        status = request.args.get('status')
        limit = request.args.get('limit', 1000, type=int)
        
        query = """
            SELECT 
                norad_id, name, object_type, country,
                tracking_status, last_tle_epoch, tle_age_days,
                last_altitude_km, last_velocity_kms,
                total_observations, status_updated_at
            FROM satellites
        """
        
        params = []
        if status:
            query += " WHERE tracking_status = %s"
            params.append(status)
        
        query += " ORDER BY status_updated_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'count': len(results),
            'satellites': results
        })
        
    except Exception as e:
        logger.error(f"Error fetching satellites: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/satellites/<int:norad_id>', methods=['GET'])
def get_satellite_by_id(norad_id):
    """
    Get detailed information for a specific satellite by NORAD ID.
    
    Args:
        norad_id (int): NORAD catalog ID of the satellite
    
    Returns:
        JSON response with complete satellite details or 404 if not found
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM satellites WHERE norad_id = %s
        """, (norad_id,))
        
        row = cursor.fetchone()
        if not row:
            cursor.close()
            release_db_connection(conn)
            return jsonify({'error': 'Satellite not found'}), 404
        
        columns = [desc[0] for desc in cursor.description]
        result = dict(zip(columns, row))
        
        for key, value in result.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
        
        cursor.close()
        release_db_connection(conn)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error fetching satellite {norad_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/satellites/summary', methods=['GET'])
def get_satellites_summary():
    """
    Get summary statistics of satellites grouped by tracking status.
    
    Returns:
        JSON response with summary statistics from active_satellites_summary view
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM active_satellites_summary
        """)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'summary': results
        })
        
    except Exception as e:
        logger.error(f"Error fetching satellite summary: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/collisions', methods=['GET'])
def get_collision_alerts():
    """
    Get collision alerts with optional filtering by risk level and active status.
    
    Query Parameters:
        risk_level (str): Filter by risk level ('HIGH', 'MEDIUM', 'LOW')
        active (bool): Show only active alerts (default: true)
        limit (int): Maximum number of results to return (default: 100)
    
    Returns:
        JSON response with collision count and array of collision alert objects
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        risk_level = request.args.get('risk_level')
        active_only = request.args.get('active', 'true').lower() == 'true'
        limit = request.args.get('limit', 100, type=int)
        
        query = """
            SELECT 
                id, satellite_1_id, satellite_2_id,
                satellite_1_name, satellite_2_name,
                predicted_time, miss_distance_km,
                relative_velocity_kms, risk_level,
                collision_probability, detected_at, is_active
            FROM collision_alerts
            WHERE 1=1
        """
        
        params = []
        if active_only:
            query += " AND is_active = TRUE"
        if risk_level:
            query += " AND risk_level = %s"
            params.append(risk_level)
        
        query += " ORDER BY miss_distance_km ASC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'count': len(results),
            'collisions': results,
            'simulated_time': get_simulated_time().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching collision alerts: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/collisions/high-risk', methods=['GET'])
def get_high_risk_collisions():
    """
    Get high-risk and critical collision alerts for dashboard priority alerts.
    
    Returns:
        JSON response with high-risk collision count and array of collision objects.
        Includes CRITICAL, HIGH, and MEDIUM risk levels for comprehensive monitoring.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, satellite_1_id, satellite_2_id,
                satellite_1_name, satellite_2_name,
                predicted_time, miss_distance_km,
                risk_level, is_active, detected_at
            FROM collision_alerts
            WHERE is_active = TRUE
              AND risk_level IN ('HIGH', 'MEDIUM', 'CRITICAL')
            ORDER BY miss_distance_km ASC
            LIMIT 50
        """)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'count': len(results),
            'high_risk_collisions': results,
            'simulated_time': get_simulated_time().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching high-risk collisions: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/tracking-changes', methods=['GET'])
def get_tracking_changes():
    """
    Get recent satellite tracking status changes for monitoring system health.
    
    Query Parameters:
        days (int): Number of days to look back (default: 7)
        limit (int): Maximum number of results to return (default: 100)
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with tracking change count and array of change records
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        days = request.args.get('days', 7, type=int)
        limit = request.args.get('limit', 100, type=int)
        use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
        
        if use_sim:
            # Use simulated time for filtering
            sim_time = get_simulated_time()
            cutoff_time = sim_time - timedelta(days=days)
            
            cursor.execute("""
                SELECT 
                    id, norad_id, satellite_name,
                    old_status, new_status, reason,
                    altitude_km, tle_age_days, sgp4_error_code,
                    changed_at
                FROM tracking_status_changes
                WHERE changed_at >= %s AND changed_at <= %s
                ORDER BY changed_at DESC
                LIMIT %s
            """, (cutoff_time, sim_time, limit))
        else:
            cursor.execute("""
                SELECT 
                    id, norad_id, satellite_name,
                    old_status, new_status, reason,
                    altitude_km, tle_age_days, sgp4_error_code,
                    changed_at
                FROM tracking_status_changes
                WHERE changed_at >= NOW() - INTERVAL '%s days'
                ORDER BY changed_at DESC
                LIMIT %s
            """, (days, limit))
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        cursor.close()
        release_db_connection(conn)
        
        response = {
            'count': len(results),
            'changes': results
        }
        
        if use_sim:
            response['simulated_time'] = get_simulated_time().isoformat()
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error fetching tracking changes: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics', methods=['GET'])
def get_system_metrics():
    """
    Get latest system performance metrics grouped by metric name.
    
    Query Parameters:
        hours (int): Number of hours to look back for metrics (default: 24)
        use_simulation (bool): Use simulated time filtering (default: true)
    
    Returns:
        JSON response with metrics grouped by metric name for dashboard charts
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        hours = request.args.get('hours', 24, type=int)
        use_sim = request.args.get('use_simulation', 'true').lower() == 'true'
        
        if use_sim:
            # Use simulated time for filtering
            sim_time = get_simulated_time()
            cutoff_time = sim_time - timedelta(hours=hours)
            
            cursor.execute("""
                SELECT 
                    metric_name, metric_value, metric_unit,
                    metric_description, recorded_at
                FROM system_metrics
                WHERE recorded_at >= %s AND recorded_at <= %s
                ORDER BY recorded_at DESC
            """, (cutoff_time, sim_time))
        else:
            cursor.execute("""
                SELECT 
                    metric_name, metric_value, metric_unit,
                    metric_description, recorded_at
                FROM system_metrics
                WHERE recorded_at >= NOW() - INTERVAL '%s hours'
                ORDER BY recorded_at DESC
            """, (hours,))
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        for row in results:
            if isinstance(row['recorded_at'], datetime):
                row['recorded_at'] = row['recorded_at'].isoformat()
        
        cursor.close()
        release_db_connection(conn)
        
        metrics_by_name = {}
        for row in results:
            name = row['metric_name']
            if name not in metrics_by_name:
                metrics_by_name[name] = []
            metrics_by_name[name].append(row)
        
        response = {
            'metrics': metrics_by_name
        }
        
        if use_sim:
            response['simulated_time'] = get_simulated_time().isoformat()
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error fetching system metrics: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    """
    Get comprehensive statistics for dashboard overview.

    Returns per-risk-level collision counts, distance statistics,
    satellite counts, closest approach info, and last batch metadata.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Satellite counts
        cursor.execute("SELECT COUNT(*) FROM satellites WHERE tracking_status = 'ACTIVE'")
        active_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM satellites WHERE tracking_status != 'ACTIVE'")
        stopped_count = cursor.fetchone()[0]

        # Per-risk collision counts
        cursor.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END), 0),
                COUNT(*)
            FROM collision_alerts WHERE is_active = TRUE
        """)
        row = cursor.fetchone()
        critical_count, high_count, medium_count, low_count, total_collisions = row

        # Distance statistics
        cursor.execute("""
            SELECT
                MIN(miss_distance_km),
                AVG(miss_distance_km),
                MAX(miss_distance_km)
            FROM collision_alerts WHERE is_active = TRUE
        """)
        dist_row = cursor.fetchone()
        min_dist = float(dist_row[0]) if dist_row[0] is not None else None
        avg_dist = float(dist_row[1]) if dist_row[1] is not None else None
        max_dist = float(dist_row[2]) if dist_row[2] is not None else None

        # Closest approach details
        cursor.execute("""
            SELECT satellite_1_id, satellite_2_id,
                   satellite_1_name, satellite_2_name,
                   miss_distance_km, risk_level, predicted_time
            FROM collision_alerts
            WHERE is_active = TRUE
            ORDER BY miss_distance_km ASC
            LIMIT 1
        """)
        closest = cursor.fetchone()
        closest_approach = None
        if closest:
            closest_approach = {
                'satellite_1_id': closest[0],
                'satellite_2_id': closest[1],
                'satellite_1_name': closest[2],
                'satellite_2_name': closest[3],
                'miss_distance_km': float(closest[4]) if closest[4] else None,
                'risk_level': closest[5],
                'predicted_time': closest[6].isoformat() if closest[6] else None
            }

        # Average altitude
        cursor.execute("SELECT AVG(last_altitude_km) FROM satellites WHERE tracking_status = 'ACTIVE'")
        avg_altitude = cursor.fetchone()[0]

        # Last batch info
        cursor.execute("SELECT batch_id, MAX(detected_at) FROM collision_alerts WHERE is_active = TRUE GROUP BY batch_id ORDER BY MAX(detected_at) DESC LIMIT 1")
        batch_row = cursor.fetchone()
        last_batch_id = batch_row[0] if batch_row else None
        last_batch_time = batch_row[1].isoformat() if batch_row and batch_row[1] else None

        cursor.close()
        release_db_connection(conn)

        return jsonify({
            'active_satellites': active_count,
            'stopped_satellites': stopped_count,
            'critical_risk_collisions': critical_count,
            'high_risk_collisions': high_count,
            'medium_risk_collisions': medium_count,
            'low_risk_collisions': low_count,
            'total_active_collisions': total_collisions,
            'min_distance_km': min_dist,
            'avg_distance_km': avg_dist,
            'max_distance_km': max_dist,
            'closest_approach': closest_approach,
            'avg_altitude_km': float(avg_altitude) if avg_altitude else 0,
            'last_batch_id': last_batch_id,
            'last_batch_time': last_batch_time,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching dashboard stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/collisions/all', methods=['GET'])
def get_all_collisions():
    """
    Get all active collision alerts with pagination, filtering, and sorting.

    Query Parameters:
        risk_level (str): Filter by risk level ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')
        page (int): Page number (default: 1)
        per_page (int): Results per page (default: 50, max: 200)
        sort_by (str): Column to sort by (default: 'miss_distance_km')
        sort_order (str): 'asc' or 'desc' (default: 'asc')
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        risk_level = request.args.get('risk_level')
        page = max(1, request.args.get('page', 1, type=int))
        per_page = min(200, max(1, request.args.get('per_page', 50, type=int)))
        sort_by = request.args.get('sort_by', 'miss_distance_km')
        sort_order = request.args.get('sort_order', 'asc').upper()

        allowed_sort_cols = ['miss_distance_km', 'predicted_time', 'risk_level', 'detected_at', 'satellite_1_id']
        if sort_by not in allowed_sort_cols:
            sort_by = 'miss_distance_km'
        if sort_order not in ('ASC', 'DESC'):
            sort_order = 'ASC'

        offset = (page - 1) * per_page
        params = []

        where_clause = "WHERE is_active = TRUE"
        if risk_level:
            where_clause += " AND risk_level = %s"
            params.append(risk_level.upper())

        # Count total matching
        cursor.execute(f"SELECT COUNT(*) FROM collision_alerts {where_clause}", params)
        total_count = cursor.fetchone()[0]

        query = f"""
            SELECT
                id, satellite_1_id, satellite_2_id,
                satellite_1_name, satellite_2_name,
                predicted_time, miss_distance_km,
                relative_velocity_kms, risk_level,
                collision_probability, detected_at,
                batch_id, is_active
            FROM collision_alerts
            {where_clause}
            ORDER BY {sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, offset])
        cursor.execute(query, params)

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()

        cursor.close()
        release_db_connection(conn)

        total_pages = (total_count + per_page - 1) // per_page if per_page > 0 else 1

        return jsonify({
            'count': len(results),
            'total_count': total_count,
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'collisions': results
        })

    except Exception as e:
        logger.error(f"Error fetching all collisions: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/collisions/frequency', methods=['GET'])
def get_collision_frequency():
    """
    Get frequently colliding satellite pairs with aggregated statistics.

    Query Parameters:
        limit (int): Maximum number of pairs to return (default: 20)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        limit = request.args.get('limit', 20, type=int)

        cursor.execute("""
            SELECT
                satellite_1_id, satellite_2_id,
                MAX(satellite_1_name) as satellite_1_name,
                MAX(satellite_2_name) as satellite_2_name,
                COUNT(*) as collision_count,
                MIN(miss_distance_km) as min_distance_km,
                AVG(miss_distance_km) as avg_distance_km,
                MAX(miss_distance_km) as max_distance_km,
                MIN(predicted_time) as earliest_collision,
                MAX(predicted_time) as latest_collision
            FROM collision_alerts
            WHERE is_active = TRUE
            GROUP BY satellite_1_id, satellite_2_id
            ORDER BY COUNT(*) DESC, MIN(miss_distance_km) ASC
            LIMIT %s
        """, (limit,))

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()

        cursor.close()
        release_db_connection(conn)

        return jsonify({
            'count': len(results),
            'pairs': results
        })

    except Exception as e:
        logger.error(f"Error fetching collision frequency: {e}")
        return jsonify({'error': str(e)}), 500


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


def index():
    """
    API documentation endpoint providing overview of all available endpoints.
    
    Returns:
        JSON response with API information, available endpoints, and configuration details
    """
    return jsonify({
        'name': 'Space Debris Dashboard API',
        'version': '2.0 (PostgreSQL Hybrid + Simulation)',
        'endpoints': {
            '/api/health': 'Health check with simulation info',
            '/api/satellites': 'Get all satellites (optional: ?status=ACTIVE&limit=100)',
            '/api/satellites/<norad_id>': 'Get specific satellite details',
            '/api/satellites/summary': 'Get satellite status summary',
            '/api/collisions': 'Get collision alerts (optional: ?risk_level=HIGH&active=true&limit=100)',
            '/api/collisions/high-risk': 'Get high-risk collisions in next 7 days',
            '/api/collisions/all': 'Get all collisions with pagination (optional: ?risk_level=HIGH&page=1&per_page=50&sort_by=miss_distance_km&sort_order=asc)',
            '/api/collisions/frequency': 'Get frequently colliding satellite pairs (optional: ?limit=20)',
            '/api/tracking-changes': 'Get recent tracking status changes (optional: ?days=7&limit=100&use_simulation=true)',
            '/api/metrics': 'Get system metrics (optional: ?hours=24&use_simulation=true)',
            '/api/dashboard/stats': 'Get comprehensive dashboard statistics with per-risk counts and distance stats',
            '/api/simulation/time': 'Get current simulated time and parameters',
            '/api/simulation/advance': 'Advance time after processing (POST: {"days": 1, "hours": 0, "minutes": 0})',
            '/api/simulation/set': 'Set time or reset (POST: {"time": "ISO-datetime"} or {"reset": true})'
        },
        'database': 'PostgreSQL (fast queries) + HDFS (historical data)',
        'performance': 'Query response time: 10-50ms (PostgreSQL indexed)',
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
    PORT = int(os.getenv('DASHBOARD_PORT', 5001))
    logger.info("=" * 60)
    logger.info("Starting Dashboard API with EVENT-DRIVEN SIMULATION")
    logger.info("=" * 60)
    logger.info(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    logger.info(f"Simulation Epoch: {SIMULATION_EPOCH.isoformat()}")
    logger.info(f"Current Simulated Time: {SIMULATION_CURRENT_DATE.isoformat()}")
    logger.info(f"Simulation Mode: Event-driven (advances when processing completes)")
    logger.info(f"API Port: {PORT}")
    logger.info("=" * 60)
    logger.info("Simulation Endpoints:")
    logger.info("  GET  /api/simulation/time    - Get current simulated time")
    logger.info("  POST /api/simulation/advance - Advance time after processing")
    logger.info("  POST /api/simulation/set     - Set time or reset to epoch")
    logger.info("=" * 60)
    app.run(host='0.0.0.0', port=PORT, debug=False)
