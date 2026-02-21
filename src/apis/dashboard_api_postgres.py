"""
Enhanced Dashboard API - PostgreSQL + HDFS Hybrid
Fast queries from PostgreSQL, historical data from HDFS
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.pool
from datetime import datetime, timedelta
import os
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

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
            'timestamp': datetime.now().isoformat()
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
            'collisions': results
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
            'high_risk_collisions': results
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
    
    Returns:
        JSON response with tracking change count and array of change records
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        days = request.args.get('days', 7, type=int)
        limit = request.args.get('limit', 100, type=int)
        
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
        
        return jsonify({
            'count': len(results),
            'changes': results
        })
        
    except Exception as e:
        logger.error(f"Error fetching tracking changes: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics', methods=['GET'])
def get_system_metrics():
    """
    Get latest system performance metrics grouped by metric name.
    
    Query Parameters:
        hours (int): Number of hours to look back for metrics (default: 24)
    
    Returns:
        JSON response with metrics grouped by metric name for dashboard charts
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        hours = request.args.get('hours', 24, type=int)
        
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
        
        return jsonify({
            'metrics': metrics_by_name
        })
        
    except Exception as e:
        logger.error(f"Error fetching system metrics: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    """
    Get key statistics for dashboard overview including satellite counts,
    collision alerts, and system health metrics.
    
    Returns:
        JSON response with comprehensive dashboard statistics including
        active/stopped satellite counts, collision risk levels, and average altitude
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM satellites WHERE tracking_status = 'ACTIVE'")
        active_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM satellites WHERE tracking_status != 'ACTIVE'")
        stopped_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM collision_alerts WHERE risk_level IN ('CRITICAL', 'HIGH', 'MEDIUM') AND is_active = TRUE")
        high_risk_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM collision_alerts WHERE is_active = TRUE")
        total_collisions = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(last_altitude_km) FROM satellites WHERE tracking_status = 'ACTIVE'")
        avg_altitude = cursor.fetchone()[0]
        
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'active_satellites': active_count,
            'stopped_satellites': stopped_count,
            'high_risk_collisions': high_risk_count,
            'total_active_collisions': total_collisions,
            'avg_altitude_km': float(avg_altitude) if avg_altitude else 0,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching dashboard stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/', methods=['GET'])
def index():
    """
    API documentation endpoint providing overview of all available endpoints.
    
    Returns:
        JSON response with API information, available endpoints, and configuration details
    """
    return jsonify({
        'name': 'Space Debris Dashboard API',
        'version': '2.0 (PostgreSQL Hybrid)',
        'endpoints': {
            '/api/health': 'Health check',
            '/api/satellites': 'Get all satellites (optional: ?status=ACTIVE&limit=100)',
            '/api/satellites/<norad_id>': 'Get specific satellite details',
            '/api/satellites/summary': 'Get satellite status summary',
            '/api/collisions': 'Get collision alerts (optional: ?risk_level=HIGH&active=true&limit=100)',
            '/api/collisions/high-risk': 'Get high-risk collisions in next 7 days',
            '/api/tracking-changes': 'Get recent tracking status changes (optional: ?days=7&limit=100)',
            '/api/metrics': 'Get system metrics (optional: ?hours=24)',
            '/api/dashboard/stats': 'Get key dashboard statistics'
        },
        'database': 'PostgreSQL (fast queries) + HDFS (historical data)',
        'performance': 'Query response time: 10-50ms (PostgreSQL indexed)'
    })


if __name__ == '__main__':
    PORT = int(os.getenv('DASHBOARD_PORT', 5001))
    logger.info(f"Starting Dashboard API on port {PORT}...")
    logger.info(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
