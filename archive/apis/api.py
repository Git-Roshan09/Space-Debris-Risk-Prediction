#!/usr/bin/env python3
"""
Flask API for TLE Data Streaming
Runs on base machine (localhost:5000) and streams TLE data to Kafka
"""

from flask import Flask, Response, jsonify, request, stream_with_context
import csv
import os
import glob
from datetime import datetime
import time
import json
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
TLE_DATA_DIR = 'data/processed/TLE_History'
DEFAULT_ACCELERATION = 100  # 100x faster than real-time
DEFAULT_MAX_DELAY = 5.0  # Maximum delay between records in seconds

# Global cache
_tle_data_cache = None
_cache_loaded_at = None


def load_all_tle_data():
    """Load all TLE data from CSV files and sort by timestamp."""
    global _tle_data_cache, _cache_loaded_at
    
    # Return cached data if available and recent (< 1 hour old)
    if _tle_data_cache and _cache_loaded_at:
        if (datetime.now() - _cache_loaded_at).seconds < 3600:
            logger.info(f"Using cached TLE data ({len(_tle_data_cache)} records)")
            return _tle_data_cache
    
    all_data = []
    tle_files = glob.glob(os.path.join(TLE_DATA_DIR, '*.csv'))
    
    logger.info(f"Loading TLE data from {len(tle_files)} files...")
    
    for tle_file in tle_files:
        try:
            satellite_id = os.path.basename(tle_file).replace('_tle.csv', '')
            
            with open(tle_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        epoch = datetime.fromisoformat(row['EPOCH'])
                        all_data.append({
                            'satellite_id': satellite_id,
                            'epoch': row['EPOCH'],
                            'epoch_dt': epoch,
                            'tle_line1': row['TLE_LINE1'],
                            'tle_line2': row['TLE_LINE2']
                        })
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Error parsing row in {tle_file}: {e}")
                        continue
        except Exception as e:
            logger.error(f"Error reading file {tle_file}: {e}")
            continue
    
    # Sort by timestamp
    all_data.sort(key=lambda x: x['epoch_dt'])
    
    logger.info(f"Loaded {len(all_data)} TLE records")
    if all_data:
        logger.info(f"Date range: {all_data[0]['epoch']} to {all_data[-1]['epoch']}")
    
    # Cache the data
    _tle_data_cache = all_data
    _cache_loaded_at = datetime.now()
    
    return all_data


def generate_stream(acceleration_factor=DEFAULT_ACCELERATION, limit=None, 
                   max_delay=DEFAULT_MAX_DELAY, mode='adaptive'):
    """
    Generate streaming data with time-based acceleration.
    
    Args:
        acceleration_factor: How many times faster than real-time to stream
        limit: Maximum number of records to stream (None for all)
        max_delay: Maximum delay between records in seconds (None for unlimited)
        mode: Streaming mode - 'proportional', 'fixed', or 'adaptive'
    """
    data = load_all_tle_data()
    
    if not data:
        logger.error("No TLE data available to stream")
        yield json.dumps({'error': 'No TLE data available'}) + '\n'
        return
    
    if limit:
        data = data[:limit]
    
    count = 0
    prev_time = None
    total_real_time = 0
    total_stream_time = 0
    
    logger.info(f"Starting stream: {len(data)} records, mode={mode}, acceleration={acceleration_factor}x")
    
    for record in data:
        if prev_time is not None:
            # Calculate the time difference in seconds
            time_diff = (record['epoch_dt'] - prev_time).total_seconds()
            total_real_time += time_diff
            
            # Calculate sleep time based on mode
            if mode == 'fixed':
                sleep_time = 1.0 / acceleration_factor if acceleration_factor > 0 else 0
            elif mode == 'adaptive':
                sleep_time = time_diff / acceleration_factor
                if max_delay is not None and sleep_time > max_delay:
                    sleep_time = max_delay
            else:  # proportional
                sleep_time = time_diff / acceleration_factor
            
            # Apply sleep
            if sleep_time > 0:
                time.sleep(sleep_time)
                total_stream_time += sleep_time
        
        # Prepare the record for streaming
        stream_record = {
            'satellite_id': record['satellite_id'],
            'epoch': record['epoch'],
            'tle_line1': record['tle_line1'],
            'tle_line2': record['tle_line2'],
            'sequence_number': count + 1,
            'time_gap_seconds': (record['epoch_dt'] - prev_time).total_seconds() if prev_time else 0
        }
        
        yield json.dumps(stream_record) + '\n'
        
        prev_time = record['epoch_dt']
        count += 1
    
    # Send summary as last message
    summary = {
        'type': 'summary',
        'total_records': count,
        'real_time_span_seconds': total_real_time,
        'stream_time_seconds': total_stream_time,
        'effective_acceleration': total_real_time / total_stream_time if total_stream_time > 0 else 0
    }
    yield json.dumps(summary) + '\n'
    logger.info(f"Stream completed: {count} records sent")


@app.route('/')
def index():
    """API documentation endpoint."""
    return jsonify({
        'name': 'TLE Streaming API',
        'version': '2.0',
        'description': 'Streams Two-Line Element (TLE) data for space debris tracking',
        'endpoints': {
            '/': 'API documentation (this page)',
            '/stream': 'Stream TLE data in chronological order',
            '/stats': 'Get statistics about the TLE dataset',
            '/health': 'Health check endpoint'
        },
        'stream_parameters': {
            'acceleration': f'Acceleration factor (default: {DEFAULT_ACCELERATION}x)',
            'limit': 'Maximum number of records to stream (optional)',
            'max_delay': f'Maximum delay between records in seconds (default: {DEFAULT_MAX_DELAY})',
            'mode': "Streaming mode: 'adaptive' (recommended), 'fixed', or 'proportional'"
        },
        'example_urls': {
            'stream_default': 'http://localhost:5000/stream',
            'stream_fast': 'http://localhost:5000/stream?acceleration=500',
            'stream_limited': 'http://localhost:5000/stream?limit=100',
            'stream_no_delay': 'http://localhost:5000/stream?mode=fixed&acceleration=1000'
        }
    })


@app.route('/health')
def health():
    """Health check endpoint."""
    try:
        data = load_all_tle_data()
        return jsonify({
            'status': 'healthy',
            'tle_records_available': len(data),
            'data_directory': TLE_DATA_DIR
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/stats')
def stats():
    """Get statistics about the TLE dataset."""
    try:
        data = load_all_tle_data()
        
        if not data:
            return jsonify({'error': 'No TLE data available'}), 404
        
        # Calculate statistics
        satellites = set(record['satellite_id'] for record in data)
        
        return jsonify({
            'total_records': len(data),
            'unique_satellites': len(satellites),
            'date_range': {
                'start': data[0]['epoch'],
                'end': data[-1]['epoch']
            },
            'time_span_days': (data[-1]['epoch_dt'] - data[0]['epoch_dt']).days,
            'data_directory': TLE_DATA_DIR
        })
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/stream')
def stream():
    """Stream TLE data in chronological order."""
    # Get parameters
    acceleration = request.args.get('acceleration', DEFAULT_ACCELERATION, type=int)
    limit = request.args.get('limit', None, type=int)
    max_delay = request.args.get('max_delay', DEFAULT_MAX_DELAY, type=float)
    mode = request.args.get('mode', 'adaptive', type=str)
    
    # Validate parameters
    if acceleration <= 0:
        return jsonify({'error': 'Acceleration factor must be positive'}), 400
    
    if mode not in ['adaptive', 'fixed', 'proportional']:
        return jsonify({'error': "Mode must be 'adaptive', 'fixed', or 'proportional'"}), 400
    
    logger.info(f"Stream request: acceleration={acceleration}x, limit={limit}, mode={mode}")
    
    return Response(
        stream_with_context(generate_stream(acceleration, limit, max_delay, mode)),
        mimetype='application/json',
        headers={
            'X-Accel-Buffering': 'no',
            'Cache-Control': 'no-cache'
        }
    )


if __name__ == '__main__':
    # Verify data directory exists
    if not os.path.exists(TLE_DATA_DIR):
        logger.error(f"TLE data directory not found: {TLE_DATA_DIR}")
        logger.error("Please ensure the data directory exists before starting the API")
    else:
        logger.info(f"TLE data directory: {TLE_DATA_DIR}")
        logger.info(f"Starting Flask API on http://localhost:5000")
        logger.info("Press CTRL+C to quit")
        
        # Run Flask app
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False,
            threaded=True
        )
