#!/usr/bin/env python3
"""
Flask API to stream TLE data from Output/TLE_History directory
in chronological order at an accelerated pace.
"""

from flask import Flask, Response, jsonify, request, stream_with_context
import csv
import os
import glob
from datetime import datetime
import time
import json
from pathlib import Path

app = Flask(__name__)

# Configuration
TLE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'Output', 'TLE_History')
DEFAULT_ACCELERATION = 100  # 100x faster than real-time
DEFAULT_MAX_DELAY = 5.0  # Maximum delay between records in seconds (None for unlimited)


def load_all_tle_data():
    """Load all TLE data from CSV files and sort by timestamp."""
    all_data = []
    
    # Find all CSV files in the TLE_History directory
    tle_files = glob.glob(os.path.join(TLE_DATA_DIR, '*.csv'))
    
    print(f"Loading TLE data from {len(tle_files)} files...")
    
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
                        print(f"Error parsing row in {tle_file}: {e}")
                        continue
        except Exception as e:
            print(f"Error reading file {tle_file}: {e}")
            continue
    
    # Sort by timestamp
    all_data.sort(key=lambda x: x['epoch_dt'])
    
    print(f"Loaded {len(all_data)} TLE records")
    print(f"Date range: {all_data[0]['epoch']} to {all_data[-1]['epoch']}")
    
    return all_data


def generate_stream(acceleration_factor=DEFAULT_ACCELERATION, limit=None, max_delay=DEFAULT_MAX_DELAY, mode='adaptive'):
    """
    Generate streaming data with time-based acceleration.
    
    Args:
        acceleration_factor: How many times faster than real-time to stream
        limit: Maximum number of records to stream (None for all)
        max_delay: Maximum delay between records in seconds (None for unlimited)
        mode: Streaming mode - 'proportional', 'fixed', or 'adaptive'
              - proportional: Maintains exact time proportions (can be slow with large gaps)
              - fixed: Fixed delay between all records
              - adaptive: Caps delays at max_delay while preserving relative timing (RECOMMENDED)
    """
    data = load_all_tle_data()
    
    if limit:
        data = data[:limit]
    
    count = 0
    prev_time = None
    total_real_time = 0
    total_stream_time = 0
    
    for record in data:
        if prev_time is not None:
            # Calculate the time difference in seconds
            time_diff = (record['epoch_dt'] - prev_time).total_seconds()
            total_real_time += time_diff
            
            # Calculate sleep time based on mode
            if mode == 'fixed':
                # Fixed delay regardless of actual time gap
                sleep_time = 1.0 / acceleration_factor if acceleration_factor > 0 else 0
            elif mode == 'adaptive':
                # Proportional but capped at max_delay (RECOMMENDED for variable gaps)
                sleep_time = time_diff / acceleration_factor
                if max_delay is not None and sleep_time > max_delay:
                    sleep_time = max_delay
            else:  # proportional (default)
                # Maintain exact time proportions (WARNING: can be very slow with large gaps)
                sleep_time = time_diff / acceleration_factor
            
            # Apply sleep
            if sleep_time > 0:
                time.sleep(sleep_time)
                total_stream_time += sleep_time
        
        # Prepare the record for streaming (remove datetime object)
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


@app.route('/')
def index():
    """API documentation endpoint."""
    return jsonify({
        'name': 'TLE Streaming API',
        'version': '1.0',
        'endpoints': {
            '/': 'API documentation (this page)',
            '/stream': 'Stream TLE data in chronological order',
            '/stats': 'Get statistics about the TLE dataset'
        },
        'stream_parameters': {
            'acceleration': f'Acceleration factor (default: {DEFAULT_ACCELERATION}x)',
            'limit': 'Maximum number of records to stream (optional)',
            'max_delay': f'Maximum delay between records in seconds (default: {DEFAULT_MAX_DELAY}s)',
            'mode': 'Streaming mode: proportional, fixed, or adaptive (default: adaptive)'
        },
        'streaming_modes': {
            'proportional': 'Maintains exact time proportions (WARNING: slow with large gaps)',
            'fixed': 'Fixed delay between all records (1/acceleration seconds)',
            'adaptive': 'Caps large gaps at max_delay, preserves short gaps (RECOMMENDED)'
        },
        'example': f'/stream?acceleration={DEFAULT_ACCELERATION}&limit=1000&max_delay=2&mode=adaptive'
    })


@app.route('/stream')
def stream():
    """
    Stream TLE data in chronological order.
    
    Query parameters:
        - acceleration: Speed multiplier (default: 100)
        - limit: Maximum number of records (optional)
        - max_delay: Maximum delay between records in seconds (default: 5.0)
        - mode: Streaming mode - proportional, fixed, or adaptive (default: adaptive)
    """
    try:
        acceleration = int(request.args.get('acceleration', DEFAULT_ACCELERATION))
        limit = request.args.get('limit')
        limit = int(limit) if limit else None
        
        max_delay = request.args.get('max_delay', DEFAULT_MAX_DELAY)
        max_delay = float(max_delay) if max_delay != 'none' else None
        
        mode = request.args.get('mode', 'adaptive')
        if mode not in ['proportional', 'fixed', 'adaptive']:
            return jsonify({'error': 'Mode must be proportional, fixed, or adaptive'}), 400
        
        if acceleration <= 0:
            return jsonify({'error': 'Acceleration must be positive'}), 400
        
        return Response(
            stream_with_context(generate_stream(acceleration, limit, max_delay, mode)),
            mimetype='application/x-ndjson',
            headers={
                'X-Acceleration-Factor': str(acceleration),
                'X-Max-Delay': str(max_delay),
                'X-Stream-Mode': mode,
                'Cache-Control': 'no-cache',
                'X-Content-Type-Options': 'nosniff'
            }
        )
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/stats')
def stats():
    """Get statistics about the TLE dataset."""
    try:
        data = load_all_tle_data()
        
        if not data:
            return jsonify({'error': 'No data available'}), 404
        
        # Count satellites
        satellites = set(record['satellite_id'] for record in data)
        
        # Time range
        min_date = data[0]['epoch']
        max_date = data[-1]['epoch']
        
        # Calculate total time span
        time_span = data[-1]['epoch_dt'] - data[0]['epoch_dt']
        
        return jsonify({
            'total_records': len(data),
            'total_satellites': len(satellites),
            'date_range': {
                'start': min_date,
                'end': max_date,
                'span_days': time_span.days,
                'span_hours': time_span.total_seconds() / 3600
            },
            'sample_satellites': sorted(list(satellites))[:20]
        })
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })


if __name__ == '__main__':
    # Verify data directory exists
    if not os.path.exists(TLE_DATA_DIR):
        print(f"ERROR: TLE data directory not found: {TLE_DATA_DIR}")
        print(f"Please ensure the Output/TLE_History directory exists")
        exit(1)
    
    print(f"TLE data directory: {TLE_DATA_DIR}")
    print(f"Starting Flask server...")
    print(f"Access the API at: http://localhost:5000")
    print(f"Stream endpoint: http://localhost:5000/stream?acceleration={DEFAULT_ACCELERATION}")
    
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
