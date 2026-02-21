#!/usr/bin/env python3
"""
Optimized TLE Data API - Production Ready
Serves classified satellite and debris data from comprehensive TLE dataset
Optimized for large datasets (65M+ objects) with satellite/debris classification
"""

import os
import glob
import csv
import re
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
from flask import Flask, jsonify, request, Response, stream_with_context
from flask_cors import CORS
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

TLE_DATA_DIR = 'data/alldata'
CATALOG_DIR = 'data/raw'
CACHE_EXPIRE_HOURS = 6
MAX_OBJECTS_PER_REQUEST = 10000
DEFAULT_SAMPLE_SIZE = 1000
_object_catalog = {
    'satellites': set(),
    'debris': set(),
    'metadata': {},
    'loaded_at': None
}

_tle_cache = {
    'satellites_sample': [],
    'debris_sample': [],
    'stats': {},
    'loaded_at': None
}


class OptimizedTLEProvider:
    """Efficiently loads and serves classified TLE data"""
    
    @staticmethod
    def load_object_catalogs():
        """
        Load satellite and debris classification catalogs from CSV files.
        
        Returns cached data if loaded recently (within CACHE_EXPIRE_HOURS).
        Loads both satellites_and_objects_catalog.csv and space_debris_catalog.csv
        to classify objects as SATELLITE, DEBRIS, or UNKNOWN.
        
        Returns:
            dict: Object catalog containing satellites set, debris set, 
                  metadata dict, and loaded_at timestamp
        """
        global _object_catalog
        
        if (_object_catalog['loaded_at'] and 
            (datetime.now() - _object_catalog['loaded_at']).seconds < CACHE_EXPIRE_HOURS * 3600):
            return _object_catalog
        
        logger.info("🔄 Loading object classification catalogs...")
        
        _object_catalog['satellites'].clear()
        _object_catalog['debris'].clear()
        _object_catalog['metadata'].clear()
        sat_catalog_path = os.path.join(CATALOG_DIR, 'satellites_and_objects_catalog.csv')
        if os.path.exists(sat_catalog_path):
            with open(sat_catalog_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        norad_id = int(row['NORAD_CAT_ID'])
                        _object_catalog['satellites'].add(norad_id)
                        _object_catalog['metadata'][norad_id] = {
                            'name': row.get('OBJECT_NAME', ''),
                            'type': 'SATELLITE',
                            'object_type': row.get('OBJECT_TYPE', ''),
                            'country': row.get('COUNTRY', ''),
                            'launch': row.get('LAUNCH', ''),
                            'rcs_size': row.get('RCS_SIZE', '')
                        }
                    except (ValueError, KeyError):
                        continue
            logger.info(f"✅ Loaded {len(_object_catalog['satellites']):,} satellite classifications")
        debris_catalog_path = os.path.join(CATALOG_DIR, 'space_debris_catalog.csv')
        if os.path.exists(debris_catalog_path):
            with open(debris_catalog_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        norad_id = int(row['NORAD_CAT_ID'])
                        _object_catalog['debris'].add(norad_id)
                        _object_catalog['metadata'][norad_id] = {
                            'name': row.get('OBJECT_NAME', ''),
                            'type': 'DEBRIS',
                            'object_type': row.get('OBJECT_TYPE', ''),
                            'country': row.get('COUNTRY', ''),
                            'launch': row.get('LAUNCH', ''),
                            'rcs_size': row.get('RCS_SIZE', '')
                        }
                    except (ValueError, KeyError):
                        continue
            logger.info(f"✅ Loaded {len(_object_catalog['debris']):,} debris classifications")
        
        _object_catalog['loaded_at'] = datetime.now()
        return _object_catalog
    
    @staticmethod
    def classify_object(norad_id):
        """
        Classify a space object by NORAD ID as SATELLITE, DEBRIS, or UNKNOWN.
        
        Args:
            norad_id (int): NORAD catalog ID of the space object
            
        Returns:
            str: Classification as 'SATELLITE', 'DEBRIS', or 'UNKNOWN'
        """
        catalog = OptimizedTLEProvider.load_object_catalogs()
        
        if norad_id in catalog['satellites']:
            return 'SATELLITE'
        elif norad_id in catalog['debris']:
            return 'DEBRIS'
        else:
            return 'UNKNOWN'
    
    @staticmethod
    def get_object_metadata(norad_id):
        """
        Retrieve detailed metadata for a space object by NORAD ID.
        
        Args:
            norad_id (int): NORAD catalog ID of the space object
            
        Returns:
            dict: Object metadata including name, type, country, launch date, RCS size
        """
        catalog = OptimizedTLEProvider.load_object_catalogs()
        return catalog['metadata'].get(norad_id, {})
    
    @staticmethod
    def parse_tle_file_sampling(file_path, max_objects=5000):
        """
        Parse TLE file with intelligent sampling for large files.
        Supports both 2-line and 3-line TLE formats.
        """
        satellite_ids = _object_catalog['satellites']
        debris_ids = _object_catalog['debris']
        metadata = _object_catalog['metadata']
        
        satellites = []
        debris = []
        objects_processed = 0
        sampling_ratio = 1
        
        try:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > 100:
                estimated_objects = file_size_mb * 1000
                if estimated_objects > max_objects:
                    sampling_ratio = int(estimated_objects / max_objects)
                    logger.info(f"📊 Large file detected ({file_size_mb:.1f}MB), using 1:{sampling_ratio} sampling")
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines_buffer = []
                is_two_line_format = None
                
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    lines_buffer.append(line)
                    
                    if is_two_line_format is None and len(lines_buffer) >= 2:
                        if lines_buffer[0].startswith('1 ') and lines_buffer[1].startswith('2 '):
                            is_two_line_format = True
                        elif not lines_buffer[0].startswith('1 ') and lines_buffer[1].startswith('1 '):
                            is_two_line_format = False
                    
                    expected_lines = 2 if is_two_line_format else 3
                    
                    if len(lines_buffer) >= expected_lines:
                        if objects_processed % sampling_ratio == 0:
                            if is_two_line_format:
                                line1, line2 = lines_buffer[0], lines_buffer[1]
                                name_line = None
                            else:
                                name_line, line1, line2 = lines_buffer[0], lines_buffer[1], lines_buffer[2]
                            
                            if line1.startswith('1 ') and line2.startswith('2 '):
                                norad_id_str = line1[2:7].strip()
                                if norad_id_str.isdigit():
                                    norad_id = int(norad_id_str)
                                    
                                    obj_metadata = metadata.get(norad_id, {})
                                    object_name = obj_metadata.get('name', '')
                                    if not object_name and name_line:
                                        object_name = name_line
                                    if not object_name:
                                        object_name = f"NORAD-{norad_id}"
                                    
                                    tle_object = {
                                        'norad_id': norad_id,
                                        'name': object_name,
                                        'tle_line1': line1,
                                        'tle_line2': line2,
                                        'classification': OptimizedTLEProvider.classify_object(norad_id),
                                        'metadata': obj_metadata,
                                        'source_file': os.path.basename(file_path)
                                    }
                                    

                                    if norad_id in satellite_ids:
                                        satellites.append(tle_object)
                                    elif norad_id in debris_ids:
                                        debris.append(tle_object)
                        
                        objects_processed += 1
                        if is_two_line_format:
                            lines_buffer = lines_buffer[2:]
                        else:
                            lines_buffer = lines_buffer[3:]
                        
                        if len(satellites) + len(debris) >= max_objects:
                            break
                
        except Exception as e:
            logger.error(f"❌ Error parsing {file_path}: {e}")
            
        return satellites, debris
    
    @staticmethod
    def load_sample_data():
        """
        Load representative sample of satellites and debris for API endpoints.
        
        Returns cached data if loaded recently. Processes TLE files to extract
        classified objects up to DEFAULT_SAMPLE_SIZE for each type.
        
        Returns:
            dict: Cache containing satellites_sample, debris_sample, stats, and loaded_at timestamp
        """
        global _tle_cache
        
        if (_tle_cache['loaded_at'] and 
            (datetime.now() - _tle_cache['loaded_at']).seconds < CACHE_EXPIRE_HOURS * 3600):
            return _tle_cache
        
        logger.info("🚀 Loading TLE sample data...")
        
        _tle_cache['satellites_sample'].clear()
        _tle_cache['debris_sample'].clear()
        
        OptimizedTLEProvider.load_object_catalogs()
        tle_files = []
        txt_files = glob.glob(os.path.join(TLE_DATA_DIR, '*.txt'))
        tle_files.extend([f for f in txt_files if not f.endswith('.zip')])
        
        for subdir in ['tle2012.txt', 'tle2014.txt', 'tle2015.txt']:
            subdir_path = os.path.join(TLE_DATA_DIR, subdir)
            if os.path.isdir(subdir_path):
                sub_files = glob.glob(os.path.join(subdir_path, '*.txt'))
                tle_files.extend(sub_files)
        
        tle_files_with_size = [(f, os.path.getsize(f)) for f in tle_files]
        tle_files_with_size.sort(key=lambda x: x[1])
        
        logger.info(f"📁 Found {len(tle_files_with_size)} TLE files")
        
        total_satellites = 0
        total_debris = 0
        years_processed = set()
        
        for file_path, file_size in tle_files_with_size:
            if total_satellites >= DEFAULT_SAMPLE_SIZE and total_debris >= DEFAULT_SAMPLE_SIZE:
                break
                
            filename = os.path.basename(file_path)
            logger.info(f"📄 Processing {filename} ({file_size/1024/1024:.1f}MB)")
            
            year_match = re.search(r'tle(\d{4})', filename)
            if year_match:
                years_processed.add(year_match.group(1))
            
            satellites, debris = OptimizedTLEProvider.parse_tle_file_sampling(file_path)
            
            _tle_cache['satellites_sample'].extend(satellites)
            _tle_cache['debris_sample'].extend(debris)
            
            total_satellites = len(_tle_cache['satellites_sample'])
            total_debris = len(_tle_cache['debris_sample'])
            
            logger.info(f"  ✅ Added {len(satellites)} satellites, {len(debris)} debris")
            logger.info(f"  📊 Running totals: {total_satellites} satellites, {total_debris} debris")
        
        _tle_cache['stats'] = {
            'satellites_count': total_satellites,
            'debris_count': total_debris,
            'total_objects': total_satellites + total_debris,
            'years_covered': sorted(list(years_processed)),
            'last_updated': datetime.now().isoformat()
        }
        
        _tle_cache['loaded_at'] = datetime.now()
        
        logger.info(f"🎉 Sample data loaded successfully!")
        logger.info(f"📊 Current sample: {total_satellites} satellites, {total_debris} debris")
        
        return _tle_cache


@app.route('/api/health', methods=['GET'])
def health_check():
    """
    Health check endpoint to verify API status and cache state.
    
    Returns:
        JSON response with service status, timestamp, and cache information
    """
    return jsonify({
        'status': 'healthy',
        'service': 'TLE Data API',
        'timestamp': datetime.now().isoformat(),
        'cache_status': {
            'catalogs_loaded': _object_catalog['loaded_at'] is not None,
            'tle_cache_loaded': _tle_cache['loaded_at'] is not None
        }
    })


@app.route('/api/stats', methods=['GET'])
def get_data_stats():
    """
    Get comprehensive dataset statistics including object counts and coverage.
    
    Returns:
        JSON response with dataset statistics from loaded sample data
    """
    cache = OptimizedTLEProvider.load_sample_data()
    
    return jsonify({
        'success': True,
        'data': cache['stats']
    })


@app.route('/api/objects/satellites', methods=['GET'])
def get_satellites():
    """
    Get satellite data with optional filtering and pagination.
    
    Query Parameters:
        limit (int): Maximum objects to return (default: DEFAULT_SAMPLE_SIZE)
        offset (int): Number of objects to skip for pagination (default: 0)
        country (str): Filter by country code (optional)
    
    Returns:
        JSON response with satellite objects, pagination info, and metadata
    """
    try:
        limit = min(int(request.args.get('limit', DEFAULT_SAMPLE_SIZE)), MAX_OBJECTS_PER_REQUEST)
        offset = int(request.args.get('offset', 0))
        country_filter = request.args.get('country', '').upper()
        
        cache = OptimizedTLEProvider.load_sample_data()
        satellites = cache['satellites_sample']
        
        if country_filter:
            satellites = [s for s in satellites 
                         if s.get('metadata', {}).get('country', '').upper() == country_filter]
        
        total = len(satellites)
        satellites_page = satellites[offset:offset + limit]
        
        return jsonify({
            'success': True,
            'data': {
                'objects': satellites_page,
                'pagination': {
                    'total': total,
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total
                },
                'classification': 'SATELLITE'
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting satellites: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/objects/debris', methods=['GET'])
def get_debris():
    """
    Get debris data with optional filtering and pagination.
    
    Query Parameters:
        limit (int): Maximum objects to return (default: DEFAULT_SAMPLE_SIZE)
        offset (int): Number of objects to skip for pagination (default: 0)
        rcs_size (str): Filter by radar cross-section size category (optional)
    
    Returns:
        JSON response with debris objects, pagination info, and metadata
    """
    try:
        limit = min(int(request.args.get('limit', DEFAULT_SAMPLE_SIZE)), MAX_OBJECTS_PER_REQUEST)
        offset = int(request.args.get('offset', 0))
        rcs_filter = request.args.get('rcs_size', '').upper()
        
        cache = OptimizedTLEProvider.load_sample_data()
        debris = cache['debris_sample']
        
        if rcs_filter:
            debris = [d for d in debris 
                     if d.get('metadata', {}).get('rcs_size', '').upper() == rcs_filter]
        
        total = len(debris)
        debris_page = debris[offset:offset + limit]
        
        return jsonify({
            'success': True,
            'data': {
                'objects': debris_page,
                'pagination': {
                    'total': total,
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total
                },
                'classification': 'DEBRIS'
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting debris: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/objects/collision-pairs', methods=['GET'])
def get_collision_pairs():
    """
    Get objects specifically formatted for collision prediction analysis.
    
    Only returns satellite-satellite and satellite-debris pairs as requested.
    Excludes debris-debris collisions from the response.
    
    Query Parameters:
        satellite_limit (int): Maximum satellites to return (default: 500)
        debris_limit (int): Maximum debris objects to return (default: 500)
    
    Returns:
        JSON response with satellites and debris arrays for collision analysis
    """
    try:
        satellite_limit = int(request.args.get('satellite_limit', 500))
        debris_limit = int(request.args.get('debris_limit', 500))
        
        cache = OptimizedTLEProvider.load_sample_data()
        
        satellites = cache['satellites_sample'][:satellite_limit]
        debris = cache['debris_sample'][:debris_limit]
        
        return jsonify({
            'success': True,
            'data': {
                'satellites': satellites,
                'debris': debris,
                'collision_types_supported': ['SAT-SAT', 'SAT-DEB'],
                'note': 'DEB-DEB collisions are excluded as requested'
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting collision pairs: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/objects/stream', methods=['GET'])
def stream_objects():
    """
    Stream TLE data for real-time processing using Server-Sent Events.
    
    Query Parameters:
        type (str): Object type filter ('satellites', 'debris', 'all') - default: 'all'
        batch_size (int): Objects per batch (1-1000) - default: 100
        delay_ms (int): Delay between batches in milliseconds - default: 100
    
    Returns:
        Server-Sent Events stream with batched object data
    """
    def generate_stream():
        try:
            cache = OptimizedTLEProvider.load_sample_data()
            
            object_type = request.args.get('type', 'all').lower()
            batch_size = min(int(request.args.get('batch_size', 100)), 1000)
            delay_ms = int(request.args.get('delay_ms', 100))
            
            if object_type == 'satellites':
                data_stream = cache['satellites_sample']
            elif object_type == 'debris':
                data_stream = cache['debris_sample']
            else:
                data_stream = cache['satellites_sample'] + cache['debris_sample']
            
            for i in range(0, len(data_stream), batch_size):
                batch = data_stream[i:i + batch_size]
                yield f"data: {json.dumps({'batch': batch, 'batch_number': i//batch_size + 1})}\n\n"
                
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
                    
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(
        stream_with_context(generate_stream()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
        }
    )


if __name__ == '__main__':
    logger.info("🚀 Starting Optimized TLE Data API...")
    
    try:
        OptimizedTLEProvider.load_object_catalogs()
        logger.info("✅ Catalogs pre-loaded successfully")
    except Exception as e:
        logger.error(f"❌ Error pre-loading catalogs: {e}")
    
    app.run(host='0.0.0.0', port=5000, debug=False)