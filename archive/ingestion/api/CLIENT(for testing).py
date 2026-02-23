#!/usr/bin/env python3
"""
Test client for the TLE streaming API.
Demonstrates how to consume the streaming endpoint.
"""

import requests
import json
import time
from datetime import datetime


def consume_tle_stream(base_url='http://localhost:5000', acceleration=100, limit=100):
    """
    Consume the TLE streaming API.
    
    Args:
        base_url: Base URL of the API
        acceleration: Acceleration factor
        limit: Number of records to consume
    """
    stream_url = f"{base_url}/stream?acceleration={acceleration}&limit={limit}"
    
    print(f"Connecting to TLE stream API: {stream_url}")
    print(f"Acceleration: {acceleration}x")
    print(f"Limit: {limit} records")
    print("-" * 80)
    
    try:
        response = requests.get(stream_url, stream=True, timeout=300)
        response.raise_for_status()
        
        count = 0
        start_time = time.time()
        
        for line in response.iter_lines():
            if line:
                try:
                    record = json.loads(line)
                    count += 1
                    
                    # Print every 10th record to avoid cluttering output
                    if count % 10 == 0 or count <= 5:
                        elapsed = time.time() - start_time
                        print(f"[{count:5d}] Satellite: {record['satellite_id']:8s} | "
                              f"Epoch: {record['epoch']} | "
                              f"Elapsed: {elapsed:.2f}s")
                    
                    # You can process the TLE data here
                    # Example: record['tle_line1'], record['tle_line2']
                    
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    continue
        
        elapsed = time.time() - start_time
        print("-" * 80)
        print(f"Stream complete!")
        print(f"Total records received: {count}")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Average rate: {count/elapsed:.2f} records/second")
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to API: {e}")
    except KeyboardInterrupt:
        print("\nStream interrupted by user")


def get_stats(base_url='http://localhost:5000'):
    """Get statistics from the API."""
    try:
        response = requests.get(f"{base_url}/stats", timeout=60)
        response.raise_for_status()
        
        stats = response.json()
        print("\n=== TLE Dataset Statistics ===")
        print(f"Total Records: {stats['total_records']:,}")
        print(f"Total Satellites: {stats['total_satellites']}")
        print(f"Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
        print(f"Time Span: {stats['date_range']['span_days']} days ({stats['date_range']['span_hours']:.1f} hours)")
        print(f"\nSample Satellites: {', '.join(stats['sample_satellites'][:10])}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error getting stats: {e}")


def main():
    """Main function."""
    base_url = 'http://localhost:5000'
    
    print("=" * 80)
    print("TLE Streaming API Test Client")
    print("=" * 80)
    
    # First, get stats
    get_stats(base_url)
    
    print("\n" + "=" * 80)
    input("Press Enter to start streaming (Ctrl+C to stop)...")
    
    # Stream with 100x acceleration, limit to 100 records for testing
    consume_tle_stream(
        base_url=base_url,
        acceleration=100,  # 100x faster
        limit=100  # Limit to 100 records for testing
    )


if __name__ == '__main__':
    main()
