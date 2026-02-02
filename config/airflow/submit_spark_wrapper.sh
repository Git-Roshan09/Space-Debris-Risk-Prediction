#!/usr/bin/env python3
"""Submit Spark job via HTTP trigger from Airflow container."""

import requests
import json
import sys
from datetime import datetime

TRIGGER_URL = "http://spark-master:6066/submit"

def submit_job():
    """Submit Spark streaming job via HTTP trigger."""
    
    print(f"[{datetime.now()}] Triggering Spark job submission...")
    print(f"Endpoint: {TRIGGER_URL}")
    
    try:
        response = requests.post(TRIGGER_URL, timeout=10)
        result = response.json()
        
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(result, indent=2)}")
        
        if response.status_code == 200 and result.get("success"):
            print(f"✓ Job submitted successfully (PID: {result.get('pid')})")
            return 0
        else:
            print(f"✗ Submission failed: {result.get('error')}")
            return 1
            
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to Spark trigger service")
        print("Ensure Spark master is running")
        return 1
    except Exception as e:
        print(f"✗ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(submit_job())
