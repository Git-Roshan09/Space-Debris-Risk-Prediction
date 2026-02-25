#!/usr/bin/env python3
"""
Quick simulation time advancement script - simplified version
"""

import requests
import sys
import json

API_URL = 'http://localhost:5001'

def advance_simulation(days=1):
    """Advance simulation time by specified days."""
    try:
        response = requests.post(
            f"{API_URL}/api/simulation/advance",
            json={"days": days},
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Simulation advanced to: {data['current_simulated_time']}")
            print(f"   Elapsed days: {data['elapsed_days']}")
            return True
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
        return False

def get_status():
    """Get current simulation status."""
    try:
        response = requests.get(f"{API_URL}/api/simulation/time")
        if response.status_code == 200:
            data = response.json()
            print(f"🕐 Current simulation time: {data['current_simulated_time']}")
            print(f"   Elapsed days: {data['elapsed_simulated_days']}")
            print(f"   Mode: {data['simulation_mode']}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--status":
            get_status()
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python advance_sim.py         # Advance by 1 day")
            print("  python advance_sim.py 5       # Advance by 5 days")
            print("  python advance_sim.py --status # Show current time")
        else:
            try:
                days = int(sys.argv[1])
                advance_simulation(days)
            except ValueError:
                print("Error: Please provide a number of days")
                sys.exit(1)
    else:
        advance_simulation(1)