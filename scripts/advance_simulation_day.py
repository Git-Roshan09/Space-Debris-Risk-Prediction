#!/usr/bin/env python3
"""
Helper script to advance the simulation time after processing completes.

This script should be called after your daily processing pipeline finishes
to move the simulation clock forward by one day.

DEPLOYMENT OPTIONS:
  
  1. Run on Host Machine (default):
     python scripts/advance_simulation_day.py --days 1
     
     Uses http://localhost:5001 (port exposed from dashboard-api container)
  
  2. Run Inside Container (e.g., Airflow):
     export DASHBOARD_API_URL=http://dashboard-api:5001
     python scripts/advance_simulation_day.py --days 1
     
     Uses internal Docker network to reach dashboard-api container

USAGE EXAMPLES:
    # Advance by 1 day (default)
    python scripts/advance_simulation_day.py
    
    # Advance by specific amount
    python scripts/advance_simulation_day.py --days 1 --hours 6
    
    # Check current simulation time
    python scripts/advance_simulation_day.py --status
    
    # Reset to epoch
    python scripts/advance_simulation_day.py --reset
    
    # Use custom API URL
    python scripts/advance_simulation_day.py --api-url http://dashboard-api:5001
"""

import requests
import argparse
import sys
import json
import os
from datetime import datetime

# Dashboard API endpoint
# Use environment variable for flexibility (container vs host)
# - From host machine: http://localhost:5001
# - From inside container: http://dashboard-api:5001
DASHBOARD_API_URL = os.getenv('DASHBOARD_API_URL', 'http://localhost:5001')


def get_simulation_status():
    """Get current simulation time and status."""
    try:
        response = requests.get(f"{DASHBOARD_API_URL}/api/simulation/time")
        response.raise_for_status()
        data = response.json()
        
        print("=" * 60)
        print("SIMULATION STATUS")
        print("=" * 60)
        print(f"Current Simulated Time: {data['current_simulated_time']}")
        print(f"Simulation Epoch:       {data['simulation_epoch']}")
        print(f"Elapsed Days:           {data['elapsed_simulated_days']:.2f}")
        print(f"Simulation Mode:        {data['simulation_mode']}")
        print(f"Description:            {data['simulation_description']}")
        print(f"Locked:                 {data.get('simulation_locked', False)}")
        print("=" * 60)
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to Dashboard API: {e}", file=sys.stderr)
        print(f"   Make sure the API is running at {DASHBOARD_API_URL}", file=sys.stderr)
        sys.exit(1)


def advance_simulation(days=1, hours=0, minutes=0):
    """Advance the simulation time."""
    try:
        payload = {
            "days": days,
            "hours": hours,
            "minutes": minutes
        }
        
        response = requests.post(
            f"{DASHBOARD_API_URL}/api/simulation/advance",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        data = response.json()
        
        print("=" * 60)
        print("SIMULATION ADVANCED")
        print("=" * 60)
        print(f"New Simulated Time: {data['current_simulated_time']}")
        print(f"Elapsed Days:       {data['elapsed_days']:.2f}")
        print(f"Advanced by:        {days} days, {hours} hours, {minutes} minutes")
        print("=" * 60)
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Error advancing simulation: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"   Server error: {error_data.get('error', 'Unknown error')}", file=sys.stderr)
            except:
                pass
        sys.exit(1)


def reset_simulation(epoch=None):
    """Reset simulation to epoch."""
    try:
        payload = {"reset": True}
        if epoch:
            payload["epoch"] = epoch
        
        response = requests.post(
            f"{DASHBOARD_API_URL}/api/simulation/set",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        data = response.json()
        
        print("=" * 60)
        print("SIMULATION RESET")
        print("=" * 60)
        print(f"Current Simulated Time: {data['current_simulated_time']}")
        print(f"Simulation Epoch:       {data['simulation_epoch']}")
        print("=" * 60)
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Error resetting simulation: {e}", file=sys.stderr)
        sys.exit(1)


def set_simulation_time(iso_datetime):
    """Set simulation to specific datetime."""
    try:
        payload = {"time": iso_datetime}
        
        response = requests.post(
            f"{DASHBOARD_API_URL}/api/simulation/set",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        data = response.json()
        
        print("=" * 60)
        print("SIMULATION TIME SET")
        print("=" * 60)
        print(f"Current Simulated Time: {data['current_simulated_time']}")
        print("=" * 60)
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Error setting simulation time: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Advance or manage simulation time after processing completes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check current status
  %(prog)s --status
  
  # Advance by 1 day (typical after daily processing)
  %(prog)s --days 1
  
  # Advance by 1 day and 6 hours
  %(prog)s --days 1 --hours 6
  
  # Reset to epoch
  %(prog)s --reset
  
  # Set to specific datetime
  %(prog)s --set-time "2004-01-15T00:00:00"
        """
    )
    
    parser.add_argument(
        '--status', '-s',
        action='store_true',
        help='Show current simulation status'
    )
    
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=1,
        help='Number of days to advance (default: 1)'
    )
    
    parser.add_argument(
        '--hours', '-H',
        type=int,
        default=0,
        help='Number of hours to advance (default: 0)'
    )
    
    parser.add_argument(
        '--minutes', '-m',
        type=int,
        default=0,
        help='Number of minutes to advance (default: 0)'
    )
    
    parser.add_argument(
        '--reset', '-r',
        action='store_true',
        help='Reset simulation to epoch'
    )
    
    parser.add_argument(
        '--set-time', '-t',
        type=str,
        help='Set simulation to specific datetime (ISO format)'
    )
    
    parser.add_argument(
        '--api-url',
        type=str,
        default=DASHBOARD_API_URL,
        help=f'Dashboard API URL (default: {DASHBOARD_API_URL} or env DASHBOARD_API_URL)'
    )
    
    args = parser.parse_args()
    
    # Use the API URL from args
    api_url = args.api_url
    
    # Handle different actions
    if args.status:
        get_simulation_status()
    elif args.reset:
        reset_simulation()
    elif args.set_time:
        set_simulation_time(args.set_time)
    else:
        # Default: advance simulation
        advance_simulation(days=args.days, hours=args.hours, minutes=args.minutes)


if __name__ == "__main__":
    main()
