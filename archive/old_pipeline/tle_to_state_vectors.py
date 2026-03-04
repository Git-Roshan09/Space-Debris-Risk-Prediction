import os
import pandas as pd
from sgp4.api import Satrec, jday
import numpy as np

# =============================
# CONFIG
# =============================
TLE_DIR = "Output/TLE_History"
OUT_DIR = "Output/TLE_Processed"

os.makedirs(OUT_DIR, exist_ok=True)

def process_tle_row(row):
    """
    Converts a single TLE row into State Vectors (Position & Velocity)
    using SGP4 propagation at the specific Epoch.
    """
    try:
        line1 = row['TLE_LINE1']
        line2 = row['TLE_LINE2']
        dt = row['EPOCH']
        
        # Initialize Satellite object
        satellite = Satrec.twoline2rv(line1, line2)
        
        # Convert Datetime to Julian Date for SGP4
        # Note: jday expects (year, month, day, hour, minute, second)
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond/1e6)
        
        # Propagate to the exact epoch
        error_code, position, velocity = satellite.sgp4(jd, fr)
        
        if error_code != 0:
            # SGP4 Error (e.g. decayed elements)
            return pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
            
        # Returns:
        # Position (r): x, y, z in kilometers
        # Velocity (v): vx, vy, vz in km/second
        return pd.Series([
            position[0], position[1], position[2],
            velocity[0], velocity[1], velocity[2]
        ])
        
    except Exception as e:
        return pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])

# =============================
# PROCESS FILES
# =============================
tle_files = [f for f in os.listdir(TLE_DIR) if f.endswith("_tle.csv")]

print(f"🚀 Starting processing for {len(tle_files)} satellite files...")

for i, fname in enumerate(tle_files, 1):
    norad_id = fname.split("_")[0]
    
    in_path = os.path.join(TLE_DIR, fname)
    out_path = os.path.join(OUT_DIR, f"{norad_id}_state_vectors.csv")
    
    # Read Data
    df = pd.read_csv(in_path)
    
    # Parse timestamp
    df["EPOCH"] = pd.to_datetime(df["EPOCH"], utc=True)
    
    print(f"[{i}/{len(tle_files)}] Processing NORAD {norad_id} ({len(df)} records)...", end=" ")
    
    # ⚡ Apply SGP4 Calculation
    state_vectors = df.apply(process_tle_row, axis=1)
    state_vectors.columns = ['POS_X', 'POS_Y', 'POS_Z', 'VEL_X', 'VEL_Y', 'VEL_Z']
    
    # Merge Clean Results
    df_final = pd.concat([df, state_vectors], axis=1)
    
    # Remove rows where calculation failed (NaNs)
    df_final.dropna(subset=['POS_X'], inplace=True)
    
    # Save processed data
    df_final.to_csv(out_path, index=False)
    
    print(f"✅ Saved {len(df_final)} vectors")

print("\n🎯 All TLEs successfully converted to State Vectors!")
