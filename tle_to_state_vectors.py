import os
import pandas as pd
import numpy as np
from sgp4.api import Satrec, jday

# =============================
# PATHS
# =============================
TLE_DIR = "Output/TLE_History"
OUT_DIR = "Output/State_Vectors"

os.makedirs(OUT_DIR, exist_ok=True)

# =============================
# HELPERS
# =============================
def epoch_to_jday(epoch):
    """
    Convert pandas Timestamp → Julian Date
    """
    return jday(
        epoch.year,
        epoch.month,
        epoch.day,
        epoch.hour,
        epoch.minute,
        epoch.second + epoch.microsecond * 1e-6,
    )

# =============================
# PROCESS FILES
# =============================
for fname in os.listdir(TLE_DIR):
    if not fname.endswith("_tle.csv"):
        continue

    norad_id = fname.split("_")[0]
    print(f"📡 Processing NORAD {norad_id}")

    tle_path = os.path.join(TLE_DIR, fname)
    df = pd.read_csv(tle_path)

    # Parse timestamp
    df["EPOCH"] = pd.to_datetime(
    df["EPOCH"],
    utc=True,
    format="mixed"
)


    records = []

    for _, row in df.iterrows():
        line1 = row["TLE_LINE1"]
        line2 = row["TLE_LINE2"]
        epoch = row["EPOCH"]

        sat = Satrec.twoline2rv(line1, line2)

        jd, fr = epoch_to_jday(epoch)
        error, r, v = sat.sgp4(jd, fr)

        if error != 0:
            continue

        records.append({
            "EPOCH": epoch,
            "x_km": r[0],
            "y_km": r[1],
            "z_km": r[2],
            "vx_km_s": v[0],
            "vy_km_s": v[1],
            "vz_km_s": v[2],
        })

    if not records:
        print(f"⚠️ No valid states for {norad_id}")
        continue

    df_out = pd.DataFrame(records)

    out_file = os.path.join(OUT_DIR, f"{norad_id}_state_vectors.csv")
    df_out.to_csv(out_file, index=False)

    print(f"✅ Saved {len(df_out)} states → {out_file}")

print("🎯 Finished TLE → state vector conversion.")
