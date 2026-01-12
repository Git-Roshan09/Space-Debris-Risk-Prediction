import os
import pandas as pd

# =============================
# PATHS
# =============================
TLE_DIR = "Output/TLE_History"
OUT_DIR = "Output/TLE_Processed"

os.makedirs(OUT_DIR, exist_ok=True)

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

    # Keep TLE data as-is
    df_out = df[["EPOCH", "TLE_LINE1", "TLE_LINE2"]].copy()

    out_file = os.path.join(OUT_DIR, f"{norad_id}_tle.csv")
    df_out.to_csv(out_file, index=False)

    print(f"Saved {len(df_out)} TLEs → {out_file}")

print("🎯 Finished TLE processing.")
