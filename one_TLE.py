import os
import json
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient

# 1. Load credentials and setup folder
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

OUTPUT_DIR = "Output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 2. Load debris catalog CSV
DEBRIS_CSV = os.path.join(OUTPUT_DIR, "space_debris_catalog.csv")
df_debris = pd.read_csv(DEBRIS_CSV)

# Select ONE debris object (example: first valid NORAD ID)
norad_id = int(df_debris.iloc[0]["NORAD_CAT_ID"])
print(f"Requesting full TLE time series for debris NORAD ID: {norad_id}")

# 3. Initialize Space-Track client
st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

try:
    # 4. Fetch FULL GP history (no time constraint)
    response = st.gp_history(
        norad_cat_id=norad_id,
        orderby="epoch asc",
        format="json"
    )

    # 5. Parse response
    data = json.loads(response) if isinstance(response, str) else response

    if not data:
        print("No TLE history available for this debris object.")
        exit()

    # 6. Convert to DataFrame
    df = pd.DataFrame(data)

    # 7. Convert numeric columns (Space-Track returns strings)
    numeric_cols = [
        "INCLINATION",
        "ECCENTRICITY",
        "MEAN_MOTION",
        "BSTAR",
        "MEAN_MOTION_DOT",
        "MEAN_MOTION_DDOT"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 8. Save time-series CSV
    output_file = os.path.join(
        OUTPUT_DIR, f"tle_history_debris_{norad_id}.csv"
    )
    df.to_csv(output_file, index=False)

    print(f"✅ TLE time series saved to: {output_file}")
    print(f"📈 Total epochs retrieved: {len(df)}")

except Exception as e:
    print(f"❌ Error occurred: {e}")
