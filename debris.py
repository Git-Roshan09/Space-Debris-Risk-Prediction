import os
import json
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient

# 1. Load credentials
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

OUTPUT_DIR = "Output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 2. Initialize Space-Track client
st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

print("Requesting debris-only catalog data (no TLEs)...")

try:
    # 3. Fetch debris objects from SATCAT
    response = st.satcat(
        object_type="DEBRIS",
        current="Y",          # Only currently tracked debris
        orderby="launch asc",
        format="json"
    )

    # 4. Parse response
    data = json.loads(response) if isinstance(response, str) else response

    if not data:
        print("No debris records found.")
        exit()

    # 5. Convert to DataFrame
    df = pd.DataFrame(data)

    # 6. Select ONLY useful debris metadata (no TLE fields)
    keep_columns = [
        "NORAD_CAT_ID",
        "OBJECT_NAME",
        "OBJECT_TYPE",
        "COUNTRY",
        "LAUNCH",
        "SITE",
        "DECAY",
        "PERIOD",
        "INCLINATION",
        "APOGEE",
        "PERIGEE",
        "RCS_SIZE"
    ]

    df = df[[c for c in keep_columns if c in df.columns]]

    # 7. Convert numeric fields
    numeric_cols = ["PERIOD", "INCLINATION", "APOGEE", "PERIGEE"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 8. Save CSV
    output_file = os.path.join(OUTPUT_DIR, "space_debris_catalog.csv")
    df.to_csv(output_file, index=False)

    print(f"✅ Debris-only catalog saved to: {output_file}")
    print(f"📦 Total debris objects retrieved: {len(df)}")

except Exception as e:
    print(f"❌ Error occurred: {e}")
