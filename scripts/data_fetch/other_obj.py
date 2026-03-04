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

# 2. Initialize client
st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

print("Requesting satellite and non-debris object catalog data...")

try:
    # 3. Fetch NON-DEBRIS objects from SATCAT
    response = st.satcat(
        object_type=["PAYLOAD", "ROCKET BODY", "UNKNOWN"],
        current="Y",            # Only currently tracked objects
        orderby="launch asc",
        format="json"
    )

    # 4. Parse response
    data = json.loads(response) if isinstance(response, str) else response

    if not data:
        print("No satellite or object records found.")
        exit()

    # 5. Convert to DataFrame
    df = pd.DataFrame(data)

    # 6. Keep meaningful non-TLE metadata
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

    # 7. Convert numeric columns
    numeric_cols = ["PERIOD", "INCLINATION", "APOGEE", "PERIGEE"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 8. Save CSV
    output_file = os.path.join(OUTPUT_DIR, "satellites_and_objects_catalog.csv")
    df.to_csv(output_file, index=False)

    print(f"✅ Satellite & object catalog saved to: {output_file}")
    print(f"🛰️ Total objects retrieved: {len(df)}")

except Exception as e:
    print(f"❌ Error occurred: {e}")
