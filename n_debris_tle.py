import os
import json
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from time import sleep

# =============================
# CONFIG
# =============================
N_DEBRIS = 50        # Number of debris objects
SLEEP_SEC = 2        # Space-Track rate limit safety

# =============================
# LOAD CREDENTIALS
# =============================
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

# =============================
# PATHS
# =============================
BASE_OUTPUT = "Output"
DEBRIS_CSV = os.path.join(BASE_OUTPUT, "space_debris_catalog.csv")
TLE_DIR = os.path.join(BASE_OUTPUT, "TLE_History")

os.makedirs(TLE_DIR, exist_ok=True)

# =============================
# LOAD DEBRIS IDS
# =============================
df = pd.read_csv(DEBRIS_CSV)

norad_ids = (
    df["NORAD_CAT_ID"]
    .dropna()
    .astype(int)
    .unique()[:N_DEBRIS]
)

print(f"📦 Extracting timestamped TLEs for {len(norad_ids)} debris objects")

# =============================
# INIT CLIENT
# =============================
st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

# =============================
# FETCH TLE HISTORY
# =============================
for i, norad_id in enumerate(norad_ids, start=1):
    print(f"[{i}/{len(norad_ids)}] NORAD {norad_id}")

    try:
        response = st.gp_history(
            norad_cat_id=norad_id,
            orderby="epoch asc",
            format="json"
        )

        data = json.loads(response) if isinstance(response, str) else response

        if not data:
            print(f"⚠️ No TLEs for {norad_id}")
            continue

        df_tle = pd.DataFrame(data)

        # Ensure required columns exist
        required = {"EPOCH", "TLE_LINE1", "TLE_LINE2"}
        if not required.issubset(df_tle.columns):
            print(f"⚠️ Missing required fields for {norad_id}")
            continue

        # Keep ONLY timestamp + TLE lines
        df_tle = df_tle[["EPOCH", "TLE_LINE1", "TLE_LINE2"]]

        # Convert timestamp
        df_tle["EPOCH"] = pd.to_datetime(df_tle["EPOCH"], utc=True)

        # Remove duplicates
        df_tle = df_tle.drop_duplicates()

        # Save
        out_file = os.path.join(TLE_DIR, f"{norad_id}_tle.csv")
        df_tle.to_csv(out_file, index=False)

        print(f"✅ Saved {len(df_tle)} timestamped TLEs → {out_file}")

        sleep(SLEEP_SEC)

    except Exception as e:
        print(f"❌ Error for NORAD {norad_id}: {e}")

print("🎯 Finished extracting timestamped TLE history.")
