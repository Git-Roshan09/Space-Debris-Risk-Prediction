import os
import json
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from time import sleep
import requests

# =============================
# CONFIG
# =============================
N_SATELLITES = 10000   # Number of satellites to process
SLEEP_SEC = 5          # Space-Track rate limit safety
BATCH_SIZE = 50        # Pause longer after every batch
BATCH_SLEEP = 60       # Extra sleep after each batch (seconds)

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
SATELLITE_CSV = os.path.join(BASE_OUTPUT, "satellites_and_objects_catalog.csv")
TLE_DIR = os.path.join(BASE_OUTPUT, "TLE_History_Satellites")

os.makedirs(TLE_DIR, exist_ok=True)

# =============================
# LOAD SATELLITE IDS
# =============================
df = pd.read_csv(SATELLITE_CSV)

# Filter for active satellites (you can adjust the filter criteria)
norad_ids = (
    df["NORAD_CAT_ID"]
    .dropna()
    .astype(int)
    .unique()[:N_SATELLITES]
)

print(f"🛰️  Extracting timestamped TLEs for {len(norad_ids)} satellites")

# =============================
# INIT CLIENT
# =============================
st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

# =============================
# FETCH TLE HISTORY
# =============================
for i, norad_id in enumerate(norad_ids, start=1):
    out_file = os.path.join(TLE_DIR, f"{norad_id}_tle.csv")

    # 🔹 SKIP IF CSV ALREADY EXISTS
    if os.path.exists(out_file):
        print(f"[{i}/{len(norad_ids)}] ⏭️  NORAD {norad_id} — already exists, skipping")
        continue

    print(f"[{i}/{len(norad_ids)}] NORAD {norad_id}")

    # Retry logic with exponential backoff
    max_retries = 3
    retry_count = 0
    success = False

    while retry_count < max_retries and not success:
        try:
            response = st.gp_history(
                norad_cat_id=norad_id,
                orderby="epoch asc",
                format="json"
            )

            data = json.loads(response) if isinstance(response, str) else response

            if not data:
                print(f"⚠️  No TLEs for {norad_id}")
                success = True
                break

            df_tle = pd.DataFrame(data)

            # Ensure required columns exist
            required = {"EPOCH", "TLE_LINE1", "TLE_LINE2"}
            if not required.issubset(df_tle.columns):
                print(f"⚠️  Missing required fields for {norad_id}")
                print(df_tle)
                success = True
                break

            # Keep ONLY timestamp + TLE lines
            df_tle = df_tle[["EPOCH", "TLE_LINE1", "TLE_LINE2"]]

            # Convert timestamp
            df_tle["EPOCH"] = pd.to_datetime(df_tle["EPOCH"], utc=True)

            # Remove duplicates
            df_tle = df_tle.drop_duplicates()

            # Save
            df_tle.to_csv(out_file, index=False)

            print(f"✅ Saved {len(df_tle)} timestamped TLEs → {out_file}")
            success = True

        except requests.exceptions.HTTPError as e:
            if "429" in str(e) or "rate" in str(e).lower():
                retry_count += 1
                wait_time = SLEEP_SEC * (2 ** retry_count)  # Exponential backoff
                print(f"⚠️  Rate limit hit for {norad_id}. Waiting {wait_time}s... (retry {retry_count}/{max_retries})")
                sleep(wait_time)
            else:
                print(f"❌ HTTP Error for NORAD {norad_id}: {e}")
                break
        except Exception as e:
            print(f"❌ Error for NORAD {norad_id}: {e}")
            break

    if success:
        sleep(SLEEP_SEC)
        
        # Batch pause
        if i % BATCH_SIZE == 0:
            print(f"⏸️  Batch pause: waiting {BATCH_SLEEP}s after {i} requests...")
            sleep(BATCH_SLEEP)

print("🎯 Finished extracting timestamped TLE history for satellites.")
