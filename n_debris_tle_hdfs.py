"""
TLE History Fetcher - HDFS Direct Ingestion
Fetches TLE history from Space-Track.org and writes directly to HDFS.
"""

import os
import json
import io
import requests
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from time import sleep

# =============================
# CONFIG
# =============================
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

N_DEBRIS = int(os.getenv("N_DEBRIS", 35731))  # Number of debris objects to fetch
START_INDEX = int(os.getenv("START_INDEX", 298))  # Start from this index (0-based, so 298 = 299th row)
SLEEP_SEC = 2  # Space-Track rate limit safety

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_BASE_PATH = "/space-debris-webhdfs"

def write_to_hdfs(df, hdfs_path, filename, replication=1):
    """
    Write a DataFrame directly to HDFS using WebHDFS REST API.
    
    Args:
        replication: HDFS replication factor (default: 1 for single-node cluster)
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue().encode('utf-8')
    
    full_path = f"{hdfs_path}/{filename}"
    
    # Create directory
    mkdir_url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}"
    requests.put(mkdir_url)
    
    # Initiate file creation with replication factor
    create_url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{full_path}?op=CREATE&overwrite=true&replication={replication}&user.name={HDFS_USER}"
    
    response = requests.put(create_url, allow_redirects=False)
    
    if response.status_code == 307:
        datanode_url = response.headers['Location']
        datanode_url = datanode_url.replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
        
        upload_response = requests.put(datanode_url, data=csv_data,
                                       headers={'Content-Type': 'application/octet-stream'})
        
        return upload_response.status_code == 201
    return False

def check_file_exists_hdfs(hdfs_path):
    """Check if a file already exists in HDFS."""
    url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{hdfs_path}?op=GETFILESTATUS&user.name={HDFS_USER}"
    response = requests.get(url)
    return response.status_code == 200

def load_catalog_from_hdfs():
    """Load the debris catalog from HDFS."""
    catalog_path = f"{HDFS_BASE_PATH}/catalog/space_debris_catalog.csv"
    url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{catalog_path}?op=OPEN&user.name={HDFS_USER}"
    
    # Don't follow redirects automatically - we need to fix the datanode hostname
    response = requests.get(url, allow_redirects=False)
    
    if response.status_code == 307:
        # Get redirect URL and replace Docker hostname with localhost
        datanode_url = response.headers['Location']
        datanode_url = datanode_url.replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
        
        # Now fetch the actual data
        data_response = requests.get(datanode_url)
        if data_response.status_code == 200:
            df = pd.read_csv(io.StringIO(data_response.text))
            return df
        else:
            raise FileNotFoundError(f"Failed to read catalog: {data_response.status_code}")
    elif response.status_code == 200:
        df = pd.read_csv(io.StringIO(response.text))
        return df
    else:
        raise FileNotFoundError(f"Catalog not found in HDFS. Run debris_hdfs.py first! Status: {response.status_code}")

def main():
    # Initialize Space-Track client
    st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)
    
    print(f"📦 Loading debris catalog from HDFS...")
    
    try:
        df_catalog = load_catalog_from_hdfs()
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return
    
    # Get NORAD IDs
    norad_ids = (
        df_catalog["NORAD_CAT_ID"]
        .dropna()
        .astype(int)
        .unique()[:N_DEBRIS]
    )
    
    print(f"🚀 Extracting TLEs for debris objects {START_INDEX+1} to {len(norad_ids)} → HDFS")
    
    for i, norad_id in enumerate(norad_ids[START_INDEX:], start=START_INDEX+1):
        tle_filename = f"{norad_id}_tle.csv"
        tle_hdfs_path = f"{HDFS_BASE_PATH}/tle-history/{tle_filename}"
        
        # Skip if already exists
        if check_file_exists_hdfs(tle_hdfs_path):
            print(f"[{i}/{len(norad_ids)}] ⏭️ NORAD {norad_id} — already in HDFS, skipping")
            continue
        
        print(f"[{i}/{len(norad_ids)}] Fetching NORAD {norad_id}...", end=" ")
        
        try:
            response = st.gp_history(
                norad_cat_id=norad_id,
                orderby="epoch asc",
                format="json"
            )
            
            data = json.loads(response) if isinstance(response, str) else response
            
            if not data:
                print("⚠️ No TLEs found")
                continue
            
            df_tle = pd.DataFrame(data)
            
            # Validate columns
            required = {"EPOCH", "TLE_LINE1", "TLE_LINE2"}
            if not required.issubset(df_tle.columns):
                print("⚠️ Missing required fields")
                continue
            
            # Keep only needed columns
            df_tle = df_tle[["EPOCH", "TLE_LINE1", "TLE_LINE2"]]
            df_tle["EPOCH"] = pd.to_datetime(df_tle["EPOCH"], utc=True)
            df_tle = df_tle.drop_duplicates()
            
            # Write directly to HDFS
            success = write_to_hdfs(df_tle, f"{HDFS_BASE_PATH}/tle-history", tle_filename)
            
            if success:
                print(f"✅ {len(df_tle)} TLEs → HDFS")
            else:
                print("❌ Upload failed")
            
            sleep(SLEEP_SEC)
            
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n🎯 Finished TLE ingestion to HDFS!")

if __name__ == "__main__":
    main()
