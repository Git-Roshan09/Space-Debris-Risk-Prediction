"""
Space Debris Catalog Fetcher - HDFS Direct Ingestion
Fetches debris catalog from Space-Track.org and writes directly to HDFS.
"""

import os
import json
import io
import requests
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient

# =============================
# CONFIG
# =============================
load_dotenv()
USERNAME = os.getenv("SPACETRACK_USER")
PASSWORD = os.getenv("SPACETRACK_PASS")

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_BASE_PATH = "/space-debris-webhdfs"

def write_to_hdfs(df, hdfs_path, filename, replication=1):
    """
    Write a DataFrame directly to HDFS using WebHDFS REST API.
    
    Args:
        df: pandas DataFrame to upload
        hdfs_path: HDFS directory path
        filename: Name of the file to create
        replication: HDFS replication factor (default: 1 for single-node cluster)
    """
    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue().encode('utf-8')
    
    # WebHDFS URL for creating file
    full_path = f"{hdfs_path}/{filename}"
    
    # Step 1: Create directory if not exists
    mkdir_url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}"
    requests.put(mkdir_url)
    
    # Step 2: Initiate file creation (get redirect URL)
    # Added replication parameter to set replication factor
    create_url = f"http://{HDFS_NAMENODE}:{HDFS_PORT}/webhdfs/v1{full_path}?op=CREATE&overwrite=true&replication={replication}&user.name={HDFS_USER}"
    
    response = requests.put(create_url, allow_redirects=False)
    
    if response.status_code == 307:
        # Step 3: Follow redirect to DataNode and upload data
        datanode_url = response.headers['Location']
        # Replace container hostname with localhost if needed
        datanode_url = datanode_url.replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
        
        upload_response = requests.put(datanode_url, data=csv_data, 
                                       headers={'Content-Type': 'application/octet-stream'})
        
        if upload_response.status_code == 201:
            print(f"✅ Successfully uploaded to HDFS: {full_path}")
            return True
        else:
            print(f"❌ Upload failed: {upload_response.status_code} - {upload_response.text}")
            return False
    else:
        print(f"❌ Failed to initiate upload: {response.status_code} - {response.text}")
        return False

def main():
    # Initialize Space-Track client
    st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)
    
    print("🚀 Requesting debris-only catalog data from Space-Track...")
    
    try:
        # Fetch debris objects from SATCAT
        response = st.satcat(
            object_type="DEBRIS",
            current="Y",
            orderby="launch asc",
            format="json"
        )
        
        # Parse response
        data = json.loads(response) if isinstance(response, str) else response
        
        if not data:
            print("No debris records found.")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Select useful columns
        keep_columns = [
            "NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "COUNTRY",
            "LAUNCH", "SITE", "DECAY", "PERIOD", "INCLINATION",
            "APOGEE", "PERIGEE", "RCS_SIZE"
        ]
        df = df[[c for c in keep_columns if c in df.columns]]
        
        # Convert numeric fields
        numeric_cols = ["PERIOD", "INCLINATION", "APOGEE", "PERIGEE"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        print(f"📦 Total debris objects retrieved: {len(df)}")
        
        # Write directly to HDFS
        write_to_hdfs(df, f"{HDFS_BASE_PATH}/catalog", "space_debris_catalog.csv")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
