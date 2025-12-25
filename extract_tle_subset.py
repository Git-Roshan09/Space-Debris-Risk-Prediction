import os
import json
import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
import spacetrack.operators as op
from datetime import datetime

# 1. Load credentials and setup folder
load_dotenv()
USERNAME = os.getenv('SPACETRACK_USER')
PASSWORD = os.getenv('SPACETRACK_PASS')
OUTPUT_DIR = "Output"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 2. Initialize Client
st = SpaceTrackClient(identity=USERNAME, password=PASSWORD)

# 3. Define query parameters
norad_id = 25544
# For debris analysis, you might want a longer window (e.g., 1 year)
date_range = op.inclusive_range(datetime(2023, 1, 1), datetime(2023, 12, 31))

print(f"Requesting data for NORAD {norad_id}...")

try:
    # 4. Fetch data
    response = st.gp_history(
        norad_cat_id=norad_id, 
        creation_date=date_range, 
        orderby='epoch asc', 
        format='json'
    )

    # 5. Parse JSON response
    data = json.loads(response) if isinstance(response, str) else response

    if data:
        # 6. Save to CSV in the Output folder
        df = pd.DataFrame(data)
        
        # Convert numeric columns for analysis (Space-Track returns strings)
        numeric_cols = ['INCLINATION', 'ECCENTRICITY', 'MEAN_MOTION', 'BSTAR']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        filename = f"tle_history_{norad_id}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(filepath, index=False)
        print(f"Success! Data saved to: {filepath}")
        print(f"Total records retrieved: {len(df)}")
    else:
        print("No records found for that ID or date range.")

except Exception as e:
    print(f"An error occurred: {e}")