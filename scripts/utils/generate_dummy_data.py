import os
import csv
import random
from datetime import datetime, timedelta
import math

OUTPUT_DIR = "Output/TLE_History"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def create_dummy_tle(norad_id, epoch):
    """Generates a valid TLE string pair for ISS-like orbit."""
    # Calculate TLE epoch format: YYDDD.FFFFFFFF (year + day of year with fractional day)
    year_2digit = epoch.strftime('%y')
    day_of_year = epoch.timetuple().tm_yday
    fraction_of_day = (epoch.hour * 3600 + epoch.minute * 60 + epoch.second) / 86400.0
    # Combine day and fraction into single number: DDD.FFFFFFFF
    epoch_day = day_of_year + fraction_of_day
    tle_epoch = f"{year_2digit}{epoch_day:012.8f}"  # Format: YYDDD.FFFFFFFF (14 chars)
    
    # Standard ISS-like TLE with proper formatting
    line1 = f"1 {norad_id:05d}U 98067A   {tle_epoch}  .00016901  00000-0  30629-3 0  999"
    line2 = f"2 {norad_id:05d}  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537"
    
    # Add simple checksum (sum of digits mod 10, '-' counts as 1)
    def checksum(line):
        total = 0
        for c in line[:-1]:  # Exclude last position
            if c.isdigit():
                total += int(c)
            elif c == '-':
                total += 1
        return str(total % 10)
    
    line1 = line1[:-1] + checksum(line1)
    
    return line1, line2

def generate_satellite_history(norad_id, num_records=100):
    filename = os.path.join(OUTPUT_DIR, f"{norad_id}_tle.csv")
    print(f"Generating {filename}...")
    
    start_date = datetime.now() - timedelta(days=1)
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['EPOCH', 'TLE_LINE1', 'TLE_LINE2'])
        
        for i in range(num_records):
            current_time = start_date + timedelta(minutes=10*i)
            epoch = current_time 
            line1, line2 = create_dummy_tle(norad_id, epoch)
            writer.writerow([epoch.isoformat(), line1, line2])

# Generate for 5 dummy satellites
for sat_id in range(25544, 25549):
    generate_satellite_history(sat_id)

print("✅ Dummy TLE data generated in Output/TLE_History")
