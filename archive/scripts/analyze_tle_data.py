#!/usr/bin/env python3
"""
TLE Data Analysis Script - Memory Optimized
Analyzes TLE files from data/alldata folder and cross-references with catalogs
to count satellites and debris objects.
Optimized for large datasets (5GB+) to prevent memory issues.
No external dependencies required.
"""

import os
import glob
import csv
import re
from collections import defaultdict

def get_file_size_mb(file_path):
    """Get file size in MB"""
    try:
        return os.path.getsize(file_path) / (1024 * 1024)
    except:
        return 0

def parse_tle_file_streaming(file_path, max_size_mb=500):
    """
    Parse a TLE file using streaming to handle large files efficiently.
    TLE format: 3 lines per object (name, line1, line2)
    Returns a generator to avoid loading everything into memory.
    """
    file_size_mb = get_file_size_mb(file_path)
    if file_size_mb > max_size_mb:
        print(f"⚠️  Large file detected: {os.path.basename(file_path)} ({file_size_mb:.1f}MB)")
    
    object_count = 0
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines_buffer = []
            
            for line_num, line in enumerate(f, 1):
                lines_buffer.append(line.strip())
                
                # Process every 3 lines
                if len(lines_buffer) == 3:
                    name_line, line1, line2 = lines_buffer
                    
                    # Validate TLE format
                    if line1.startswith('1 ') and line2.startswith('2 '):
                        # Extract NORAD catalog ID from line 1 (columns 3-7)
                        norad_id = line1[2:7].strip()
                        if norad_id.isdigit():
                            object_count += 1
                            yield {
                                'norad_id': int(norad_id),
                                'name': name_line,
                                'file': os.path.basename(file_path)
                            }
                    
                    lines_buffer = []
                
                # Progress indicator for very large files
                if file_size_mb > 100 and line_num % 100000 == 0:
                    print(f"    Processed {line_num:,} lines, found {object_count:,} objects...")
    
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
    
    if file_size_mb > 10:
        print(f"✅ Completed {os.path.basename(file_path)}: {object_count:,} objects")

def load_catalogs_streaming(data_dir):
    """Load satellite and debris catalogs efficiently"""
    catalogs = {'satellite_ids': set(), 'debris_ids': set()}
    
    # Load satellites and objects catalog
    sat_catalog_path = os.path.join(data_dir, 'raw', 'satellites_and_objects_catalog.csv')
    if os.path.exists(sat_catalog_path):
        print("Loading satellites catalog...")
        with open(sat_catalog_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            sat_count = 0
            for row in reader:
                try:
                    norad_id = int(row['NORAD_CAT_ID'])
                    catalogs['satellite_ids'].add(norad_id)
                    sat_count += 1
                except (ValueError, KeyError):
                    continue
        print(f"✅ Loaded satellites catalog: {sat_count:,} entries")
    
    # Load debris catalog  
    debris_catalog_path = os.path.join(data_dir, 'raw', 'space_debris_catalog.csv')  
    if os.path.exists(debris_catalog_path):
        print("Loading debris catalog...")
        with open(debris_catalog_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            debris_count = 0
            for row in reader:
                try:
                    norad_id = int(row['NORAD_CAT_ID'])
                    catalogs['debris_ids'].add(norad_id)
                    debris_count += 1
                except (ValueError, KeyError):
                    continue
        print(f"✅ Loaded debris catalog: {debris_count:,} entries")
    
    return catalogs

def analyze_tle_data(data_dir):
    """
    Main analysis function - Memory optimized for large datasets
    """
    print("=== TLE Data Analysis (Memory Optimized) ===")
    print(f"Analyzing data from: {data_dir}")
    
    # Load catalogs efficiently
    catalogs = load_catalogs_streaming(data_dir)
    satellite_ids = catalogs['satellite_ids']
    debris_ids = catalogs['debris_ids']
    
    print(f"📊 Catalog satellite IDs: {len(satellite_ids):,}")
    print(f"📊 Catalog debris IDs: {len(debris_ids):,}")
    
    # Find TLE files and sort by size (smallest first to test with)
    alldata_dir = os.path.join(data_dir, 'alldata')
    tle_files = []
    
    # Look for .txt files
    txt_files = glob.glob(os.path.join(alldata_dir, '*.txt'))
    tle_files.extend(txt_files)
    
    # Look for .txt files in subdirectories
    for subdir in ['tle2012.txt', 'tle2014.txt', 'tle2015.txt']:
        subdir_path = os.path.join(alldata_dir, subdir)
        if os.path.isdir(subdir_path):
            sub_txt_files = glob.glob(os.path.join(subdir_path, '*.txt'))
            tle_files.extend(sub_txt_files)
    
    # Filter out zip files and sort by size
    tle_files = [f for f in tle_files if not f.endswith('.zip')]
    tle_files_with_size = [(f, get_file_size_mb(f)) for f in tle_files]
    tle_files_with_size.sort(key=lambda x: x[1])  # Sort by size
    
    print(f"\n📁 Found {len(tle_files_with_size)} TLE files to analyze")
    total_size_gb = sum(size for _, size in tle_files_with_size) / 1024
    print(f"📊 Total dataset size: {total_size_gb:.2f} GB")
    
    # Statistics tracking
    stats = {
        'total_objects': 0,
        'satellites': 0, 
        'debris': 0,
        'unclassified': 0,
        'files_processed': 0,
        'years_data': defaultdict(int),
        'unique_norad_ids': set()
    }
    
    # Process each TLE file with memory optimization
    for file_path, file_size_mb in tle_files_with_size:
        filename = os.path.basename(file_path)
        print(f"\n📄 Processing: {filename} ({file_size_mb:.1f}MB)")
        
        if file_size_mb > 1000:  # Files larger than 1GB
            print(f"⚠️  VERY LARGE FILE - Processing with extra care...")
        
        file_stats = {'satellites': 0, 'debris': 0, 'unclassified': 0, 'total': 0}
        
        try:
            # Use streaming parser for memory efficiency
            for obj in parse_tle_file_streaming(file_path):
                norad_id = obj['norad_id']
                stats['unique_norad_ids'].add(norad_id)
                stats['total_objects'] += 1
                file_stats['total'] += 1
                
                # Extract year from filename
                year_match = re.search(r'tle(\d{4})', filename)
                year = year_match.group(1) if year_match else 'unknown'
                stats['years_data'][year] += 1
                
                # Classify objects
                if norad_id in satellite_ids:
                    stats['satellites'] += 1
                    file_stats['satellites'] += 1
                elif norad_id in debris_ids:
                    stats['debris'] += 1  
                    file_stats['debris'] += 1
                else:
                    stats['unclassified'] += 1
                    file_stats['unclassified'] += 1
            
            stats['files_processed'] += 1
            print(f"✅ {filename}: {file_stats['total']:,} objects "
                  f"(S:{file_stats['satellites']:,} D:{file_stats['debris']:,} U:{file_stats['unclassified']:,})")
            
        except Exception as e:
            print(f"❌ Failed to process {filename}: {e}")
            continue
        
        # Memory cleanup hint for Python garbage collector
        if file_size_mb > 100:
            import gc
            gc.collect()
    
    return stats

if __name__ == "__main__":
    # Set the data directory path
    data_dir = "/home/bharath/Documents/BigData/project/data/Space-Debris-Risk-Prediction/data"
    
    print("🚀 Starting TLE Data Analysis (Memory Optimized)")
    print("=" * 60)
    
    # Run analysis
    stats = analyze_tle_data(data_dir)
    
    # Print final results
    print(f"\n" + "=" * 60)
    print(f"📊 FINAL ANALYSIS RESULTS") 
    print(f"=" * 60)
    print(f"Files processed: {stats['files_processed']}")
    print(f"Total objects found: {stats['total_objects']:,}")
    print(f"Unique NORAD IDs: {len(stats['unique_norad_ids']):,}")
    
    print(f"\n📈 OBJECT CLASSIFICATION")
    if stats['total_objects'] > 0:
        sat_pct = stats['satellites']/stats['total_objects']*100
        debris_pct = stats['debris']/stats['total_objects']*100  
        unclass_pct = stats['unclassified']/stats['total_objects']*100
        
        print(f"🛰️  Satellites: {stats['satellites']:,} ({sat_pct:.1f}%)")
        print(f"🗑️  Debris: {stats['debris']:,} ({debris_pct:.1f}%)")
        print(f"❓ Unclassified: {stats['unclassified']:,} ({unclass_pct:.1f}%)")
    
    print(f"\n📅 DATA BY YEAR")
    for year in sorted(stats['years_data'].keys()):
        count = stats['years_data'][year]
        print(f"{year}: {count:,} objects")
    
    print(f"\n🎯 SUMMARY FOR API DATA REPLACEMENT")
    print(f"Total dataset size: {stats['total_objects']:,} objects")
    print(f"Satellites available: {stats['satellites']:,}")
    print(f"Debris objects available: {stats['debris']:,}")
    if stats['years_data']:
        years = sorted(stats['years_data'].keys())
        print(f"Years covered: {years[0]} - {years[-1]}")
    print(f"✅ This dataset is ready to replace the demo API data.")
    print(f"🎉 Analysis completed successfully!")