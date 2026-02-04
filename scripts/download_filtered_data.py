import pandas as pd
import gzip
import shutil
from pathlib import Path
import time
from datetime import datetime

# Configuration
DATA_DIR = Path("data")
START_YEAR = 2000
END_YEAR = 2024
BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"

def load_selected_stations():
    """Load list of selected station IDs"""
    ids_file = DATA_DIR / "selected_station_ids.txt"
    
    if not ids_file.exists():
        print("ERROR: selected_station_ids.txt not found!")
        print("Run select_stations.py first!")
        return None
    
    with open(ids_file, 'r') as f:
        station_ids = set(line.strip() for line in f)
    
    print(f"Loaded {len(station_ids)} station IDs")
    return station_ids

def download_and_filter_year(year, station_ids):
    """Download one year and filter to selected stations"""
    print(f"\nProcessing year {year}...")
    
    import requests
    
    # Download URL
    url = f"{BASE_URL}/by_year/{year}.csv.gz"
    temp_file = DATA_DIR / "temp" / f"{year}.csv.gz"
    temp_file.parent.mkdir(exist_ok=True)
    
    # Check if already filtered
    output_file = DATA_DIR / "filtered" / f"{year}_filtered.csv"
    output_file.parent.mkdir(exist_ok=True)
    
    if output_file.exists():
        size_mb = output_file.stat().st_size / (1024 ** 2)
        print(f"  Already exists ({size_mb:.1f} MB), skipping...")
        return True
    
    # Download
    print(f"  Downloading {year}...")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(temp_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"  ✓ Downloaded")
    except Exception as e:
        print(f"  ✗ Download failed: {e}")
        return False
    
    # Decompress and filter
    print(f"  Filtering to {len(station_ids)} stations...")
    
    records_kept = 0
    
    with gzip.open(temp_file, 'rt') as f_in:
        with open(output_file, 'w') as f_out:
            # Write header
            f_out.write("STATION,DATE,ELEMENT,VALUE,MFLAG,QFLAG,SFLAG,TIME\n")
            
            for line in f_in:
                station_id = line[:11].strip()
                if station_id in station_ids:
                    f_out.write(line)
                    records_kept += 1
    
    # Cleanup temp file
    temp_file.unlink()
    
    # Get file size
    size_mb = output_file.stat().st_size / (1024 ** 2)
    print(f"  ✓ Filtered: {records_kept:,} records ({size_mb:.1f} MB)")
    
    time.sleep(1)  # Be nice to server
    return True

def combine_and_clean_to_long_format():
    """Combine all filtered years and output in LONG format"""
    print("\n=== Combining and Cleaning Data (Long Format) ===")
    
    filtered_dir = DATA_DIR / "filtered"
    files = sorted(list(filtered_dir.glob("*_filtered.csv")))
    
    if not files:
        print("No filtered files found!")
        return
    
    all_data = []
    
    for file in files:
        print(f"  Reading {file.name}...")
        df = pd.read_csv(file, dtype={'QFLAG': str}, low_memory=False)
        all_data.append(df)
    
    # Combine
    print("Combining datasets...")
    df_combined = pd.concat(all_data, ignore_index=True)
    print(f"  Combined: {len(df_combined):,} records")
    
    # Filter quality
    print("Removing poor quality records...")
    df_clean = df_combined[df_combined['QFLAG'].isna()].copy()
    
    # Keep only relevant elements
    elements = ['TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD', 'AWND']
    df_clean = df_clean[df_clean['ELEMENT'].isin(elements)]
    
    print(f"  After quality filter: {len(df_clean):,} records")
    
    # Convert units
    print("Converting units...")
    
    # Convert VALUE column to float
    df_clean['VALUE'] = df_clean['VALUE'].astype(float)
    
    # Temperatures: tenths of °C to °C
    temp_mask = df_clean['ELEMENT'].isin(['TMAX', 'TMIN'])
    df_clean.loc[temp_mask, 'VALUE'] = df_clean.loc[temp_mask, 'VALUE'] / 10.0
    
    # Precipitation: tenths of mm to mm
    prcp_mask = df_clean['ELEMENT'] == 'PRCP'
    df_clean.loc[prcp_mask, 'VALUE'] = df_clean.loc[prcp_mask, 'VALUE'] / 10.0
    
    # Keep only essential columns
    df_clean = df_clean[['STATION', 'DATE', 'ELEMENT', 'VALUE']]
    
    # Save in LONG format
    output_file = DATA_DIR / "final_dataset_long.csv"
    print(f"\nSaving final dataset (long format)...")
    df_clean.to_csv(output_file, index=False)
    
    size_mb = output_file.stat().st_size / (1024**2)
    size_gb = size_mb / 1024
    
    print(f"\n✓ Final dataset saved: {output_file}")
    print(f"  Size: {size_gb:.2f} GB ({size_mb:.1f} MB)")
    print(f"  Records: {len(df_clean):,}")
    print(f"  Stations: {df_clean['STATION'].nunique()}")
    print(f"  Date range: {df_clean['DATE'].min()} to {df_clean['DATE'].max()}")
    
    # Show distribution
    print(f"\nRecords by element:")
    print(df_clean['ELEMENT'].value_counts().sort_index())

if __name__ == "__main__":
    start_time = time.time()
    
    print("=" * 70)
    print("Filtered Data Download & Combine Script (Long Format)")
    print("=" * 70)
    print(f"Years: {START_YEAR}-{END_YEAR}")
    print(f"Output: Long format (no pivot)")
    
    # Load selected stations
    station_ids = load_selected_stations()
    
    if station_ids:
        # Download and filter each year
        success_count = 0
        for year in range(START_YEAR, END_YEAR + 1):
            if download_and_filter_year(year, station_ids):
                success_count += 1
        
        print(f"\nSuccessfully processed: {success_count}/{END_YEAR - START_YEAR + 1} years")
        
        # Combine and clean to long format
        combine_and_clean_to_long_format()
    
    elapsed = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"✓ Complete!")
    print(f"{'=' * 70}")
    print(f"Total time: {elapsed/60:.1f} minutes")