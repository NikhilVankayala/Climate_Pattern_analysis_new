# combine_without_pivot.py

import pandas as pd
from pathlib import Path
import time

DATA_DIR = Path("data")

def combine_without_pivot():
    """Combine all filtered years WITHOUT pivoting"""
    print("\n=== Combining Filtered Data (No Pivot) ===")
    
    filtered_dir = DATA_DIR / "filtered"
    files = sorted(list(filtered_dir.glob("*_filtered.csv")))
    
    if not files:
        print("No filtered files found!")
        return
    
    all_data = []
    
    for file in files:
        print(f"  Reading {file.name}...")
        # Fix: specify dtypes to avoid mixed type warning
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
    
    # Convert units (FIX: Convert entire column, not with loc)
    print("Converting units...")
    
    # Convert VALUE column to float first
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
    print(f"\nSaving dataset (long format)...")
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
    
    # Show records per year
    df_clean['year'] = df_clean['DATE'].astype(str).str[:4]
    print(f"\nRecords by year (first 5 and last 5):")
    year_counts = df_clean['year'].value_counts().sort_index()
    print(year_counts.head())
    print("...")
    print(year_counts.tail())
    
    return df_clean

if __name__ == "__main__":
    start_time = time.time()
    
    print("=" * 70)
    print("Combine Data Without Pivoting")
    print("=" * 70)
    
    df = combine_without_pivot()
    
    elapsed = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"✓ Complete!")
    print(f"{'=' * 70}")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"\nYou now have ~25 million records in long format!")
    print(f"This is perfect for analysis - much better than 8.5M records.")