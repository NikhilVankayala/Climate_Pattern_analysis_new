import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")

print("=" * 70)
print("DATA DIAGNOSTIC")
print("=" * 70)

# 1. Check station selection
stations = pd.read_csv(DATA_DIR / "selected_stations.csv")
print(f"\n1. Selected stations file: {len(stations)} stations")

with open(DATA_DIR / "selected_station_ids.txt", 'r') as f:
    station_ids = set(line.strip() for line in f)
print(f"   Station IDs file: {len(station_ids)} IDs")

# 2. Check filtered files
filtered_dir = DATA_DIR / "filtered"
filtered_files = list(filtered_dir.glob("*_filtered.csv"))
print(f"\n2. Filtered files: {len(filtered_files)} years")

if filtered_files:
    total_filtered_size = sum(f.stat().st_size for f in filtered_files)
    print(f"   Total filtered size: {total_filtered_size / (1024**2):.1f} MB")
    
    # Sample one filtered file
    sample_file = filtered_files[0]
    df_sample = pd.read_csv(sample_file, nrows=100)
    print(f"\n   Sample from {sample_file.name}:")
    print(f"   Columns: {list(df_sample.columns)}")
    print(f"   First few station IDs: {df_sample['STATION'].head().tolist()}")
    
    # Count records per file
    print(f"\n   Records per filtered file:")
    for f in sorted(filtered_files)[:5]:  # Show first 5 years
        with open(f, 'r') as file:
            line_count = sum(1 for _ in file) - 1  # Subtract header
        size_mb = f.stat().st_size / (1024**2)
        print(f"   {f.name}: {line_count:,} records ({size_mb:.1f} MB)")

# 3. Check final dataset
final_file = DATA_DIR / "final_dataset.csv"
if final_file.exists():
    size_mb = final_file.stat().st_size / (1024**2)
    print(f"\n3. Final dataset: {size_mb:.1f} MB")
    
    # Load and analyze
    print("   Loading final dataset...")
    df = pd.read_csv(final_file)
    
    print(f"\n   Records: {len(df):,}")
    print(f"   Unique stations: {df['STATION'].nunique()}")
    print(f"   Date range: {df['DATE'].min()} to {df['DATE'].max()}")
    
    # Check years
    df['year'] = df['DATE'].astype(str).str[:4]
    print(f"   Years present: {sorted(df['year'].unique())}")
    print(f"   Records per year:")
    print(df['year'].value_counts().sort_index())
    
    # Check columns
    print(f"\n   Columns: {list(df.columns)}")
    print(f"\n   Data coverage:")
    for col in df.columns:
        if col not in ['STATION', 'DATE', 'year']:
            coverage = (df[col].notna().sum() / len(df)) * 100
            print(f"   {col:6s}: {coverage:>6.1f}%")
    
    # Top stations by record count
    print(f"\n   Top 10 stations by record count:")
    station_counts = df['STATION'].value_counts().head(10)
    for station, count in station_counts.items():
        print(f"   {station}: {count:,} records")
    
    # Calculate expected size
    print(f"\n   ANALYSIS:")
    expected_records = len(station_ids) * 365 * 25 * 0.6  # 60% coverage
    print(f"   Expected records (with 60% activity): {expected_records:,.0f}")
    print(f"   Actual records: {len(df):,}")
    print(f"   Ratio: {len(df) / expected_records * 100:.1f}%")
    
else:
    print("\n3. ✗ Final dataset not found!")

print("\n" + "=" * 70)