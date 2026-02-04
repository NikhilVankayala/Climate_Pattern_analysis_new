import pandas as pd

# Check selected stations
stations = pd.read_csv('data/selected_stations.csv')
print(f"Selected stations: {len(stations)}")

# Check final dataset
df = pd.read_csv('data/final_dataset.csv')
print(f"Stations in final dataset: {df['STATION'].nunique()}")
print(f"Records: {len(df):,}")
print(f"Date range: {df['DATE'].min()} to {df['DATE'].max()}")

# Size calculation
years = df['DATE'].astype(str).str[:4].nunique()
print(f"\nYears of data: {years}")
print(f"Avg records per station: {len(df) / df['STATION'].nunique():.0f}")
print(f"Expected for 1GB: ~1,000,000 records")
print(f"Current: {len(df):,} records")