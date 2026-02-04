import pandas as pd
import numpy as np
from pathlib import Path

# Configuration
NUM_STATIONS = 5500  
DATA_DIR = Path("data")

# Regional distribution (targeting 5500 total)
REGIONAL_DISTRIBUTION = {
    'Northeast': 770,    
    'Southeast': 963,    
    'Midwest': 1100,     
    'Southwest': 770,    
    'West': 1045,        
    'Alaska': 426,       
    'Hawaii': 426        
}

# State to region mapping
STATE_TO_REGION = {
    # Northeast
    'ME': 'Northeast', 'NH': 'Northeast', 'VT': 'Northeast', 'MA': 'Northeast',
    'RI': 'Northeast', 'CT': 'Northeast', 'NY': 'Northeast', 'NJ': 'Northeast',
    'PA': 'Northeast', 'MD': 'Northeast', 'DE': 'Northeast',
    # Southeast
    'VA': 'Southeast', 'WV': 'Southeast', 'NC': 'Southeast', 'SC': 'Southeast',
    'GA': 'Southeast', 'FL': 'Southeast', 'AL': 'Southeast', 'MS': 'Southeast',
    'LA': 'Southeast', 'TN': 'Southeast', 'KY': 'Southeast', 'AR': 'Southeast',
    # Midwest
    'OH': 'Midwest', 'IN': 'Midwest', 'IL': 'Midwest', 'MI': 'Midwest',
    'WI': 'Midwest', 'MN': 'Midwest', 'IA': 'Midwest', 'MO': 'Midwest',
    'ND': 'Midwest', 'SD': 'Midwest', 'NE': 'Midwest', 'KS': 'Midwest',
    # Southwest
    'TX': 'Southwest', 'OK': 'Southwest', 'NM': 'Southwest', 'AZ': 'Southwest',
    # West
    'CA': 'West', 'NV': 'West', 'OR': 'West', 'WA': 'West',
    'ID': 'West', 'UT': 'West', 'CO': 'West', 'WY': 'West', 'MT': 'West',
    # Alaska & Hawaii
    'AK': 'Alaska', 'HI': 'Hawaii'
}

def load_and_filter_stations():
    """Load station metadata and filter US stations"""
    print("\n=== Loading Station Metadata ===")
    
    stations_file = DATA_DIR / "raw" / "stations" / "ghcnd-stations.txt"
    
    if not stations_file.exists():
        print("ERROR: Station metadata not found. Run download_noaa_data.py first!")
        return None
    
    stations = []
    with open(stations_file, 'r') as f:
        for line in f:
            if len(line) >= 71 and line[0:2] == 'US':  # US stations only
                station = {
                    'STATION_ID': line[0:11].strip(),
                    'LATITUDE': float(line[12:20].strip()),
                    'LONGITUDE': float(line[21:30].strip()),
                    'ELEVATION': float(line[31:37].strip()) if line[31:37].strip() else 0,
                    'STATE': line[38:40].strip(),
                    'NAME': line[41:71].strip()
                }
                
                # Add region
                station['REGION'] = STATE_TO_REGION.get(station['STATE'], 'Unknown')
                
                if station['REGION'] != 'Unknown':
                    stations.append(station)
    
    df = pd.DataFrame(stations)
    print(f"Total US stations: {len(df)}")
    print(f"States covered: {df['STATE'].nunique()}")
    
    return df

def select_representative_stations(df_all_stations):
    """Select 5500 stations distributed across regions"""
    print("\n=== Selecting Representative Stations ===")
    print(f"Target: {NUM_STATIONS} stations total\n")
    
    selected_stations = []
    
    for region, target_count in REGIONAL_DISTRIBUTION.items():
        region_stations = df_all_stations[df_all_stations['REGION'] == region].copy()
        
        if len(region_stations) == 0:
            print(f"  {region}: No stations found!")
            continue
        
        # Sort by elevation for diversity
        region_stations = region_stations.sort_values('ELEVATION')
        
        # Take evenly spaced stations
        if len(region_stations) <= target_count:
            selected = region_stations
            print(f"  {region}: Selected {len(selected):4d} stations (all available)")
        else:
            indices = np.linspace(0, len(region_stations)-1, target_count, dtype=int)
            selected = region_stations.iloc[indices]
            print(f"  {region}: Selected {len(selected):4d}/{target_count} stations")
        
        selected_stations.append(selected)
    
    df_selected = pd.concat(selected_stations, ignore_index=True)
    
    print(f"\n{'='*70}")
    print(f"Total selected: {len(df_selected)} stations")
    print(f"{'='*70}")
    
    return df_selected

def save_station_list(df_selected):
    """Save selected station list"""
    output_file = DATA_DIR / "selected_stations.csv"
    df_selected.to_csv(output_file, index=False)
    print(f"\n✓ Station list saved to: {output_file}")
    
    ids_file = DATA_DIR / "selected_station_ids.txt"
    with open(ids_file, 'w') as f:
        for station_id in df_selected['STATION_ID']:
            f.write(f"{station_id}\n")
    
    print(f"✓ Station IDs saved to: {ids_file}")
    
    return output_file

def display_statistics(df_selected):
    """Display selection statistics"""
    print("\n=== Selection Statistics ===")
    
    print(f"\nElevation range:")
    print(f"  Min: {df_selected['ELEVATION'].min():.1f} m")
    print(f"  Max: {df_selected['ELEVATION'].max():.1f} m")
    print(f"  Mean: {df_selected['ELEVATION'].mean():.1f} m")
    
    print(f"\nGeographic spread:")
    print(f"  Latitude: {df_selected['LATITUDE'].min():.2f}° to {df_selected['LATITUDE'].max():.2f}°")
    print(f"  Longitude: {df_selected['LONGITUDE'].min():.2f}° to {df_selected['LONGITUDE'].max():.2f}°")
    
    print(f"\nStates covered: {df_selected['STATE'].nunique()}")

if __name__ == "__main__":
    print("=" * 70)
    print("Station Selection Script - 5,500 Stations")
    print("=" * 70)
    
    df_all = load_and_filter_stations()
    
    if df_all is not None:
        df_selected = select_representative_stations(df_all)
        save_station_list(df_selected)
        display_statistics(df_selected)
        
        print("\n" + "=" * 70)
        print("✓ Selection Complete!")
        print("=" * 70)
        print("\nExpected result with 5,500 stations (25 years):")
        print("  ~1.0-1.2 GB in long format")