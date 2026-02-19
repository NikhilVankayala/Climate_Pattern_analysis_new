import requests
from pathlib import Path
import time
from datetime import datetime

# Configuration
BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
DATA_DIR = Path("data")

def create_directories():
    """Create directory structure"""
    (DATA_DIR / "raw" / "stations").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "raw" / "daily").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "filtered").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "temp").mkdir(parents=True, exist_ok=True)
    print("✓ Directories created")

def download_file(url, output_path):
    """Download a file with progress indicator"""
    try:
        print(f"  Downloading from: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        mb_downloaded = downloaded / (1024 ** 2)
                        mb_total = total_size / (1024 ** 2)
                        print(f"\r  Progress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='')
        
        print(f"\n  ✓ Downloaded: {output_path.name}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"\n  ✗ Error downloading {url}: {e}")
        return False
    except Exception as e:
        print(f"\n  ✗ Unexpected error: {e}")
        return False

def download_station_metadata():
    #Downloading station metadata
    url = f"{BASE_URL}/ghcnd-stations.txt"
    output = DATA_DIR / "raw" / "stations" / "ghcnd-stations.txt"
    
    if output.exists():
        print(f"Station metadata already exists at: {output}")
        print(f"Skipping download...")
        return True
    
    success = download_file(url, output)
    
    if success:
        # Display basic stats
        with open(output, 'r') as f:
            total_stations = sum(1 for _ in f)
        
        # Count US stations
        us_stations = 0
        with open(output, 'r') as f:
            for line in f:
                if line.startswith('US'):
                    us_stations += 1
        
        print(f"\n  Total stations in file: {total_stations:,}")
        print(f"  US stations: {us_stations:,}")
    
    return success

def save_download_log():
    log_file = DATA_DIR / "download_log.txt"
    
    with open(log_file, 'w') as f:
        f.write("NOAA GHCN-Daily Data Download Log\n")
        f.write("=" * 50 + "\n")
        f.write(f"Download Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Source: {BASE_URL}\n")
        f.write(f"Output Directory: {DATA_DIR.absolute()}\n")
        f.write(f"\nNote: Only station metadata downloaded.\n")
        f.write(f"Run select_stations.py next to choose 300 stations.\n")
        f.write(f"Then run download_filtered_data.py to download actual weather data.\n")
    
    print(f"\n✓ Download log saved to {log_file}")

if __name__ == "__main__":
    start_time = time.time()
    
    print("=" * 70)
    print("NOAA GHCN-Daily Station Metadata Download")
    print("=" * 70)
    print(f"This script downloads only the station metadata file.")
    print(f"Weather data will be downloaded later for selected stations only.")
    print(f"\nOutput directory: {DATA_DIR.absolute()}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create directories
    create_directories()
    
    # Download station metadata
    success = download_station_metadata()
    
    if success:
        # Save log
        save_download_log()
        
        elapsed = time.time() - start_time
        print(f"\n{'=' * 70}")
        print(f"✓ Download Complete!")
        print(f"{'=' * 70}")
        print(f"Total time: {elapsed:.1f} seconds")
        print(f"\nNext steps:")
        print(f"  1. Run: python scripts/select_stations.py")
        print(f"  2. Run: python scripts/download_filtered_data.py")
    else:
        print(f"\n✗ Download failed!")
        print(f"Please check your internet connection and try again.")