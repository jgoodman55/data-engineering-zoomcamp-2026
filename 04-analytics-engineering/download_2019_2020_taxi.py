import os
import gzip
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DOWNLOAD_DIR = "./taxi_data"
YEAR = 2019
COLOR = "green"

# Create download directory if it doesn't exist
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

def download_file(month):
    """Download and unzip a taxi data CSV.GZ file"""
    filename = f"{COLOR}_tripdata_{YEAR}-{month}.csv"
    url = f"{BASE_URL}/{COLOR}/{filename}.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        print(f"Downloading: {filename}.gz")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Decompress and write CSV file
        with gzip.GzipFile(fileobj=response.raw) as gz_file:
            with open(file_path, 'wb') as out_file:
                out_file.write(gz_file.read())
        
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
        print(f"✓ Successfully saved: {filename} ({file_size:.2f} MB)")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to download {filename}: {e}")
        return False
    except Exception as e:
        print(f"✗ Error processing {filename}: {e}")
        return False

def main():
    taxi_types = ["yellow", "green"]
    successful = 0
    failed = 0
    
    for taxi_type in taxi_types:
        print(f"\n{'='*50}")
        print(f"Processing {taxi_type.upper()} taxi data...")
        print(f"{'='*50}")

        MONTHS = [f"{i:02d}" for i in range(1, 13)]
        with ThreadPoolExecutor(max_workers=4) as executor:
            file_paths = list(executor.map(download_file, MONTHS))
        
        # for month in range(1, 13):
        #     if download_file(taxi_type, month):
        #         successful += 1
        #     else:
        #         failed += 1
    
    # print(f"\n{'='*50}")
    # print(f"Download complete!")
    # print(f"✓ Successful: {successful}")
    # print(f"✗ Failed: {failed}")
    # print(f"Files saved to: {DOWNLOAD_DIR}")
    # print(f"{'='*50}")

if __name__ == "__main__":
    main()