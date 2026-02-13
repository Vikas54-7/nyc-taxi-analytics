"""
Download Real NYC TLC Taxi Data
Official source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

import os
import requests
from tqdm import tqdm

# NYC TLC Data URLs (Yellow Taxi - Parquet format)
# We'll download 3 months of data (~15M trips)
DATA_URLS = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
]

OUTPUT_DIR = "data/raw"

def download_file(url, output_path):
    """Download file with progress bar"""
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(output_path, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(output_path)) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))

def main():
    print("\n" + "=" * 60)
    print("📥 NYC TLC Data Downloader")
    print("=" * 60)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for url in DATA_URLS:
        filename = url.split("/")[-1]
        output_path = os.path.join(OUTPUT_DIR, filename)
        
        if os.path.exists(output_path):
            print(f"✅ Already exists: {filename}")
            continue
        
        print(f"\n📥 Downloading: {filename}")
        try:
            download_file(url, output_path)
            print(f"✅ Saved: {output_path}")
        except Exception as e:
            print(f"❌ Error downloading {filename}: {e}")
    
    # Show summary
    print("\n" + "=" * 60)
    print("📊 Download Summary")
    print("=" * 60)
    
    total_size = 0
    for f in os.listdir(OUTPUT_DIR):
        if f.endswith('.parquet'):
            size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
            total_size += size
            print(f"  {f}: {size / (1024*1024):.1f} MB")
    
    print(f"\n  Total: {total_size / (1024*1024):.1f} MB")
    print("=" * 60)

if __name__ == "__main__":
    main()