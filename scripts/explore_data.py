"""
Explore NYC TLC Taxi Data
"""

import pandas as pd
import os

DATA_DIR = "data/raw"

def main():
    print("\n" + "=" * 70)
    print("🔍 NYC TLC Data Explorer")
    print("=" * 70)
    
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]
    
    total_rows = 0
    all_data = []
    
    for f in files:
        path = os.path.join(DATA_DIR, f)
        df = pd.read_parquet(path)
        rows = len(df)
        total_rows += rows
        all_data.append(df)
        print(f"\n📁 {f}")
        print(f"   Rows: {rows:,}")
    
    print("\n" + "=" * 70)
    print(f"📊 TOTAL RECORDS: {total_rows:,}")
    print("=" * 70)
    
    # Combine and show schema
    df = pd.concat(all_data, ignore_index=True)
    
    print("\n📋 COLUMNS:")
    print("-" * 70)
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].notna().sum()
        print(f"   {col:<30} {str(dtype):<15} ({non_null:,} non-null)")
    
    print("\n📈 SAMPLE DATA:")
    print("-" * 70)
    print(df.head(3).to_string())
    
    print("\n💰 QUICK STATS:")
    print("-" * 70)
    print(f"   Date Range: {df['tpep_pickup_datetime'].min()} to {df['tpep_pickup_datetime'].max()}")
    print(f"   Avg Fare: ${df['fare_amount'].mean():.2f}")
    print(f"   Avg Distance: {df['trip_distance'].mean():.2f} miles")
    print(f"   Avg Tip: ${df['tip_amount'].mean():.2f}")
    print(f"   Total Revenue: ${df['total_amount'].sum():,.2f}")
    print("=" * 70)

if __name__ == "__main__":
    main()