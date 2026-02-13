"""
Test Kafka Producer - Small batch to verify connection
"""

import json
import time
from datetime import datetime
from confluent_kafka import Producer
import pandas as pd

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'taxi-trips'
DATA_DIR = 'data/raw'
TEST_ROWS = 10000  # Only send 10K messages for testing

def create_producer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'taxi-producer-test',
    }
    return Producer(conf)

def transform_row(row):
    return {
        'trip_id': f"{row['VendorID']}_{row['tpep_pickup_datetime'].timestamp()}_{row['PULocationID']}",
        'vendor_id': int(row['VendorID']),
        'pickup_datetime': row['tpep_pickup_datetime'].isoformat(),
        'dropoff_datetime': row['tpep_dropoff_datetime'].isoformat(),
        'passenger_count': int(row['passenger_count']) if pd.notna(row['passenger_count']) else 1,
        'trip_distance': float(row['trip_distance']),
        'pickup_location_id': int(row['PULocationID']),
        'dropoff_location_id': int(row['DOLocationID']),
        'payment_type': int(row['payment_type']),
        'fare_amount': float(row['fare_amount']),
        'tip_amount': float(row['tip_amount']),
        'total_amount': float(row['total_amount']),
    }

def main():
    print("\n" + "=" * 50)
    print("🧪 Kafka Test Producer")
    print("=" * 50)
    
    producer = create_producer()
    print("✅ Connected to Kafka")
    
    # Load first file, take TEST_ROWS
    df = pd.read_parquet('data/raw/yellow_tripdata_2024-01.parquet')
    df = df.head(TEST_ROWS)
    
    print(f"📤 Sending {TEST_ROWS:,} messages...")
    
    sent = 0
    for _, row in df.iterrows():
        try:
            message = transform_row(row)
            producer.produce(
                KAFKA_TOPIC,
                key=message['trip_id'],
                value=json.dumps(message)
            )
            sent += 1
            if sent % 1000 == 0:
                producer.flush()
                print(f"   Sent: {sent:,}")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    producer.flush()
    
    print("\n" + "=" * 50)
    print(f"✅ Total Sent: {sent:,}")
    print("🔍 Check Kafka UI: http://localhost:8080")
    print("=" * 50)

if __name__ == "__main__":
    main()