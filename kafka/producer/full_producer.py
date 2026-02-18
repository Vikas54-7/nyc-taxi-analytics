"""
NYC Taxi Kafka Producer - Full Dataset
Sends 100K trips for demo
"""

import json
import time
from datetime import datetime
from confluent_kafka import Producer
import pandas as pd
from tqdm import tqdm

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'taxi-trips'
TOTAL_TRIPS = 100000  # 100K trips

def create_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'taxi-producer-full',
        'batch.size': 32768,
        'linger.ms': 10,
    })

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
    print("\n" + "=" * 60)
    print("🚕 NYC Taxi Producer - 100K Trips")
    print("=" * 60)
    
    producer = create_producer()
    df = pd.read_parquet('data/raw/yellow_tripdata_2024-01.parquet')
    df = df.head(TOTAL_TRIPS)
    
    print(f"📤 Sending {TOTAL_TRIPS:,} messages to Kafka...")
    
    sent = 0
    with tqdm(total=TOTAL_TRIPS, desc="Sending", unit="msg") as pbar:
        for _, row in df.iterrows():
            try:
                msg = transform_row(row)
                producer.produce(KAFKA_TOPIC, value=json.dumps(msg))
                sent += 1
                if sent % 5000 == 0:
                    producer.flush()
                pbar.update(1)
            except Exception as e:
                pass
    
    producer.flush()
    
    print(f"\n✅ Sent: {sent:,} messages")
    print("🔍 Now run the consumer to process them!")

if __name__ == "__main__":
    main()