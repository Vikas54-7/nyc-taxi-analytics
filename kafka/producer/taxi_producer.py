"""
NYC Taxi Kafka Producer
Streams real NYC TLC data to Kafka
"""

import json
import time
import os
from datetime import datetime
from confluent_kafka import Producer
import pandas as pd
from tqdm import tqdm

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'taxi-trips'
DATA_DIR = 'data/raw'
BATCH_SIZE = 1000
DELAY_BETWEEN_BATCHES = 0.1  # seconds

def delivery_callback(err, msg):
    """Callback for message delivery"""
    if err:
        print(f'❌ Message delivery failed: {err}')

def create_producer():
    """Create Kafka producer"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'taxi-producer',
        'acks': 'all',
        'batch.size': 16384,
        'linger.ms': 10,
    }
    return Producer(conf)

def transform_row(row):
    """Transform DataFrame row to JSON message"""
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
        'tolls_amount': float(row['tolls_amount']),
        'total_amount': float(row['total_amount']),
        'congestion_surcharge': float(row['congestion_surcharge']) if pd.notna(row['congestion_surcharge']) else 0.0,
        'airport_fee': float(row['Airport_fee']) if pd.notna(row['Airport_fee']) else 0.0,
        'ingestion_timestamp': datetime.now().isoformat()
    }

def main():
    print("\n" + "=" * 70)
    print("🚕 NYC Taxi Kafka Producer")
    print("=" * 70)
    print(f"📍 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📋 Topic: {KAFKA_TOPIC}")
    print(f"📦 Batch Size: {BATCH_SIZE}")
    print("=" * 70)
    
    # Create producer
    producer = create_producer()
    print("✅ Kafka producer connected")
    
    # Load data files
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')])
    
    total_sent = 0
    total_errors = 0
    start_time = time.time()
    
    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        print(f"\n📁 Processing: {filename}")
        
        # Read parquet file
        df = pd.read_parquet(filepath)
        total_rows = len(df)
        
        # Process in batches with progress bar
        with tqdm(total=total_rows, desc="Sending", unit="msg") as pbar:
            for i in range(0, total_rows, BATCH_SIZE):
                batch = df.iloc[i:i+BATCH_SIZE]
                
                for _, row in batch.iterrows():
                    try:
                        message = transform_row(row)
                        producer.produce(
                            KAFKA_TOPIC,
                            key=message['trip_id'],
                            value=json.dumps(message),
                            callback=delivery_callback
                        )
                        total_sent += 1
                    except Exception as e:
                        total_errors += 1
                
                # Flush batch
                producer.poll(0)
                pbar.update(len(batch))
                
                # Small delay to control throughput
                time.sleep(DELAY_BETWEEN_BATCHES)
        
        # Flush remaining messages
        producer.flush()
    
    # Summary
    elapsed = time.time() - start_time
    rate = total_sent / elapsed if elapsed > 0 else 0
    
    print("\n" + "=" * 70)
    print("📊 Producer Summary")
    print("=" * 70)
    print(f"   ✅ Messages Sent: {total_sent:,}")
    print(f"   ❌ Errors: {total_errors:,}")
    print(f"   ⏱️  Duration: {elapsed:.1f} seconds")
    print(f"   🚀 Throughput: {rate:,.0f} msg/sec")
    print("=" * 70)

if __name__ == "__main__":
    main()