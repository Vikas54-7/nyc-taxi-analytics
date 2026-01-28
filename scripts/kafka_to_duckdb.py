"""
Simple Kafka to DuckDB Consumer
No Spark required - works on 8GB RAM
"""

from confluent_kafka import Consumer, KafkaError
import duckdb
import json
import os
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'taxi-trips'
KAFKA_GROUP_ID = 'duckdb-consumer-v2'
DB_PATH = 'data/warehouse/nyc_taxi.duckdb'

def setup_database():
    """Create DuckDB tables if not exist"""
    os.makedirs('data/warehouse', exist_ok=True)
    conn = duckdb.connect(DB_PATH)
    
    # Drop old table and recreate with correct types
    conn.execute("DROP TABLE IF EXISTS raw_trips")
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_trips (
            trip_id VARCHAR,
            vendor_id VARCHAR,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            payment_type VARCHAR,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            borough VARCHAR,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.close()
    print("✅ Database ready")

def create_consumer():
    """Create Kafka consumer"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    print(f"✅ Subscribed to topic: {KAFKA_TOPIC}")
    return consumer

def insert_trip(conn, trip):
    """Insert a trip into DuckDB"""
    try:
        conn.execute("""
            INSERT INTO raw_trips (
                trip_id, vendor_id, pickup_datetime, dropoff_datetime,
                passenger_count, trip_distance, pickup_location_id, 
                dropoff_location_id, payment_type, fare_amount,
                tip_amount, total_amount, borough, ingestion_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(trip.get('trip_id', '')),
            str(trip.get('vendor_id', '')),
            trip.get('pickup_datetime'),
            trip.get('dropoff_datetime'),
            trip.get('passenger_count'),
            trip.get('trip_distance'),
            trip.get('pickup_location_id'),
            trip.get('dropoff_location_id'),
            str(trip.get('payment_type', '')),
            trip.get('fare_amount'),
            trip.get('tip_amount'),
            trip.get('total_amount'),
            str(trip.get('borough', '')),
            datetime.now()
        ])
        return True
    except Exception as e:
        print(f"❌ Insert error: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("🚕 NYC TAXI - Kafka to DuckDB Consumer")
    print("=" * 60)
    
    setup_database()
    consumer = create_consumer()
    conn = duckdb.connect(DB_PATH)
    
    message_count = 0
    batch_size = 100
    
    print(f"📡 Listening for messages... (Ctrl+C to stop)\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Error: {msg.error()}")
                    break
            
            try:
                trip = json.loads(msg.value().decode('utf-8'))
                if insert_trip(conn, trip):
                    message_count += 1
                    
                    if message_count % batch_size == 0:
                        print(f"  📊 Processed {message_count} messages...")
                        
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON error: {e}")
                continue
                
    except KeyboardInterrupt:
        print(f"\n\n🛑 Stopping consumer...")
        
    finally:
        consumer.close()
        conn.close()
        
        print(f"\n✅ Total messages processed: {message_count}")
        print(f"💾 Data saved to: {DB_PATH}")

if __name__ == "__main__":
    main()