"""
Kafka to DuckDB Consumer
Reads taxi trips from Kafka and stores in DuckDB warehouse
"""

import json
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import duckdb

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'taxi-trips'
KAFKA_GROUP_ID = 'duckdb-consumer-v3'
DB_PATH = 'data/warehouse/nyc_taxi.duckdb'

def setup_database():
    """Create DuckDB tables"""
    os.makedirs('data/warehouse', exist_ok=True)
    conn = duckdb.connect(DB_PATH)
    
    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS raw_trips")
    
    conn.execute("""
        CREATE TABLE raw_trips (
            trip_id VARCHAR,
            vendor_id INTEGER,
            pickup_datetime VARCHAR,
            dropoff_datetime VARCHAR,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            ingestion_timestamp TIMESTAMP
        )
    """)
    
    conn.close()
    print("✅ DuckDB database ready")

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
    print(f"✅ Subscribed to: {KAFKA_TOPIC}")
    return consumer

def insert_trip(conn, trip):
    """Insert trip into DuckDB"""
    try:
        conn.execute("""
            INSERT INTO raw_trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(trip.get('trip_id', '')),
            trip.get('vendor_id'),
            str(trip.get('pickup_datetime', '')),
            str(trip.get('dropoff_datetime', '')),
            trip.get('passenger_count'),
            trip.get('trip_distance'),
            trip.get('pickup_location_id'),
            trip.get('dropoff_location_id'),
            trip.get('payment_type'),
            trip.get('fare_amount'),
            trip.get('tip_amount'),
            trip.get('total_amount'),
            datetime.now()
        ])
        return True
    except Exception as e:
        print(f"   ❌ DB Error: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("🚕 NYC Taxi - Kafka to DuckDB Consumer")
    print("=" * 60)
    print(f"📍 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📋 Topic: {KAFKA_TOPIC}")
    print(f"💾 Database: {DB_PATH}")
    print("=" * 60)
    
    setup_database()
    consumer = create_consumer()
    conn = duckdb.connect(DB_PATH)
    
    message_count = 0
    error_count = 0
    start_time = datetime.now()
    
    print(f"\n📡 Consuming messages... (Ctrl+C to stop)\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    break
            
            try:
                trip = json.loads(msg.value().decode('utf-8'))
                success = insert_trip(conn, trip)
                if success:
                    message_count += 1
                    if message_count % 1000 == 0:
                        print(f"   ✅ Processed: {message_count:,} messages")
                else:
                    error_count += 1
            except json.JSONDecodeError as e:
                error_count += 1
                if error_count <= 3:
                    print(f"   ❌ JSON error: {e}")
            except Exception as e:
                error_count += 1
                if error_count <= 3:
                    print(f"   ❌ Error: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = message_count / elapsed if elapsed > 0 else 0
        
        print("\n" + "=" * 60)
        print("📊 Consumer Summary")
        print("=" * 60)
        print(f"   ✅ Messages Processed: {message_count:,}")
        print(f"   ❌ Errors: {error_count:,}")
        print(f"   ⏱️  Duration: {elapsed:.1f} seconds")
        print(f"   🚀 Rate: {rate:,.0f} msg/sec")
        print(f"   💾 Data saved to: {DB_PATH}")
        print("=" * 60)

if __name__ == "__main__":
    main()