"""
NYC Taxi Analytics with DuckDB
"""

import duckdb
import os

DB_PATH = "data/warehouse/nyc_taxi.duckdb"

def get_connection():
    os.makedirs("data/warehouse", exist_ok=True)
    return duckdb.connect(DB_PATH)

def setup_database():
    """Create DuckDB database and tables"""
    conn = get_connection()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_trips (
            vendor_id INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            ratecode_id BIGINT,
            store_and_fwd_flag VARCHAR,
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            payment_type BIGINT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            airport_fee DOUBLE
        )
    """)

    print("✅ DuckDB database created at:", DB_PATH)
    conn.close()

def load_parquet_data():
    """Load data from Parquet files into DuckDB"""
    conn = get_connection()

    try:
        print(f"\n📂 Loading data from: data/raw")
        conn.execute("DELETE FROM raw_trips")
        
        conn.execute("""
            INSERT INTO raw_trips
            SELECT *
            FROM read_parquet('data/raw/*.parquet')
            WHERE total_amount > 0 
              AND trip_distance > 0
              AND fare_amount > 0
        """)

        count = conn.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
        print(f"✅ Loaded {count:,} trips into DuckDB")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
    finally:
        conn.close()

def get_summary_stats():
    """Get overall summary statistics"""
    conn = get_connection()

    stats = conn.execute("""
        SELECT
            COUNT(*) as total_trips,
            COALESCE(SUM(total_amount), 0) as total_revenue,
            COALESCE(AVG(trip_distance), 0) as avg_distance,
            COALESCE(AVG(total_amount), 0) as avg_fare,
            MIN(pickup_datetime) as first_trip,
            MAX(pickup_datetime) as last_trip
        FROM raw_trips
    """).fetchone()

    print("\n📊 Overall Statistics:")
    print("=" * 60)
    print(f"  Total Trips:     {stats[0]:,}")
    print(f"  Total Revenue:   ${stats[1]:,.2f}")
    print(f"  Avg Distance:    {stats[2]:.2f} miles")
    print(f"  Avg Fare:        ${stats[3]:.2f}")
    if stats[4] and stats[5]:
        print(f"  Date Range:      {stats[4]} to {stats[5]}")
    print("=" * 60)

    conn.close()

def query_hourly_patterns():
    """Query hourly patterns"""
    conn = get_connection()

    print("\n⏰ Hourly Trip Patterns:")
    print("-" * 60)

    result = conn.execute("""
        SELECT
            EXTRACT(HOUR FROM pickup_datetime) as hour,
            COUNT(*) as trips,
            AVG(total_amount) as avg_fare
        FROM raw_trips
        WHERE pickup_datetime IS NOT NULL
        GROUP BY EXTRACT(HOUR FROM pickup_datetime)
        ORDER BY hour
    """).fetchall()

    if not result:
        print("  No data available")
        conn.close()
        return

    max_trips = max(r[1] for r in result)
    
    for row in result:
        bar = "█" * int((row[1] / max_trips) * 20)
        print(f"  {int(row[0]):02d}:00 | {row[1]:>6,} | ${row[2]:>6.2f} | {bar}")

    conn.close()

def query_payment_types():
    """Query payment type breakdown"""
    conn = get_connection()

    print("\n💳 Payment Type Breakdown:")
    print("-" * 60)

    result = conn.execute("""
        SELECT
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                ELSE 'Unknown'
            END as payment_method,
            COUNT(*) as trips,
            SUM(total_amount) as revenue,
            AVG(tip_amount) as avg_tip
        FROM raw_trips
        GROUP BY payment_type
        ORDER BY trips DESC
    """).fetchall()

    for row in result:
        print(f"  {row[0]:<15} | {row[1]:>8,} trips | ${row[2]:>12,.2f} | ${row[3]:>6.2f} avg tip")

    conn.close()

def run_all():
    """Run complete analytics pipeline"""
    print("\n🚕 NYC Taxi Analytics - DuckDB Pipeline")
    print("=" * 60)

    setup_database()
    load_parquet_data()
    get_summary_stats()
    query_hourly_patterns()
    query_payment_types()
    
    print("\n" + "=" * 60)
    print("✅ Analysis Complete!")
    print("=" * 60)

if __name__ == "__main__":
    run_all()