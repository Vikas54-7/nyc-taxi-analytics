"""
NYC Taxi Analytics with DuckDB
Lightweight analytics for 8GB RAM systems
"""

import duckdb
import os
from datetime import datetime

DB_PATH = "data/warehouse/nyc_taxi.duckdb"

def get_connection():
    os.makedirs("data/warehouse", exist_ok=True)
    return duckdb.connect(DB_PATH)

def setup_database():
    """Create DuckDB database and tables"""
    conn = get_connection()
    
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
            payment_type INTEGER,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            borough VARCHAR,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hourly_stats (
            hour_start TIMESTAMP,
            borough VARCHAR,
            trip_count INTEGER,
            total_fare DOUBLE,
            total_tips DOUBLE,
            total_distance DOUBLE,
            avg_fare_per_mile DOUBLE
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            trip_date DATE,
            borough VARCHAR,
            total_trips INTEGER,
            total_revenue DOUBLE,
            avg_trip_distance DOUBLE,
            avg_tip_percentage DOUBLE
        )
    """)
    
    print("✅ DuckDB database created at:", DB_PATH)
    conn.close()

def load_parquet_data(parquet_path="data/processed"):
    """Load data from Parquet files into DuckDB"""
    conn = get_connection()
    
    if not os.path.exists(parquet_path):
        print(f"⚠️ Path not found: {parquet_path}")
        return
    
    try:
        conn.execute(f"""
            INSERT INTO raw_trips 
            SELECT 
                trip_id,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                payment_type,
                fare_amount,
                tip_amount,
                total_amount,
                borough,
                CURRENT_TIMESTAMP as ingestion_time
            FROM read_parquet('{parquet_path}/**/*.parquet')
        """)
        
        count = conn.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
        print(f"✅ Loaded {count:,} trips into DuckDB")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
    finally:
        conn.close()

def create_hourly_aggregations():
    """Create hourly aggregations"""
    conn = get_connection()
    
    conn.execute("DELETE FROM hourly_stats")
    
    conn.execute("""
        INSERT INTO hourly_stats
        SELECT 
            DATE_TRUNC('hour', pickup_datetime) as hour_start,
            borough,
            COUNT(*) as trip_count,
            SUM(fare_amount) as total_fare,
            SUM(tip_amount) as total_tips,
            SUM(trip_distance) as total_distance,
            CASE WHEN SUM(trip_distance) > 0 
                 THEN SUM(fare_amount) / SUM(trip_distance) 
                 ELSE 0 END as avg_fare_per_mile
        FROM raw_trips
        WHERE pickup_datetime IS NOT NULL
        GROUP BY DATE_TRUNC('hour', pickup_datetime), borough
    """)
    
    count = conn.execute("SELECT COUNT(*) FROM hourly_stats").fetchone()[0]
    print(f"✅ Created {count:,} hourly aggregation records")
    conn.close()

def create_daily_summary():
    """Create daily summary"""
    conn = get_connection()
    
    conn.execute("DELETE FROM daily_summary")
    
    conn.execute("""
        INSERT INTO daily_summary
        SELECT 
            CAST(pickup_datetime AS DATE) as trip_date,
            borough,
            COUNT(*) as total_trips,
            SUM(total_amount) as total_revenue,
            AVG(trip_distance) as avg_trip_distance,
            AVG(CASE WHEN fare_amount > 0 
                     THEN (tip_amount / fare_amount) * 100 
                     ELSE 0 END) as avg_tip_percentage
        FROM raw_trips
        WHERE pickup_datetime IS NOT NULL
        GROUP BY CAST(pickup_datetime AS DATE), borough
    """)
    
    count = conn.execute("SELECT COUNT(*) FROM daily_summary").fetchone()[0]
    print(f"✅ Created {count:,} daily summary records")
    conn.close()

def query_top_boroughs():
    """Query top boroughs by revenue"""
    conn = get_connection()
    
    print("\n📊 Top Boroughs by Revenue:")
    print("-" * 60)
    
    result = conn.execute("""
        SELECT 
            COALESCE(borough, 'Unknown') as borough,
            COUNT(*) as trips,
            SUM(total_amount) as revenue,
            AVG(trip_distance) as avg_distance,
            AVG(CASE WHEN fare_amount > 0 
                     THEN (tip_amount / fare_amount) * 100 
                     ELSE 0 END) as avg_tip_pct
        FROM raw_trips
        GROUP BY borough
        ORDER BY revenue DESC
        LIMIT 10
    """).fetchall()
    
    for row in result:
        print(f"  {row[0]:<15} | {row[1]:>8,} trips | ${row[2]:>12,.2f} | {row[4]:>5.1f}% tip")
    
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
    
    for row in result:
        bar = "█" * int(row[1] / max(r[1] for r in result) * 20)
        print(f"  {int(row[0]):02d}:00 | {row[1]:>6,} | ${row[2]:>6.2f} | {bar}")
    
    conn.close()

def get_summary_stats():
    """Get overall summary statistics"""
    conn = get_connection()
    
    stats = conn.execute("""
        SELECT
            COUNT(*) as total_trips,
            SUM(total_amount) as total_revenue,
            AVG(trip_distance) as avg_distance,
            AVG(total_amount) as avg_fare,
            MIN(pickup_datetime) as first_trip,
            MAX(pickup_datetime) as last_trip
        FROM raw_trips
    """).fetchone()
    
    print("\n📈 Overall Statistics:")
    print("=" * 60)
    print(f"  Total Trips:     {stats[0]:,}")
    print(f"  Total Revenue:   ${stats[1]:,.2f}" if stats[1] else "  Total Revenue:   N/A")
    print(f"  Avg Distance:    {stats[2]:.2f} miles" if stats[2] else "  Avg Distance:    N/A")
    print(f"  Avg Fare:        ${stats[3]:.2f}" if stats[3] else "  Avg Fare:        N/A")
    print(f"  Date Range:      {stats[4]} to {stats[5]}" if stats[4] else "  Date Range:      N/A")
    print("=" * 60)
    
    conn.close()

def run_all():
    """Run complete analytics pipeline"""
    print("\n🚕 NYC Taxi Analytics - DuckDB Pipeline")
    print("=" * 60)
    
    setup_database()
    load_parquet_data()
    create_hourly_aggregations()
    create_daily_summary()
    get_summary_stats()
    query_top_boroughs()
    query_hourly_patterns()

if __name__ == "__main__":
    run_all()