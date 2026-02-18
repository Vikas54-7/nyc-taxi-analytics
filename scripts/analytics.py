"""
NYC Taxi Analytics - Query DuckDB Data
"""

import duckdb

DB_PATH = 'data/warehouse/nyc_taxi.duckdb'

def main():
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    print("\n" + "=" * 70)
    print("📊 NYC TAXI ANALYTICS REPORT")
    print("=" * 70)
    
    # Total Records
    total = conn.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
    print(f"\n📈 Total Trips: {total:,}")
    
    # Revenue Stats
    stats = conn.execute("""
        SELECT 
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_fare,
            AVG(trip_distance) as avg_distance,
            AVG(tip_amount) as avg_tip
        FROM raw_trips
    """).fetchone()
    
    print(f"\n💰 Revenue Statistics:")
    print(f"   Total Revenue:  ${stats[0]:,.2f}")
    print(f"   Average Fare:   ${stats[1]:.2f}")
    print(f"   Average Distance: {stats[2]:.2f} miles")
    print(f"   Average Tip:    ${stats[3]:.2f}")
    
    # Trips by Payment Type
    print(f"\n💳 Trips by Payment Type:")
    payment_types = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown'}
    results = conn.execute("""
        SELECT payment_type, COUNT(*) as trips, SUM(total_amount) as revenue
        FROM raw_trips
        GROUP BY payment_type
        ORDER BY trips DESC
    """).fetchall()
    
    for row in results:
        ptype = payment_types.get(row[0], f'Type {row[0]}')
        print(f"   {ptype:<15} | {row[1]:>6,} trips | ${row[2]:>10,.2f}")
    
    # Top Pickup Locations
    print(f"\n📍 Top 5 Pickup Locations:")
    results = conn.execute("""
        SELECT pickup_location_id, COUNT(*) as trips
        FROM raw_trips
        GROUP BY pickup_location_id
        ORDER BY trips DESC
        LIMIT 5
    """).fetchall()
    
    for i, row in enumerate(results, 1):
        print(f"   {i}. Location {row[0]}: {row[1]:,} trips")
    
    # Hourly Pattern (extract hour from datetime string)
    print(f"\n⏰ Trips by Hour:")
    results = conn.execute("""
        SELECT 
            CAST(SUBSTR(pickup_datetime, 12, 2) AS INTEGER) as hour,
            COUNT(*) as trips
        FROM raw_trips
        WHERE pickup_datetime IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """).fetchall()
    
    for row in results:
        bar = "█" * (row[1] // 100)
        print(f"   {row[0]:02d}:00 | {row[1]:>5,} | {bar}")
    
    print("\n" + "=" * 70)
    conn.close()

if __name__ == "__main__":
    main()