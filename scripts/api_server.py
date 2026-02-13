"""
Simple Flask API for Grafana to query DuckDB data
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import duckdb
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

DB_PATH = 'data/warehouse/nyc_taxi.duckdb'

def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """Get overall summary statistics"""
    conn = get_connection()
    result = conn.execute("""
        SELECT
            COUNT(*) as total_trips,
            COALESCE(SUM(total_amount), 0) as total_revenue,
            COALESCE(AVG(trip_distance), 0) as avg_distance,
            COALESCE(AVG(total_amount), 0) as avg_fare,
            COALESCE(AVG(tip_amount), 0) as avg_tip
        FROM raw_trips
    """).fetchone()
    conn.close()
    
    return jsonify({
        "total_trips": result[0],
        "total_revenue": round(result[1], 2),
        "avg_distance": round(result[2], 2),
        "avg_fare": round(result[3], 2),
        "avg_tip": round(result[4], 2)
    })

@app.route('/api/hourly', methods=['GET'])
def get_hourly():
    """Get hourly trip patterns"""
    conn = get_connection()
    results = conn.execute("""
        SELECT 
            EXTRACT(HOUR FROM pickup_datetime) as hour,
            COUNT(*) as trips,
            COALESCE(SUM(total_amount), 0) as revenue,
            COALESCE(AVG(total_amount), 0) as avg_fare
        FROM raw_trips
        WHERE pickup_datetime IS NOT NULL
        GROUP BY EXTRACT(HOUR FROM pickup_datetime)
        ORDER BY hour
    """).fetchall()
    conn.close()
    
    return jsonify([{
        "hour": int(r[0]),
        "trips": r[1],
        "revenue": round(r[2], 2),
        "avg_fare": round(r[3], 2)
    } for r in results])

@app.route('/api/by-borough', methods=['GET'])
def get_by_borough():
    """Get stats by borough"""
    conn = get_connection()
    results = conn.execute("""
        SELECT 
            COALESCE(borough, 'Unknown') as borough,
            COUNT(*) as trips,
            COALESCE(SUM(total_amount), 0) as revenue,
            COALESCE(AVG(trip_distance), 0) as avg_distance
        FROM raw_trips
        GROUP BY borough
        ORDER BY revenue DESC
    """).fetchall()
    conn.close()
    
    return jsonify([{
        "borough": r[0],
        "trips": r[1],
        "revenue": round(r[2], 2),
        "avg_distance": round(r[3], 2)
    } for r in results])

@app.route('/api/by-payment', methods=['GET'])
def get_by_payment():
    """Get stats by payment type"""
    conn = get_connection()
    results = conn.execute("""
        SELECT 
            COALESCE(payment_type, 'Unknown') as payment_type,
            COUNT(*) as trips,
            COALESCE(SUM(total_amount), 0) as revenue
        FROM raw_trips
        GROUP BY payment_type
        ORDER BY trips DESC
    """).fetchall()
    conn.close()
    
    return jsonify([{
        "payment_type": r[0],
        "trips": r[1],
        "revenue": round(r[2], 2)
    } for r in results])

@app.route('/api/timeseries', methods=['GET'])
def get_timeseries():
    """Get time series data for charts"""
    conn = get_connection()
    results = conn.execute("""
        SELECT 
            DATE_TRUNC('hour', pickup_datetime) as time,
            COUNT(*) as trips,
            COALESCE(SUM(total_amount), 0) as revenue
        FROM raw_trips
        WHERE pickup_datetime IS NOT NULL
        GROUP BY DATE_TRUNC('hour', pickup_datetime)
        ORDER BY time
    """).fetchall()
    conn.close()
    
    return jsonify([{
        "time": r[0].isoformat() if r[0] else None,
        "trips": r[1],
        "revenue": round(r[2], 2)
    } for r in results])

@app.route('/api/recent', methods=['GET'])
def get_recent():
    """Get recent trips"""
    limit = request.args.get('limit', 10, type=int)
    conn = get_connection()
    results = conn.execute(f"""
        SELECT 
            trip_id,
            pickup_datetime,
            borough,
            trip_distance,
            total_amount,
            payment_type
        FROM raw_trips
        ORDER BY ingestion_time DESC
        LIMIT {limit}
    """).fetchall()
    conn.close()
    
    return jsonify([{
        "trip_id": r[0],
        "pickup_datetime": r[1].isoformat() if r[1] else None,
        "borough": r[2],
        "trip_distance": round(r[3], 2) if r[3] else 0,
        "total_amount": round(r[4], 2) if r[4] else 0,
        "payment_type": r[5]
    } for r in results])

if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("🚕 NYC Taxi Analytics API Server")
    print("=" * 60)
    print("📍 Running on: http://localhost:5000")
    print("📊 Endpoints:")
    print("   GET /api/health     - Health check")
    print("   GET /api/summary    - Overall statistics")
    print("   GET /api/hourly     - Hourly patterns")
    print("   GET /api/by-borough - Stats by borough")
    print("   GET /api/by-payment - Stats by payment type")
    print("   GET /api/timeseries - Time series data")
    print("   GET /api/recent     - Recent trips")
    print("=" * 60 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)