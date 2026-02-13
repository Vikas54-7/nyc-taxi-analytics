"""
NYC Taxi Analytics Dashboard
"""

from flask import Flask, jsonify
import duckdb

app = Flask(__name__)
DB_PATH = 'data/warehouse/nyc_taxi.duckdb'

HTML = '''<!DOCTYPE html>
<html>
<head>
    <title>NYC Taxi Analytics</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; }
        h1 { color: #f39c12; text-align: center; margin-bottom: 30px; }
        .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 30px; }
        .card { background: #16213e; padding: 20px; border-radius: 10px; text-align: center; }
        .card h3 { color: #888; font-size: 12px; }
        .card .value { font-size: 28px; color: #f39c12; font-weight: bold; }
        .charts { display: grid; grid-template-columns: 2fr 1fr; gap: 20px; }
        .chart-box { background: #16213e; padding: 20px; border-radius: 10px; }
        .chart-box h3 { color: #f39c12; margin-bottom: 10px; }
    </style>
</head>
<body>
    <h1>🚕 NYC Taxi Analytics Dashboard</h1>
    <div class="grid">
        <div class="card"><h3>TOTAL TRIPS</h3><div class="value" id="trips">-</div></div>
        <div class="card"><h3>TOTAL REVENUE</h3><div class="value" id="revenue">-</div></div>
        <div class="card"><h3>AVG FARE</h3><div class="value" id="fare">-</div></div>
        <div class="card"><h3>AVG DISTANCE</h3><div class="value" id="distance">-</div></div>
    </div>
    <div class="charts">
        <div class="chart-box"><h3>Trips by Hour</h3><canvas id="hourlyChart"></canvas></div>
        <div class="chart-box"><h3>Payment Types</h3><canvas id="paymentChart"></canvas></div>
    </div>
    <script>
        fetch('/api/stats').then(r => r.json()).then(data => {
            document.getElementById('trips').textContent = data.total_trips.toLocaleString();
            document.getElementById('revenue').textContent = '$' + data.total_revenue.toLocaleString();
            document.getElementById('fare').textContent = '$' + data.avg_fare;
            document.getElementById('distance').textContent = data.avg_distance + ' mi';
        });
        fetch('/api/hourly').then(r => r.json()).then(data => {
            new Chart(document.getElementById('hourlyChart'), {
                type: 'bar',
                data: { labels: data.labels, datasets: [{ data: data.values, backgroundColor: '#f39c12' }] },
                options: { plugins: { legend: { display: false }}, scales: { y: { beginAtZero: true }}}
            });
        });
        fetch('/api/payments').then(r => r.json()).then(data => {
            new Chart(document.getElementById('paymentChart'), {
                type: 'doughnut',
                data: { labels: data.labels, datasets: [{ data: data.values, backgroundColor: ['#f39c12','#3498db','#e74c3c','#2ecc71'] }] }
            });
        });
    </script>
</body>
</html>'''

@app.route('/')
def home():
    return HTML

@app.route('/api/stats')
def stats():
    conn = duckdb.connect(DB_PATH, read_only=True)
    r = conn.execute("SELECT COUNT(*), SUM(total_amount), AVG(total_amount), AVG(trip_distance) FROM raw_trips").fetchone()
    conn.close()
    return jsonify({'total_trips': r[0], 'total_revenue': round(r[1],2), 'avg_fare': round(r[2],2), 'avg_distance': round(r[3],2)})

@app.route('/api/hourly')
def hourly():
    conn = duckdb.connect(DB_PATH, read_only=True)
    r = conn.execute("SELECT CAST(SUBSTR(pickup_datetime,12,2) AS INT) as h, COUNT(*) FROM raw_trips GROUP BY h ORDER BY h").fetchall()
    conn.close()
    return jsonify({'labels': [f"{x[0]:02d}:00" for x in r], 'values': [x[1] for x in r]})

@app.route('/api/payments')
def payments():
    conn = duckdb.connect(DB_PATH, read_only=True)
    r = conn.execute("SELECT payment_type, COUNT(*) FROM raw_trips GROUP BY payment_type ORDER BY COUNT(*) DESC").fetchall()
    conn.close()
    names = {1:'Credit Card', 2:'Cash', 3:'No Charge', 4:'Dispute'}
    return jsonify({'labels': [names.get(x[0], f'Type {x[0]}') for x in r], 'values': [x[1] for x in r]})

if __name__ == '__main__':
    print("\n🚕 Dashboard: http://127.0.0.1:5555\n")
    app.run(host='127.0.0.1', port=5555)