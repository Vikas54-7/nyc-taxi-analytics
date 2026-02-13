"""
NYC Taxi Analytics Pipeline DAG
Orchestrates the complete data pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def check_kafka_connection():
    """Verify Kafka is accessible"""
    from confluent_kafka import Producer
    try:
        p = Producer({'bootstrap.servers': 'localhost:9092'})
        p.flush(timeout=5)
        print("✅ Kafka connection successful")
        return True
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        raise

def run_producer():
    """Run Kafka producer to send taxi data"""
    import subprocess
    result = subprocess.run(
        ['python', 'kafka/producer/test_producer.py'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Producer failed: {result.stderr}")

def run_consumer():
    """Run Kafka consumer to process data"""
    import subprocess
    result = subprocess.run(
        ['python', 'kafka/consumer/kafka_to_duckdb.py'],
        capture_output=True,
        text=True,
        timeout=300  # 5 min timeout
    )
    print(result.stdout)

def run_analytics():
    """Generate analytics report"""
    import subprocess
    result = subprocess.run(
        ['python', 'scripts/analytics.py'],
        capture_output=True,
        text=True
    )
    print(result.stdout)

def data_quality_check():
    """Run data quality checks"""
    import duckdb
    conn = duckdb.connect('data/warehouse/nyc_taxi.duckdb', read_only=True)
    
    # Check record count
    count = conn.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
    print(f"📊 Total records: {count:,}")
    
    # Check for nulls in critical fields
    nulls = conn.execute("""
        SELECT 
            SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) as null_trip_id,
            SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as null_amount,
            SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) as null_datetime
        FROM raw_trips
    """).fetchone()
    
    print(f"🔍 Null trip_ids: {nulls[0]}")
    print(f"🔍 Null amounts: {nulls[1]}")
    print(f"🔍 Null datetimes: {nulls[2]}")
    
    # Check for negative fares
    negative = conn.execute(
        "SELECT COUNT(*) FROM raw_trips WHERE fare_amount < 0"
    ).fetchone()[0]
    print(f"⚠️ Negative fares: {negative}")
    
    conn.close()
    
    # Quality score
    quality_score = ((count - nulls[0] - nulls[1] - negative) / count) * 100 if count > 0 else 0
    print(f"\n✅ Data Quality Score: {quality_score:.2f}%")
    
    if quality_score < 95:
        raise Exception(f"Data quality below threshold: {quality_score:.2f}%")

with DAG(
    dag_id='nyc_taxi_daily_pipeline',
    default_args=default_args,
    description='NYC Taxi Analytics Daily Pipeline',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'production'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    check_kafka = PythonOperator(
        task_id='check_kafka_connection',
        python_callable=check_kafka_connection,
    )
    
    produce_data = PythonOperator(
        task_id='produce_taxi_data',
        python_callable=run_producer,
    )
    
    consume_data = PythonOperator(
        task_id='consume_to_warehouse',
        python_callable=run_consumer,
    )
    
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )
    
    generate_report = PythonOperator(
        task_id='generate_analytics',
        python_callable=run_analytics,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> check_kafka >> produce_data >> consume_data >> quality_check >> generate_report >> end