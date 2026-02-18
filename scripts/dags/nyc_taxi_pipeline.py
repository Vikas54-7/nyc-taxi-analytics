from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_data_quality(**context):
    print("Running data quality checks...")
    return True

def create_aggregations(**context):
    print("Creating aggregations...")
    return True

with DAG(
    dag_id='nyc_taxi_daily_pipeline',
    default_args=default_args,
    description='Daily NYC Taxi data processing',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc-taxi'],
) as dag:
    start = EmptyOperator(task_id='start')
    check_quality = PythonOperator(task_id='data_quality_checks', python_callable=check_data_quality)
    aggregations = PythonOperator(task_id='create_aggregations', python_callable=create_aggregations)
    end = EmptyOperator(task_id='end')
    
    start >> check_quality >> aggregations >> end