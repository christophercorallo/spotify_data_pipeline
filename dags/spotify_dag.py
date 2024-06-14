from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}

with DAG(
    default_args = default_args,
    dag_id = 'spotify_python_dag',
    description = 'First spotify dag using python',
    start_date = datetime(2024,6,12),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'spotify_etl',
        python_callable = run_spotify_etl
    )

    task1