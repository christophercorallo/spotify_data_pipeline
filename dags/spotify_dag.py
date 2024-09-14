from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from write_data_to_postgres import write_data_to_postgres

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 8, 15, 00, 00)
}


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='0 */3 * * *',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='write_data_to_db',
        python_callable=write_data_to_postgres
    )