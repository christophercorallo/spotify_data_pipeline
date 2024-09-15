from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from write_data_to_postgres import connect_to_spotify, load_songs_to_df, connect_to_postgres, write_data_to_postgres

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 15, 00, 00)
}
dag = DAG(
    dag_id='python_operator', default_args=default_args,
    schedule_interval='0 */3 * * *')

songs = PythonOperator(
    task_id='connect to spotify',
    python_callable=connect_to_spotify(),
    op_kwargs = {'scope': 'user-read-recently-played'},
    dag=dag)

songs_df = PythonOperator(
    task_id='load songs from spotify',
    python_callable=load_songs_to_df(),
    op_kwargs = {'songs': songs},
    dag=dag)

conn = PythonOperator(
    task_id='connect to postgres',
    python_callable=connect_to_postgres(),
    dag=dag)

write_data = PythonOperator(
    task_id='write data to postgres',
    python_callable=write_data_to_postgres(),
    op_kwargs = {'con': conn, 'song_df': songs_df},
    dag=dag)


songs >> songs_df >> conn >> write_data