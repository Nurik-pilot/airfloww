from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Функции для задач
def start():
    print("Pipeline started")

def fetch_raw_data():
    time.sleep(2)
    print("Raw data fetched")

def process_data():
    print("Data processed")

def finish():
    print("Pipeline finished")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='simple_pipeline_dag',
    default_args=default_args,
    description='Простой DAG с четырьмя задачами',
    schedule_interval='@hourly',
    start_date=datetime(2025, 11, 5),
    catchup=False,
    tags=['example'],
) as dag:

    start_task = PythonOperator(
        task_id='start',
        python_callable=start,
    )

    fetch_raw_data_task = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=fetch_raw_data,
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    finish_task = PythonOperator(
        task_id='finish',
        python_callable=finish,
    )

    # Задание зависимостей
    start_task >> fetch_raw_data_task >> process_data_task >> finish_task