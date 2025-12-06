from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import requests
import json

JSON_URL = "https://jsonplaceholder.typicode.com/comments"
FILE_PATH = "/opt/airflow/dags/files/comments.json"

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="comments_json_to_postgres",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 3),
    catchup=False,
    default_args=default_args,
    tags=["ETL", "PostgreSQL", "JSON"]
) as dag:

    @task
    def download_json():
        os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)
        response = requests.get(JSON_URL)
        with open(FILE_PATH, "w", encoding="utf-8") as f:
            f.write(response.text)

    @task.sensor(poke_interval=10, mode="reschedule")
    def wait_for_file():
        return os.path.exists(FILE_PATH)

    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        create_sql = """
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER,
            name TEXT,
            email TEXT,
            body TEXT
        );
        """
        hook.run(create_sql)

    @task(multiple_outputs=True)
    def parse_json():
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "count": len(data),
            "source": JSON_URL,
            "data": data
        }

    @task
    def load_to_postgres(data: list, count: int):
        if count == 0:
            return
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        for item in data:
            cursor.execute("""
                INSERT INTO comments (id, post_id, name, email, body)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (item["id"], item["postId"], item["name"], item["email"], item["body"]))
        conn.commit()
        cursor.close()

    # DAG dependencies
    json_file = download_json()
    file_ready = wait_for_file()
    table_created = create_table()
    parsed = parse_json()

    json_file >> file_ready >> table_created >> parsed
    load_to_postgres(parsed["data"], parsed["count"])
