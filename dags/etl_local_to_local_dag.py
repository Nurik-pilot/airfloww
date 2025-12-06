from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
from typing import List, Dict

# Пути к файлам
BASE_DIR = "/opt/airflow/data"
RAW_FILE = os.path.join(BASE_DIR, "raw_products.json")
STAGING_FILE = os.path.join(BASE_DIR, "staging_products.json")
CLEAN_FILE = os.path.join(BASE_DIR, "products_clean.json")

# Функция extract
def extract_raw_data() -> None:
    print("🔍 Запуск extract_raw_data")
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"Файл не найден: {RAW_FILE}")
    with open(RAW_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(STAGING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Данные извлечены и сохранены в {STAGING_FILE}")

# Функция transform
def transform_data() -> None:
    print("🔧 Запуск transform_data")
    if not os.path.exists(STAGING_FILE):
        raise FileNotFoundError(f"Файл не найден: {STAGING_FILE}")
    with open(STAGING_FILE, "r", encoding="utf-8") as f:
        products: List[Dict] = json.load(f)
    transformed = []
    for product in products:
        price_int = int(product["price"])
        with_vat = round(price_int * 1.12, 2)
        product_clean = {
            "id": product["id"],
            "name": product["name"],
            "price": price_int,
            "with_vat": with_vat
        }
        transformed.append(product_clean)
    with open(CLEAN_FILE, "w", encoding="utf-8") as f:
        json.dump(transformed, f, indent=2, ensure_ascii=False)
    print(f"✅ Данные преобразованы и сохранены в {CLEAN_FILE}")

# Функция load
def load_data(**kwargs) -> None:
    print("📦 Запуск load_data")
    execution_date = kwargs["execution_date"]  # ← добавляем эту строку

    if not os.path.exists(CLEAN_FILE):
        raise FileNotFoundError(f"Файл не найден: {CLEAN_FILE}")
    with open(CLEAN_FILE, "r", encoding="utf-8") as f:
        products: List[Dict] = json.load(f)
    print(f"📊 Обработано {len(products)} строк на дату запуска DAG: {execution_date.strftime('%Y-%m-%d')}")

# DAG
default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
}

dag = DAG(
    dag_id="etl_local_to_local",
    default_args=default_args,
    start_date=datetime(2025, 11, 6),
    schedule_interval="0 5 * * 1,2,5",  # Пн, Вт, Пт в 5:00
    catchup=False,
    tags=["etl", "local"],
)

extract_task = PythonOperator(
    task_id="extract_raw_data",
    python_callable=extract_raw_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

extract_task >> transform_task >> load_task