from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent.parent  
sys.path.append(str(BASE_DIR / "src"))

from job2_cleaner import run_consumer

default_args = {
    'owner': 'damir',
    'start_date': datetime.now(),
}

with DAG(
    'job2_clean_store_dag',
    default_args=default_args,
    schedule="@hourly",  # every 1 hour
    catchup=False
) as dag:

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=run_consumer
    )

