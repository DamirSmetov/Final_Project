from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent.parent 
sys.path.append(str(BASE_DIR / "src"))

from job1_producer import main

default_args = {
    'owner': 'damir',
    'start_date': datetime.now(),
}

with DAG(
    'job1_ingestion_dag',
    default_args=default_args,
    schedule='*/1 * * * *',  # every 1 minute
    catchup=False
) as dag:

    produce_data = PythonOperator(
        task_id='produce_data',
        python_callable=main
    )

