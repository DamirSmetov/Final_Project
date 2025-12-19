from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent.parent  
sys.path.append(str(BASE_DIR / "src"))

from job2_cleaner import run_consumer
from job3_analytics import run_daily_analytics

default_args = {
    'owner': 'damir',
    'start_date': datetime.now(),
}

with DAG(
    'job3_daily_summary_dag',
    default_args=default_args,
    schedule="@daily",  # every 1 day
    catchup=False
) as dag:

    run_daily_analytics = PythonOperator(
        task_id='run_daily_analytics',
        python_callable=run_daily_analytics
    )

