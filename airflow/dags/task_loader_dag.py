# task_loader_dag.py
import logging
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

sys.path.append('/opt/airflow/dags')

from tasks.load_tasks import LoadTasks

default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2025, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_djinni_folder',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['djinni', 'load'],
    params={
        "folder_path": Param(
            default="/opt/airflow/data/scraped_jobs/2025-11-06_17-38",
            type="string"
        )
    }
)

load_tasks = LoadTasks()

load_task = PythonOperator(
    task_id='load_folder',
    python_callable=load_tasks.load_from_files,
    dag=dag
)
