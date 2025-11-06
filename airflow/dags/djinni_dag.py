from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

sys.path.append('/opt/airflow/dags')

from tasks.extract_tasks import ExtractTasks
from tasks.transform_tasks import TransformTasks
from tasks.load_tasks import LoadTasks
from tasks.report_tasks import ReportTasks

default_args = {
    'owner': 'djinni',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'djinni_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for djinni.co job scraping',
    schedule_interval=timedelta(hours=24),
    catchup=False,
    max_active_runs=1,
    tags=['djinni', 'etl', 'jobs']
)

extract_tasks = ExtractTasks(max_pages=700, max_jobs=11000)
transform_tasks = TransformTasks()
load_tasks = LoadTasks()
report_tasks = ReportTasks()

extract_task = PythonOperator(
    task_id='extract_catalog',
    python_callable=extract_tasks.extract_catalog_urls,
    dag=dag
)

save_catalog_task = PythonOperator(
    task_id='save_catalog',
    python_callable=load_tasks.save_catalog_to_db,
    dag=dag
)

scrape_task = PythonOperator(
    task_id='scrape_jobs',
    python_callable=transform_tasks.scrape_jobs,
    dag=dag
)

load_files_task = PythonOperator(
    task_id='load_from_files',
    python_callable=load_tasks.load_from_files,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=report_tasks.generate_report,
    dag=dag
)

extract_task >> save_catalog_task >> scrape_task >> load_files_task >> report_task
