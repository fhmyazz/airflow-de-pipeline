from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import extract, transform, load_to_bigq

default_args = {
    'owner': 'fahmy',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_etl_pipeline',
    default_args = default_args,
    description = 'Extract trending Github, Transform, and Load to BigQuery',
    schedule = '0 8 * * *',
    catchup = False,
    tags = ['etl', 'bigquery'],
)

def extract_wrapper(**context):
    repos = extract(language='python', min_stars=10000, max_pages=2, per_page=10)
    context['task_instance'].xcom_push(key='repos', value=repos)
    return len(repos)

def transform_wrapper(**context):
    repos = context['task_instance'].xcom_pull(key='repos', task_ids='extract_task')
    df = transform(repos)

    context['task_instance'].xcom_push(key='df_json', value=df.to_json(orient='records'))
    return len(df)

def load_wrapper(**context):
    import pandas as pd

    df_json = context['task_instance'].xcom_pull(key='df_json', task_ids='transform_task')
    df = pd.read_json(df_json, orient='records')
    load_to_bigq(df)
    return "Load Success"

extract_task = PythonOperator(
    task_id = 'extract_task',
    python_callable = extract_wrapper,
    dag = dag
)

transform_task = PythonOperator(
    task_id = 'transform_task',
    python_callable = transform_wrapper,
    dag = dag
)

load_task = PythonOperator(
    task_id = 'load_task',
    python_callable = load_wrapper,
    dag = dag
)

extract_task >> transform_task >> load_task