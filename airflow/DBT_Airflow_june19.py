from datetime import datetime
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

default_args = {
    'start_date': datetime(2023, 6, 19),
    'owner': 'airflow_testing2',
    'dbt_cloud_conn_id': 'dbt-connection',
    'account_id': '177080'
}

with DAG(
    dag_id='sample_five',
    schedule_interval='*/10 * * * *',
    default_args=default_args,
    catchup=False
) as dag:
    # Task 1: Run a specific DBT job that executes a specific model
    run_job_task = DbtCloudRunJobOperator(
        task_id='run_job_task',
        job_id='350818',  # Replace with your specific DBT job ID
        check_interval=60,
        timeout=3600,
    )

run_job_task 