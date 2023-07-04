from datetime import datetime
from airflow import DAG
from airflow.providers.dbt.operators.dbt import (
    DbtCloudRunJobOperator,
    DbtCloudGetJobRunArtifactOperator,
    DbtCloudJobRunSensor,
)

default_args = {
    'start_date': datetime(2023, 6, 1),
    'owner': 'airflow',
    'account_id': '175881',
}

with DAG(
    dag_id='example_dbt_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    dbt_cloud_conn_id='dbt-connection'
) as dag:
    # Task 1: Run a specific DBT job that executes a specific model
    run_job_task = DbtCloudRunJobOperator(
        task_id='run_dbt_job',
        job_id='341118',  # Replace with your specific DBT job ID
        check_interval=60,
        timeout=3600,
    )

    # Task 2: Get the artifact from the DBT job run
    get_artifact_task = DbtCloudGetJobRunArtifactOperator(
        task_id='get_dbt_artifact',
        run_id=run_job_task.output,
        path='run_results.json',
    )

    # Task 3: Wait for the DBT job run to complete
    job_run_sensor_task = DbtCloudJobRunSensor(
        task_id='dbt_job_sensor',
        run_id=run_job_task.output,
        timeout=1800,
    )

    run_job_task >> get_artifact_task >> job_run_sensor_task
