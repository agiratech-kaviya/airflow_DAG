from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.utils.dates import days_ago
from subprocess import run

default_args = {
    'start_date': days_ago(1),
    'owner': 'airflow',
}

def run_dbt_job(job_id, conn_id):
    # Get the DBT connection details from Airflow
    dbt_conn = Connection.get_connection(conn_id)

    # Use the connection details to run the DBT job
    # Replace with your DBT command and arguments
    dbt_command = ['dbt', 'run', '--job', str(job_id)]
    run(dbt_command, check=True, env={'DBT_CONNECTION': dbt_conn.get_uri()})

def get_dbt_artifact(conn_id):
    # Get the DBT connection details from Airflow
    dbt_conn = Connection.get_connection(conn_id)

    # Use the connection details to retrieve the DBT artifact file
    # For example, you can use the DBT API or any other method supported by your DBT environment

with DAG(
    dag_id='example_dbt_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
) as dag:
    # Task 1: Run a DBT job
    run_job_task = PythonOperator(
        task_id='run_dbt_job',
        python_callable=run_dbt_job,
        op_kwargs={'job_id': 341118, 'conn_id': 'dbt-connection'},  # Replace with your DBT job ID and connection ID
    )

    # Task 2: Get the artifact from the DBT job run
    get_artifact_task = PythonOperator(
        task_id='get_dbt_artifact',
        python_callable=get_dbt_artifact,
        op_kwargs={'conn_id': 'dbt-connection'},  # Replace with your connection ID
    )

    # Define the task dependencies
    run_job_task >> get_artifact_task
