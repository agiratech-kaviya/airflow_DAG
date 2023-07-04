from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2023, 6, 15),
    'owner': 'airflow',
}

with DAG(
    dag_id='ssql_operator',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    # Task 1: Run a specific DBT model without specifying a target
    run_model_task = BashOperator(
        task_id='run_model_task',
        bash_command='dbt run --models customers1',  # Replace with the name of the specific DBT model
    )

run_model_task
