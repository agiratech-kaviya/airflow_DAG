from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2023, 6, 15),
    'owner': 'airflow',
    'dbt_cloud_conn_id': 'dbt-connection',  # Replace with your DBT cloud connection ID
    'account_id': '176192',  # Replace with your DBT cloud account ID
}

with DAG(
    dag_id='sample_dag_with_bash',
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


