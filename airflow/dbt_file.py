from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Kaviya',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 13),
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'dbt_dag',
    default_args=default_args,
    description='A DAG to schedule DBT tasks',
    schedule_interval='*/10 * * * *',  # Every 10 seconds
    catchup=False
)

dbt_task = BashOperator(
    task_id='run_dbt_models',
    bash_command='dbt run --models join-table-with-union.sql',
    dag=dag
)

dbt_task
