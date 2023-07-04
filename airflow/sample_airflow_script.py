from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Sample Airflow',
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
    bash_command='echo "Running DBT models"',
    dag=dag
)

# Add more tasks if needed

dbt_task2 = BashOperator(
    task_id='another_task',
    bash_command='echo "Running another task"',
    dag=dag
)

# Set dependencies between tasks if needed
dbt_task2.set_downstream(dbt_task)

# Add more dependencies if needed

