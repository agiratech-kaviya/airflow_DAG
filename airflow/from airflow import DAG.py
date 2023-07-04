from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow_test',
    'start_date': days_ago(1),
}

dag = DAG(
    'dbt_cloud_dag',
    default_args=default_args,
    description='A DAG to interact with dbt Cloud',
    schedule_interval='0 0 * * *',  # Run daily at midnight
)

run_dbt_job = BashOperator(
    task_id='run_dbt_job',
    bash_command='dbt cloud run --job 341118',
    dag=dag,
)

run_dbt_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='dbt cloud run-operation run-models --models my_third_dbt_model',
    dag=dag,
)

run_dbt_job >> run_dbt_model