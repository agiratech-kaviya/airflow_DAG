from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

default_args = {
    'start_date': datetime(2023, 6, 15),
}

def run_dbt_job():
    account_id = '176192'  # Replace with your DBT Cloud account ID
    model_name = 'customers1'  # Replace with the name of the specific DBT model
    
    # Make a request to the DBT Cloud API to trigger the job run for the specific model
    url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/run/'
    headers = {
        'Authorization': 'Bearer cbba08e2a8040983908fac10bee7b11ed758fdc4',  # Replace with your DBT Cloud API token
    }
    data = {
        'models': [model_name]
    }
    response = requests.post(url, headers=headers, json=data)
    
    # Handle the response
    if response.status_code == 201:
        job_id = response.json()['data']['id']
        print(f'DBT job run successfully triggered for model: {model_name}. Job ID: {job_id}')
    else:
        print('Failed to trigger DBT job run.')
        print('Response:', response.json())

with DAG(
    dag_id='dbt_cloud_dag',
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:
    run_dbt_task = PythonOperator(
        task_id='run_dbt_job',
        python_callable=run_dbt_job
    )

    run_dbt_task
