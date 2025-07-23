from datetime import datetime, timedelta
from airflow import DAG
from aiirflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'airflow',
    'start_data': datetime(2025,7,18),
    'depends_on_past':False,
    'email':['njokuchinasa86@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG('run_external_script',
          default_args = default_args,
          description = 'Runs an external Python script',
          schedule_interval = '@daily',
          catchup = False)

with dag:
    run_script_task = BashOperator(
        task_id = 'run_script',
        bash_command = 'python/home/airflow/gcs/dags/scripts/extract_data.py'
    )