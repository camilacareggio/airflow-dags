from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'simple_example_DAG',
    description='A simple example DAG',
    default_args=default_args,
    schedule=timedelta(seconds=5)
)

# Task definitions
task1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, World!"',
    dag=dag
)

task2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

# Task pipeline
task1 >> task2