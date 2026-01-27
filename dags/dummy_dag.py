from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 

# DAG arguments
default_args = {
    'owner': 'Camila',
    'start_date': datetime(2026, 1, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='Dummy dag for demonstration',
    schedule=timedelta(minutes=1),
)

# DAG tasks
task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 1',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 2',
    dag=dag,
)

task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 3',
    dag=dag,
)

# DAG pipeline
task1 >> task2 >> task3