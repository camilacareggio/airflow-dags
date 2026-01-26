from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
import zipfile
import os

# DAG arguments
default_args = {
    'owner': 'Camila',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL_Server_Access_Log_Processing',
    schedule=timedelta(days=1),
)

DATA_DIR = '/tmp/airflow/ETL_Server_Access_Log_Processing/'

# DAG tasks
prepare = BashOperator(
    task_id='prepare',
    bash_command=f'mkdir -p {DATA_DIR}',
    dag=dag
)

download = BashOperator(
    task_id='download',
    bash_command=f'wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" -O {DATA_DIR}/web-server-access-log.txt',
    dag=dag
)

extract = BashOperator(
    task_id='extract',
    bash_command=f'cut -d"#" -f1,4 {DATA_DIR}/web-server-access-log.txt > {DATA_DIR}/extracted-data.txt',
    dag=dag,
)

transform = BashOperator(
    task_id='transform',
    bash_command=f'tr "[a-z]" "[A-Z]" < {DATA_DIR}/extracted-data.txt > {DATA_DIR}/capitalized-data.txt',
    dag=dag,
)

def zip_file():
    zip_path = os.path.join(DATA_DIR, "log.zip")
    file_path = os.path.join(DATA_DIR, "capitalized-data.txt")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_path, arcname="capitalized-data.txt")

load = PythonOperator(
    task_id="load",
    python_callable=zip_file,
)

# DAG pipeline
prepare >> download >> extract >> transform >> load