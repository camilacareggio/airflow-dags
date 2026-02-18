from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.providers.standard.operators.bash import BashOperator 
import zipfile
import os

# DAG arguments
default_args = {
    'owner': 'Camila',
    'start_date': datetime(2026, 2, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule=timedelta(days=1),
)

DATA_DIR = '/tmp/airflow/ETL_toll_data/'

# DAG tasks
download_data = BashOperator(
    task_id='download_data',
    bash_command=f'''
        mkdir -p {DATA_DIR} &&
        curl -o {DATA_DIR}tolldata.tgz https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
    ''',
    dag=dag
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzf {DATA_DIR}tolldata.tgz -C {DATA_DIR}',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'cut -d"," -f1,4 {DATA_DIR}vehicle-data.csv > {DATA_DIR}csv_data.csv',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f'cut -f5,7 {DATA_DIR}tollplaza-data.tsv > {DATA_DIR}tsv_data.csv',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'cut -c59-67,69-71 {DATA_DIR}payment-data.txt > {DATA_DIR}fixed_width_data.csv',
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f'paste -d"," {DATA_DIR}csv_data.csv {DATA_DIR}tsv_data.csv {DATA_DIR}fixed_width_data.csv > {DATA_DIR}extracted_data.csv',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'tr "[a-z]" "[A-Z]" < {DATA_DIR}extracted_data.csv > {DATA_DIR}transformed_data.csv',
    dag=dag
)

preview_final_data = BashOperator(
    task_id='preview_final_data',
    bash_command=f'head -n 10 {DATA_DIR}transformed_data.csv',
    dag=dag
)


download_data >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data >> preview_final_data