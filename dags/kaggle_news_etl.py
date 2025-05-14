from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Exercice_de_Synthese',
    default_args=default_args,
    description='A simple HackerNews pipeline',
    schedule=timedelta(minutes=5),
)

SCRIPTS_PATH = "/opt/airflow/scripts"

step1_to_s3 = BashOperator(
    task_id='step1_to_s3',
    bash_command=f'python {SCRIPTS_PATH}/step1_to_s3.py  --endpoint-url http://localstack:4566',
    dag=dag,
)


step2_to_sql = BashOperator(
    task_id='step2_to_sql',
    bash_command=f'python {SCRIPTS_PATH}/step2_to_sql.py',
    dag=dag,
)

step3_to_mongo = BashOperator(
    task_id='step3_to_mongo',
    bash_command=f'python {SCRIPTS_PATH}/step3_to_mongo.py',
    dag=dag,
)

step4_index_el = BashOperator(
    task_id='step4_index_el',
    bash_command=f'python {SCRIPTS_PATH}/step4_index_el.py',
    dag=dag,
)


step1_to_s3 >>  step2_to_sql >> step3_to_mongo >> step4_index_el