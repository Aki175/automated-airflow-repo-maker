from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'test_dag',
    description='Simple test DAG',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    tags=['test'],
)

task = BashOperator(
    task_id='hello',
    bash_command='echo "AKi is!"',
    dag=dag,
)
