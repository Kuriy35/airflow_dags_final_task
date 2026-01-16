from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

with DAG (
    dag_id = 'my_test_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,                     
    catchup=False
) as dag:
    def print_msg():
        msg = "Hello Kuriy!"

    task1 = PythonOperator (
        task_id="test_task",
        python_callable=print_msg
    )

    task1