from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# from dags.python_packages.test_connect_to_db.main import main
from python_packages.test_connect_to_db.main import main

def func():
    main()

with DAG(
    "db_connect_test_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    # connect_test01 = BashOperator(task_id="task_1", bash_command="echo 'Hello World from DAG task 1'")
    connect_test01 = PythonOperator(
        task_id="connect_test01" ,
        python_callable=func
    )