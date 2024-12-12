from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "sample_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    task_1 = BashOperator(task_id="task_1", bash_command="echo 'Hello World from DAG task 1 Commit and Push was worked?'")