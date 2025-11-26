from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="airflow_monitoring",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    echo_task = BashOperator(
        task_id="echo_test", bash_command="echo 'Airflow monitoring DAG is running'"
    )
