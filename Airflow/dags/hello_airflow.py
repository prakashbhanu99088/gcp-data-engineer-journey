from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # manual run
    catchup=False,
    tags=["week8", "basics"],
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow!"',
    )

    t3 = BashOperator(
        task_id="show_working_dir",
        bash_command="pwd && ls -la",
    )

    t1 >> t2 >> t3
