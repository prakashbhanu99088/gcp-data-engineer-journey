from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from python.generate_orders import generate_orders
from python.alerts import notify_failure

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": notify_failure,   
}


with DAG(
    dag_id="week8_day2_retail_pipeline",
    start_date=pendulum.datetime(2026, 2, 2, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week8", "retail_dw", "postgres", "incremental"],
    template_searchpath=["/opt/airflow/dags/sql"],
    on_failure_callback=notify_failure,  # DAG-level failure callback
) as dag:

    generate_orders_task = PythonOperator(
        task_id="generate_orders",
        python_callable=generate_orders,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )

    smoke_test_connection = SQLExecuteQueryOperator(
        task_id="smoke_test_connection",
        conn_id="retail_dw_pg",
        sql="01_smoke_test.sql",
    )

    check_retail_dw_tables = SQLExecuteQueryOperator(
        task_id="check_retail_dw_tables",
        conn_id="retail_dw_pg",
        sql="02_check_tables.sql",
    )

    run_incremental_fact_orders = SQLExecuteQueryOperator(
        task_id="run_incremental_fact_orders",
        conn_id="retail_dw_pg",
        sql="03_run_incremental_fact_orders.sql",
    )

    data_quality_check_fact_orders = SQLExecuteQueryOperator(
        task_id="data_quality_check_fact_orders",
        conn_id="retail_dw_pg",
        sql="04_dq_check_fact_orders.sql",
    )

    generate_orders_task >> smoke_test_connection >> check_retail_dw_tables >> run_incremental_fact_orders >> data_quality_check_fact_orders
