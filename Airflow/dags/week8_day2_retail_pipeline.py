from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,  # retry twice if fail
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="week8_day2_retail_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule_interval="0 2 * * *",  # every day at 2:00 AM
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week8", "retail_dw", "postgres", "incremental"],
    template_searchpath=["/opt/airflow/dags/sql"],
) as dag:

    t1_smoke_test = PostgresOperator(
        task_id="smoke_test_connection",
        postgres_conn_id="retail_dw_pg",
        sql="01_smoke_test.sql",
    )

    t2_check_tables = PostgresOperator(
        task_id="check_retail_dw_tables",
        postgres_conn_id="retail_dw_pg",
        sql="02_check_tables.sql",
    )

    t3_run_incremental_fact = PostgresOperator(
        task_id="run_incremental_fact_orders",
        postgres_conn_id="retail_dw_pg",
        sql="03_run_incremental_fact_orders.sql",
    )

    # Data Quality Check: ensure orders exist for the last watermark date
    t4_data_quality_check = PostgresOperator(
        task_id="data_quality_check_fact_orders",
        postgres_conn_id="retail_dw_pg",
        sql="04_dq_check_fact_orders.sql",
)

    t1_smoke_test >> t2_check_tables >> t3_run_incremental_fact >> t4_data_quality_check
