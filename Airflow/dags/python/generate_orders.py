from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
log = logging.getLogger("airflow.task")
def generate_orders(ds_nodash: str, **context):
    """
    ds_nodash comes from Airflow macro: YYYYMMDD (e.g., 20260202)
    This makes order_id unique per day -> safe daily schedule.
    """
    hook = PostgresHook(postgres_conn_id="retail_dw_pg")
    order_date = datetime.strptime(ds_nodash, "%Y%m%d").date()
    # Unique order IDs per DAG run day
    order1 = f"ORD-{ds_nodash}-001"
    order2 = f"ORD-{ds_nodash}-002"

    orders = [
        (order1, 1, date.today(), "CUST-001", "SKU-2001", 2, 12.50),
        (order1, 2, date.today(), "CUST-001", "SKU-3001", 1, 4.25),
        (order2, 1, date.today(), "CUST-002", "SKU-1001", 1, 19.99),
    ]

    # staging should represent “today’s incoming batch”
    hook.run("TRUNCATE retail_dw.stg_orders;")

    hook.insert_rows(
        table="retail_dw.stg_orders",
        rows=orders,
        target_fields=[
            "order_id", "order_line_nbr", "order_date",
            "customer_nk", "product_sku", "quantity", "unit_price"
        ],
        commit_every=1000,
    )

    log.info("Inserted %s rows into stg_orders for ds=%s", len(orders), ds_nodash)
