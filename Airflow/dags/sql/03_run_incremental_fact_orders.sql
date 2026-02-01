-- 1) Ensure dim_date has dates present in staging (prevents FK failures)
INSERT INTO retail_dw.dim_date (date_id, full_date, day, month, year, week_of_year)
SELECT
  TO_CHAR(s.order_date, 'YYYYMMDD')::int,
  s.order_date,
  EXTRACT(DAY FROM s.order_date)::smallint,
  EXTRACT(MONTH FROM s.order_date)::smallint,
  EXTRACT(YEAR FROM s.order_date)::smallint,
  EXTRACT(WEEK FROM s.order_date)::smallint
FROM retail_dw.stg_orders s
ON CONFLICT (date_id) DO NOTHING;

-- 2) Incremental + idempotent load with DEDUP
WITH wm AS (
  SELECT last_date_id
  FROM retail_dw.etl_watermarks
  WHERE pipeline_name = 'fact_orders_incremental'
),
deduped AS (
  SELECT *
  FROM (
    SELECT
      s.*,
      ROW_NUMBER() OVER (
        PARTITION BY s.order_id, s.order_line_nbr
        ORDER BY s.loaded_at DESC
      ) AS rn
    FROM retail_dw.stg_orders s
  ) x
  WHERE rn = 1
),
src AS (
  SELECT
    d.order_id,
    d.order_line_nbr,
    TO_CHAR(d.order_date, 'YYYYMMDD')::int AS date_id,
    c.customer_sk,
    p.product_sk,
    d.quantity,
    d.unit_price
  FROM deduped d
  JOIN retail_dw.dim_customer c
    ON c.customer_nk = d.customer_nk
   AND c.is_current = TRUE
  JOIN retail_dw.dim_product p
    ON p.product_sku = d.product_sku
  WHERE TO_CHAR(d.order_date, 'YYYYMMDD')::int >= (SELECT last_date_id FROM wm)
)
INSERT INTO retail_dw.fact_orders
(order_id, order_line_nbr, date_id, customer_sk, product_sk, quantity, unit_price)
SELECT order_id, order_line_nbr, date_id, customer_sk, product_sk, quantity, unit_price
FROM src
ON CONFLICT (order_id, order_line_nbr)
DO UPDATE SET
  quantity   = EXCLUDED.quantity,
  unit_price = EXCLUDED.unit_price;

-- 3) Move watermark forward after successful load
UPDATE retail_dw.etl_watermarks
SET last_date_id = GREATEST(
      last_date_id,
      (SELECT COALESCE(MAX(TO_CHAR(order_date,'YYYYMMDD')::int), last_date_id) FROM retail_dw.stg_orders)
    ),
    updated_at = now()
WHERE pipeline_name = 'fact_orders_incremental';
