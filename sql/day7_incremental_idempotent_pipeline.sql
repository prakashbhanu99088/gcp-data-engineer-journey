-- =========================================
-- DAY 7: INCREMENTAL + IDEMPOTENT FACT PIPELINE
-- =========================================

-- 1. Watermark table (load tracking)
CREATE TABLE IF NOT EXISTS retail_dw.etl_watermarks (
  pipeline_name TEXT PRIMARY KEY,
  last_date_id  INT NOT NULL,
  updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO retail_dw.etl_watermarks (pipeline_name, last_date_id)
VALUES ('fact_orders_incremental', 0)
ON CONFLICT (pipeline_name) DO NOTHING;

-- =========================================
-- 2. Staging table
CREATE TABLE IF NOT EXISTS retail_dw.stg_orders (
  order_id       TEXT NOT NULL,
  order_line_nbr INT  NOT NULL,
  order_date     DATE NOT NULL,
  customer_nk    TEXT NOT NULL,
  product_sku    TEXT NOT NULL,
  quantity       INT  NOT NULL,
  unit_price     NUMERIC(10,2) NOT NULL,
  loaded_at      TIMESTAMP NOT NULL DEFAULT now(),
  row_hash       TEXT
);

-- =========================================
-- 3. Incremental INSERT (new data only)
WITH wm AS (
  SELECT last_date_id
  FROM retail_dw.etl_watermarks
  WHERE pipeline_name = 'fact_orders_incremental'
),
src AS (
  SELECT
    s.order_id,
    s.order_line_nbr,
    TO_CHAR(s.order_date, 'YYYYMMDD')::int AS date_id,
    c.customer_sk,
    p.product_sk,
    s.quantity,
    s.unit_price
  FROM retail_dw.stg_orders s
  JOIN retail_dw.dim_customer c
    ON c.customer_nk = s.customer_nk
   AND c.is_current = TRUE
  JOIN retail_dw.dim_product p
    ON p.product_sku = s.product_sku
  WHERE TO_CHAR(s.order_date,'YYYYMMDD')::int > (SELECT last_date_id FROM wm)
)
INSERT INTO retail_dw.fact_orders
(order_id, order_line_nbr, date_id, customer_sk, product_sk, quantity, unit_price)
SELECT * FROM src
ON CONFLICT (order_id, order_line_nbr) DO NOTHING;

-- =========================================
-- 4. UPSERT for corrections (same-day or late updates)
WITH wm AS (
  SELECT last_date_id
  FROM retail_dw.etl_watermarks
  WHERE pipeline_name = 'fact_orders_incremental'
),
src AS (
  SELECT
    s.order_id,
    s.order_line_nbr,
    TO_CHAR(s.order_date, 'YYYYMMDD')::int AS date_id,
    c.customer_sk,
    p.product_sk,
    s.quantity,
    s.unit_price
  FROM retail_dw.stg_orders s
  JOIN retail_dw.dim_customer c
    ON c.customer_nk = s.customer_nk
   AND c.is_current = TRUE
  JOIN retail_dw.dim_product p
    ON p.product_sku = s.product_sku
  WHERE TO_CHAR(s.order_date,'YYYYMMDD')::int >= (SELECT last_date_id FROM wm)
)
INSERT INTO retail_dw.fact_orders
(order_id, order_line_nbr, date_id, customer_sk, product_sk, quantity, unit_price)
SELECT * FROM src
ON CONFLICT (order_id, order_line_nbr)
DO UPDATE SET
  quantity   = EXCLUDED.quantity,
  unit_price = EXCLUDED.unit_price;

-- =========================================
-- 5. Deduplication logic (latest record wins)
UPDATE retail_dw.stg_orders
SET row_hash = md5(
  order_id || '|' ||
  order_line_nbr || '|' ||
  order_date || '|' ||
  customer_nk || '|' ||
  product_sku || '|' ||
  quantity || '|' ||
  unit_price
);

-- =========================================
-- 6. Update watermark AFTER successful load
UPDATE retail_dw.etl_watermarks
SET last_date_id = GREATEST(
      last_date_id,
      (SELECT COALESCE(MAX(TO_CHAR(order_date,'YYYYMMDD')::int), last_date_id)
       FROM retail_dw.stg_orders)
    ),
    updated_at = now()
WHERE pipeline_name = 'fact_orders_incremental';
