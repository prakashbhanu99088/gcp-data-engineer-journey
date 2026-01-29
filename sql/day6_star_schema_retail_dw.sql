-- Day 6: Retail Analytics Warehouse (Star Schema)
-- Schema: retail_dw
-- DB: Postgres (dedb)

-- Safety: create schema + set search path
CREATE SCHEMA IF NOT EXISTS retail_dw;
SET search_path TO retail_dw;

-- =========================
-- 1) Dimension: Date
-- =========================
CREATE TABLE IF NOT EXISTS dim_date (
  date_id      INT PRIMARY KEY,           -- YYYYMMDD
  full_date    DATE NOT NULL UNIQUE,
  day          SMALLINT NOT NULL,
  month        SMALLINT NOT NULL,
  year         SMALLINT NOT NULL,
  week_of_year SMALLINT NOT NULL
);

-- Load last 90 days including today (safe re-run)
INSERT INTO dim_date (date_id, full_date, day, month, year, week_of_year)
SELECT
  TO_CHAR(d::date, 'YYYYMMDD')::int,
  d::date,
  EXTRACT(DAY FROM d)::smallint,
  EXTRACT(MONTH FROM d)::smallint,
  EXTRACT(YEAR FROM d)::smallint,
  EXTRACT(WEEK FROM d)::smallint
FROM generate_series(CURRENT_DATE - INTERVAL '90 days', CURRENT_DATE, INTERVAL '1 day') d
ON CONFLICT (date_id) DO NOTHING;

-- =========================
-- 2) Dimension: Product
-- =========================
CREATE TABLE IF NOT EXISTS dim_product (
  product_sk   SERIAL PRIMARY KEY,
  product_sku  TEXT NOT NULL UNIQUE,
  product_name TEXT NOT NULL,
  category     TEXT NOT NULL,
  unit_price   NUMERIC(10,2) NOT NULL
);

-- Sample products (safe re-run)
INSERT INTO dim_product (product_sku, product_name, category, unit_price) VALUES
('SKU-1001','Wireless Mouse','Electronics',19.99),
('SKU-1002','Mechanical Keyboard','Electronics',79.99),
('SKU-2001','Water Bottle','Home',12.50),
('SKU-3001','Notebook','Stationery',4.25)
ON CONFLICT (product_sku) DO NOTHING;

-- =========================
-- 3) Dimension: Customer (SCD2-ready structure)
-- =========================
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk      SERIAL PRIMARY KEY,      -- surrogate key
  customer_nk      TEXT NOT NULL,            -- natural/business key (CUST-001...)
  first_name       TEXT NOT NULL,
  last_name        TEXT NOT NULL,
  email            TEXT NOT NULL,
  country          TEXT NOT NULL,
  effective_start  TIMESTAMP NOT NULL DEFAULT now(),
  effective_end    TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  is_current       BOOLEAN NOT NULL DEFAULT true
);

-- Make sure only one "current" row per customer_nk
-- (Partial unique index: only rows where is_current=true must be unique)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current
ON dim_customer (customer_nk)
WHERE is_current = true;

-- Helpful index for as-of joins
CREATE INDEX IF NOT EXISTS idx_dim_customer_nk_dates
ON dim_customer (customer_nk, effective_start, effective_end);

-- Seed customers (safe re-run)
-- NOTE: On re-run, this will not insert duplicates because of the unique partial index
INSERT INTO dim_customer (customer_nk, first_name, last_name, email, country)
VALUES
('CUST-001','Olivia','Mackay','olivia.mackay@example.com','Canada'),
('CUST-002','Andre','White','andre.white@example.com','Australia'),
('CUST-003','Morgan','Simon','morgan.simon@example.com','France')
ON CONFLICT DO NOTHING;

-- =========================
-- 4) Fact: Orders (order-line grain)
-- =========================
CREATE TABLE IF NOT EXISTS fact_orders (
  order_id       TEXT NOT NULL,
  order_line_nbr INT  NOT NULL,
  date_id        INT  NOT NULL,
  customer_sk    INT  NOT NULL,
  product_sk     INT  NOT NULL,
  quantity       INT  NOT NULL,
  unit_price     NUMERIC(10,2) NOT NULL,

  -- Generated column (you saw this when trying to update line_amount manually)
  line_amount    NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,

  PRIMARY KEY (order_id, order_line_nbr),
  FOREIGN KEY (date_id)     REFERENCES dim_date(date_id),
  FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
  FOREIGN KEY (product_sk)  REFERENCES dim_product(product_sk)
);

-- Helpful indexes for analytics
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_id ON fact_orders(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_sk ON fact_orders(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product_sk ON fact_orders(product_sk);

-- Optional sample facts (safe re-run: PK prevents duplicates)
-- NOTE: Ensure these dates exist in dim_date before inserting.
-- If they don't exist, insert them into dim_date first.
INSERT INTO fact_orders (order_id, order_line_nbr, date_id, customer_sk, product_sk, quantity, unit_price)
SELECT 'ORD-90001', 1, 20260105, c.customer_sk, p.product_sk, 2, 19.99
FROM dim_customer c JOIN dim_product p
ON c.customer_nk='CUST-001' AND c.is_current=true
AND p.product_sku='SKU-1001'
ON CONFLICT (order_id, order_line_nbr) DO NOTHING;

INSERT INTO fact_orders (order_id, order_line_nbr, date_id, customer_sk, product_sk, quantity, unit_price)
SELECT 'ORD-90001', 2, 20260105, c.customer_sk, p.product_sk, 5, 4.25
FROM dim_customer c JOIN dim_product p
ON c.customer_nk='CUST-001' AND c.is_current=true
AND p.product_sku='SKU-3001'
ON CONFLICT (order_id, order_line_nbr) DO NOTHING;

-- Quick verification queries (optional)
-- SELECT COUNT(*) AS total_dates FROM dim_date;
-- SELECT * FROM dim_product ORDER BY product_sk;
-- SELECT * FROM dim_customer ORDER BY customer_sk;
-- SELECT * FROM fact_orders ORDER BY order_id, order_line_nbr;
