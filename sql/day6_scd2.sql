-- Day 6: SCD Type 2 simulation (customer moves country)
-- Scenario: Customer CUST-001 moves from Canada -> France

SET search_path TO retail_dw;

-- 1) Create staging table for customer updates
CREATE TABLE IF NOT EXISTS stg_customer_updates (
  customer_nk TEXT NOT NULL,
  first_name  TEXT NOT NULL,
  last_name   TEXT NOT NULL,
  email       TEXT NOT NULL,
  country     TEXT NOT NULL
);

-- 2) Load update into staging (safe to rerun)
TRUNCATE stg_customer_updates;

INSERT INTO stg_customer_updates VALUES
('CUST-001','Olivia','Mackay','olivia.mackay@example.com','France');

-- 3) Expire current record IF something changed
UPDATE dim_customer dc
SET effective_end = now(),
    is_current = false
FROM stg_customer_updates s
WHERE dc.customer_nk = s.customer_nk
  AND dc.is_current = true
  AND (
    dc.first_name <> s.first_name OR
    dc.last_name  <> s.last_name  OR
    dc.email      <> s.email      OR
    dc.country    <> s.country
  );

-- 4) Insert new current version if no current row matches (after expiry)
INSERT INTO dim_customer
(customer_nk, first_name, last_name, email, country, effective_start, effective_end, is_current)
SELECT
  s.customer_nk,
  s.first_name,
  s.last_name,
  s.email,
  s.country,
  now(),
  '9999-12-31',
  true
FROM stg_customer_updates s
LEFT JOIN dim_customer dc
  ON dc.customer_nk = s.customer_nk
 AND dc.is_current = true
WHERE dc.customer_sk IS NULL;

-- 5) Verify customer history
SELECT
  customer_sk,
  customer_nk,
  country,
  effective_start,
  effective_end,
  is_current
FROM dim_customer
WHERE customer_nk = 'CUST-001'
ORDER BY effective_start;
