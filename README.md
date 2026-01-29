# GCP Data Engineering Journey

## Day 
- Environment setup (Python, Git, Docker, VS Code)
- Created and ran first Python script
## Day 2
- Practiced Python functions
- Created and read CSV data
- Computed metrics from file-based data

## Day 3
- Pulled data from a public API and saved raw JSON
- Parsed JSON using Python and extracted fields (name, country)
- Computed basic counts from JSON data

## Day 4
- Converted raw JSON into a flat CSV table (JSON → CSV)
- Designed a simple schema: first_name, last_name, country, email
- Generated a structured dataset ready for loading into a database/BigQuery

## Day 5
- Started PostgreSQL using Docker
- Created a staging table (users_staging)
- Loaded CSV data into PostgreSQL using Python (psycopg2)
- Verified loaded rows using SQL queries

## Day 6 — Data Modeling (Star Schema Retail Warehouse)

- Created schema retail_dw (warehouse namespace)
  
 -  Built dimension tables:
  
  dim_date (generated calendar dates)
  
  dim_product (product master data)
  
  dim_customer (SCD Type 2 columns: effective_start/effective_end/is_current)
  
  Built fact table:
  
  fact_orders at order-line grain
  
  Foreign keys to date/product/customer
  
  Loaded sample data into dims and facts
  
  Verified with analytical joins (fact → dims)

## Day 7 — ETL/ELT Pipelines (Incremental + Idempotent)

Implemented SCD Type 2 update (CUST-001 Canada → France)

Verified customer history preserved and facts stay correct

Added pipeline state tracking:

etl_watermarks table (pipeline_name, last_date_id)

Built incremental pipeline using staging:

stg_orders for incoming daily orders

Dedup strategy using ROW_NUMBER() and ingestion time

UPSERT strategy using ON CONFLICT DO UPDATE

Validated real scenarios:

Late update: ORD-90003 quantity updated (3 → 5)

Dedup: ORD-90006 keeps latest record only
