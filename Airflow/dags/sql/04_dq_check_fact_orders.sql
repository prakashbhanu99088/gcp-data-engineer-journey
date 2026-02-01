DO $$
DECLARE
  v_last_date_id INT;
  v_cnt BIGINT;
BEGIN
  SELECT last_date_id
  INTO v_last_date_id
  FROM retail_dw.etl_watermarks
  WHERE pipeline_name = 'fact_orders_incremental';

  IF v_last_date_id IS NULL THEN
    RAISE EXCEPTION 'DQ FAIL: watermark missing for pipeline fact_orders_incremental';
  END IF;

  SELECT COUNT(*)
  INTO v_cnt
  FROM retail_dw.fact_orders
  WHERE date_id = v_last_date_id;

  IF v_cnt = 0 THEN
    RAISE EXCEPTION 'DQ FAIL: fact_orders has 0 rows for watermark date_id=%', v_last_date_id;
  END IF;
END $$;
