DO $$
DECLARE
  v_last_date_id INT;
  v_fact_cnt BIGINT;
  v_bad_cnt BIGINT;
BEGIN
  SELECT last_date_id
  INTO v_last_date_id
  FROM retail_dw.etl_watermarks
  WHERE pipeline_name = 'fact_orders_incremental';

  IF v_last_date_id IS NULL THEN
    RAISE EXCEPTION 'DQ FAIL: watermark missing';
  END IF;

  SELECT COUNT(*)
  INTO v_fact_cnt
  FROM retail_dw.fact_orders
  WHERE date_id = v_last_date_id;

  IF v_fact_cnt = 0 THEN
    RAISE EXCEPTION 'DQ FAIL: 0 fact_orders rows for date_id=%', v_last_date_id;
  END IF;

  SELECT COUNT(*)
  INTO v_bad_cnt
  FROM retail_dw.fact_orders
  WHERE date_id = v_last_date_id
    AND (customer_sk IS NULL OR product_sk IS NULL);

  IF v_bad_cnt > 0 THEN
    RAISE EXCEPTION 'DQ FAIL: % rows have NULL keys for date_id=%', v_bad_cnt, v_last_date_id;
  END IF;
END $$;
