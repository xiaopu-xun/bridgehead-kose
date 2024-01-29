SELECT
  system_code,
  IF(
    CAST(substr(aggregate_day,6,2) AS bigint)  >= 4,
    substr(aggregate_day,1,4),
    CAST(CAST(substr(aggregate_day,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  aggregate_day AS date,
  -- 通常の売上金額(全体-(ギフトセット売上+ギフトラップ売上+刻印売上))
  (MAX(CASE kind WHEN '1' THEN sales_amount ELSE 0 END)
  - (MAX(CASE kind WHEN '2' THEN sales_amount ELSE 0 END) 
    + MAX(CASE kind WHEN '3' THEN sales_amount ELSE 0 END)
    + MAX(CASE kind WHEN '4' THEN sales_amount ELSE 0 END)))AS normal_sales_amount,
  -- ギフトセットの売上金額
  MAX(CASE kind WHEN '2' THEN sales_amount ELSE 0 END) AS gift_set_sales_amount,
  -- ギフトセットの売上数量
  MAX(CASE kind WHEN '2' THEN sales_quantity ELSE 0 END) AS gift_set_sales_quantity,
  -- ギフトラップの売上金額
  MAX(CASE kind WHEN '3' THEN sales_amount ELSE 0 END) AS gift_wrapping_sales_amount,
  -- ギフトラップの売上数量
  MAX(CASE kind WHEN '3' THEN sales_quantity ELSE 0 END) AS gift_wrapping_sales_quantity,
  -- ギフトラップの手数料売上金額
  MAX(CASE kind WHEN '3' THEN service_amount ELSE 0 END) AS gift_wrapping_service_amount,
  -- ギフトラップの手数料売上数量
  MAX(CASE kind WHEN '3' THEN service_quantity ELSE 0 END) AS gift_wrapping_service_quantity,
  -- ギフトラップの手数料売上数量（無料除く）
  MAX(CASE kind WHEN '3' THEN not_free_service_quantity ELSE 0 END) AS gift_wrapping_not_free_service_quantity,
  -- 刻印の売上金額
  MAX(CASE kind WHEN '4' THEN sales_amount ELSE 0 END) AS carved_seal_sales_amount,
  -- 刻印の売上数量
  MAX(CASE kind WHEN '4' THEN sales_quantity ELSE 0 END) AS carved_seal_sales_quantity,
  -- 刻印の手数料売上金額
  MAX(CASE kind WHEN '4' THEN service_amount ELSE 0 END) AS carved_seal_service_amount,
  -- 刻印の手数料売上数量
  MAX(CASE kind WHEN '4' THEN service_quantity ELSE 0 END) AS carved_seal_service_quantity,
  -- 刻印の手数料売上数量（無料除く）
  MAX(CASE kind WHEN '4' THEN not_free_service_quantity ELSE 0 END) AS carved_seal_not_free_service_quantity
FROM
  tmp_reporting_012
GROUP BY
  system_code,
  aggregate_day
ORDER BY
  system_code,
  date