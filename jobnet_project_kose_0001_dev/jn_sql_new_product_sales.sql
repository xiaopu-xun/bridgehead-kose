-- @TD engine_version: 350
SELECT
  IF(
    CAST(substr(shipped_ymd,6,2) AS bigint)  >= 4,
    substr(shipped_ymd,1,4),
    CAST(CAST(substr(shipped_ymd,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  shipped_ymd AS date,
  new_product_sales_amount,
  existing_product_sales_amount
FROM
  tmp_reporting_003
ORDER BY
  date