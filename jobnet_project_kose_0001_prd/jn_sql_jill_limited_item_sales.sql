SELECT
  IF(
    CAST(substr(shipped_ymd,6,2) AS bigint)  >= 4,
    substr(shipped_ymd,1,4),
    CAST(CAST(substr(shipped_ymd,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  shipped_ymd AS date,
  normal_item_sales_amount,
  limited_item_sales_amount
FROM
  tmp_reporting_004
ORDER BY
  date