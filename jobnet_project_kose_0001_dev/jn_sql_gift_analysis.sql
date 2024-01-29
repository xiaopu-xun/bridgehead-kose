-- @TD engine_version: 350
SELECT
  IF(
    CAST(substr(aggregate_day,6,2) AS bigint)  >= 4,
    substr(aggregate_day,1,4),
    CAST(CAST(substr(aggregate_day,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  aggregate_day AS date,
  MAX(CASE kind WHEN '1' THEN sales_amount ELSE 0 END) AS sales_amount,
  MAX(CASE kind WHEN '2' THEN sales_amount ELSE 0 END) AS gift_set_sales_amount,
  MAX(CASE kind WHEN '3' THEN sales_amount ELSE 0 END) AS gift_wrapping_sales_amount
FROM
  tmp_reporting_002
GROUP BY
  aggregate_day
ORDER BY
  date