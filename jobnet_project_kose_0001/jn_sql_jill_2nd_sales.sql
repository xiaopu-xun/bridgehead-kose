SELECT
  first_shipped_month  AS first_shipped_month,
  second_shipped_month AS second_shipped_month,
  customer_count       AS customer_count
FROM
  tmp_reporting_014
ORDER BY
  first_shipped_month,
  second_shipped_month