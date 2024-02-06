SELECT
  IF(
    CAST(substr(aggregate_day,6,2) AS bigint)  >= 4,
    substr(aggregate_day,1,4),
    CAST(CAST(substr(aggregate_day,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  aggregate_day AS date,
  MAX(CASE traffic_source WHEN 'LINE' THEN sales_amount ELSE 0 END) AS line_sales_amount,
  MAX(CASE traffic_source WHEN 'Mail' THEN sales_amount ELSE 0 END) AS mail_sales_amount,
  MAX(CASE traffic_source WHEN 'Other' THEN sales_amount ELSE 0 END) AS other_sales_amount,
  MAX(CASE traffic_source WHEN 'Direct' THEN sales_amount ELSE 0 END) AS direct_sales_amount,
  MAX(CASE traffic_source WHEN 'Display' THEN sales_amount ELSE 0 END) AS display_sales_amount,
  MAX(CASE traffic_source WHEN 'Organic Search' THEN sales_amount ELSE 0 END) AS organic_search_sales_amount,
  MAX(CASE traffic_source WHEN 'Paid Search' THEN sales_amount ELSE 0 END) AS paid_search_sales_amount,
  MAX(CASE traffic_source WHEN 'Referral' THEN sales_amount ELSE 0 END) AS referral_sales_amount,
  MAX(CASE traffic_source WHEN 'Social' THEN sales_amount ELSE 0 END) AS social_sales_amount
FROM
  tmp_reporting_005
GROUP BY
  aggregate_day
ORDER BY
  date