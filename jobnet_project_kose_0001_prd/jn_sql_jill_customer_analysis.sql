SELECT
  IF(
    CAST(substr(shipped_ymd,6,2) AS bigint)  >= 4,
    substr(shipped_ymd,1,4),
    CAST(CAST(substr(shipped_ymd,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  shipped_ymd AS date,
 total_amount_1,
 total_amount_2,
 total_amount_3,
 total_amount_4,
 total_amount_5,
 total_amount_6,
 total_amount_7,
 total_amount_8,
 total_amount_9,
 total_amount_10,
 total_amount_11
FROM
  tmp_reporting_001
ORDER BY
  date