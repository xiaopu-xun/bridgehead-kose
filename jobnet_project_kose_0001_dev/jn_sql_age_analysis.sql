-- @TD engine_version: 350
SELECT
  system_code,
    IF(
    CAST(substr(aggregate_day,6,2) AS bigint)  >= 4,
    substr(aggregate_day,1,4),
    CAST(CAST(substr(aggregate_day,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,
  aggregate_day AS date,
  SUM(total_amount) AS total_amount,
  SUM(total_count) AS total_count,
  age_range
FROM
  tmp_reporting_011
GROUP BY
  system_code,
  aggregate_day,
  age_range