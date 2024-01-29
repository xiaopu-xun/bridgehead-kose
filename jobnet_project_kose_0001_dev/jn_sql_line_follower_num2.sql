-- @TD engine_version: 350
-- 会員数
WITH num_with AS (
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST') AS date,
    count(1) as num
  FROM
    kosedmp_prd_secure.line_friend
  WHERE client_id = 'd5e615216fc42c1c'
)


SELECT
  'E' AS system_code, --tableauで不使用
  IF(
    CAST(substr(date,6,2) AS bigint)  >= 4,
    substr(date,1,4),
    CAST(CAST(substr(date,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,        --tableauで不使用
  date,
  num
FROM
  num_with
ORDER BY
  date;