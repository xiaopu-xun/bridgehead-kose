-- 実行時点のメルマガ登録数
WITH num_with AS (
  SELECT
    TD_TIME_FORMAT(${put_date}, 'yyyy-MM-dd', 'JST') AS date,
    count(1)                            AS num
  FROM
    segment_common_after_regist
  WHERE
    system_code            = 'E'
    AND ablemail           <> '0'
    AND mailmagazine_jill  = '1'
)


SELECT
  'E' AS system_code, --tableauで不使用
  IF(
    CAST(substr(date,6,2) AS bigint)  >= 4,
    substr(date,1,4),
    CAST(CAST(substr(date,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,         --tableauで不使用
  date,
  num
FROM
  num_with