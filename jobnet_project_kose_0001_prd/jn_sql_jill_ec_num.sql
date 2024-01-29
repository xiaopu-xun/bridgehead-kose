-- 登録数
WITH new_with AS (
  SELECT
    TD_TIME_FORMAT( CAST(systemcreatedate AS bigint) / 1000, 'yyyy-MM-dd', 'JST') AS date,
    count(1) AS num -- 当日入会分(入会→退会を同日に行うとステータスがRESIGNEDで入ってくるためステータスは見ない)
  FROM
    segment_common_after_regist
  WHERE
    system_code = 'E'
    AND systemcreatedate <> ''
    AND systemcreatedate IS NOT NULL
    AND TD_TIME_RANGE(
        CAST(systemcreatedate AS bigint) / 1000
      , NULL
      , TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST')
      , 'JST'
    )
  GROUP BY TD_TIME_FORMAT( CAST(systemcreatedate AS bigint) / 1000, 'yyyy-MM-dd', 'JST')
),

-- 退会数
left_with AS (
  SELECT
    TD_TIME_FORMAT( CAST(systemupdatedate AS bigint) / 1000, 'yyyy-MM-dd', 'JST') AS date,
    COUNT(1) AS num
  FROM
    segment_common_after_regist
  WHERE
    system_code = 'E'
    AND  systemupdatedate <> ''
    AND  systemupdatedate IS NOT NULL
    AND  TD_TIME_RANGE(
         CAST(systemupdatedate AS bigint) / 1000,
         NULL ,
         TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
         'JST')
    AND  status = 'RESIGNED'
  GROUP BY TD_TIME_FORMAT( CAST(systemupdatedate AS bigint) / 1000, 'yyyy-MM-dd', 'JST')
),

-- 登録数と退会数をjoin
total_with AS (
  SELECT
    *,
    SUM(membership_num)  OVER (ORDER BY date) AS new_total, --登録総数
    SUM(withdrawals_num) OVER (ORDER BY date) AS left_total --退会総数
  FROM
  (
    SELECT  
      COALESCE(a.date, b.date)      AS date,
      COALESCE(a.num, 0)            AS membership_num,
      COALESCE(b.num, 0)            AS withdrawals_num
    FROM
      new_with a
      FULL JOIN left_with b
      on a.date = b.date
   )
)

SELECT
  'E' AS system_code, --tableauで不使用
  IF(
    CAST(substr(date,6,2) AS bigint)  >= 4,
    substr(date,1,4),
    CAST(CAST(substr(date,1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year,         --tableauで不使用
  date,
  COALESCE(new_total, 0) - COALESCE(left_total, 0) AS num, -- 会員数
  membership_num,
  withdrawals_num
FROM
  total_with
WHERE
  date >= TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') AND
  date <  TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST')
ORDER BY
  date;