WITH birthday_tbl AS (
SELECT
 system_code,
 systemcreatedate,
 CASE WHEN birthday <> '' then date_diff('year', CAST( from_unixtime(CAST(birthday AS bigint) / 1000, 'Asia/Tokyo') AS DATE) , CURRENT_DATE) 
 ELSE NULL END as age
FROM
  segment_common_after_regist
WHERE
  systemcreatedate <> ''
  AND systemcreatedate IS NOT NULL
  AND TD_TIME_RANGE(CAST(systemcreatedate AS bigint) / 1000,
    NULL,
    TD_TIME_FORMAT(TD_DATE_TRUNC('day',
        TD_SCHEDULED_TIME(),
        'JST'),
      'yyyy-MM-dd',
      'JST'),
    'JST')
  AND  status <> 'RESIGNED'
),
age_range_tbl AS (
SELECT
 system_code,
 systemcreatedate,
 CASE 
   WHEN age >= 0 and age < 10  THEN '1'
   WHEN age >= 10 and age < 20 THEN '2'
   WHEN age >= 20 and age < 30 THEN '3'
   WHEN age >= 30 and age < 40 THEN '4'
   WHEN age >= 40 and age < 50 THEN '5'
   WHEN age >= 50 and age < 60 THEN '6'
   WHEN age >= 60              THEN '7'
   ELSE '0'
 END AS age_range
FROM
  birthday_tbl
)

SELECT
  TD_TIME_FORMAT(TD_DATE_TRUNC('day',
        TD_SCHEDULED_TIME(),
        'JST'),
      'yyyy-MM-dd',
      'JST') as date,
 system_code,
 age_range,
 count(age_range) as total_count
FROM age_range_tbl
GROUP BY
 system_code,
 age_range