-- @TD engine_version: 350
SELECT
  TD_TIME_FORMAT(TD_TIME_ADD(TD_DATE_TRUNC('day',
        TD_SCHEDULED_TIME(),
        'JST'),
      '-1d',
      'JST'),
    'yyyy',
    'JST') AS year,
  TD_TIME_FORMAT(TD_TIME_ADD(TD_DATE_TRUNC('day',
        TD_SCHEDULED_TIME(),
        'JST'),
      '-1d',
      'JST'),
    'yyyy-MM-dd',
    'JST') AS date,
  COUNT(1) AS num
FROM
  kosedmp_prd_secure.segment_common_after_regist
WHERE
  system_code = 'E'
  AND systemcreatedate <> ''
  AND systemcreatedate IS NOT NULL
  AND TD_TIME_RANGE(CAST(systemcreatedate AS bigint) / 1000,
    NULL,
    TD_TIME_FORMAT(TD_DATE_TRUNC('day',
        TD_SCHEDULED_TIME(),
        'JST'),
      'yyyy-MM-dd',
      'JST'),
    'JST')
  AND  status <> 'RESIGNED';