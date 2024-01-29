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
  kosedmp_prd_secure.line_friend
WHERE client_id = 'd5e615216fc42c1c'