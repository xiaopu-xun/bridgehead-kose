SELECT
  COUNT(filecreatedate) AS count
FROM
  "order"
WHERE
  filecreatedate = TD_TIME_FORMAT(TD_DATE_TRUNC('day',
      TD_SCHEDULED_TIME(),
      'JST'),
    'yyyyMMdd',
    'JST')