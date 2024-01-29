-- @TD engine_version: 350
SELECT 
  COUNT(filecreatedate) AS count
FROM
  kosedmp_prd_secure.periodical_purchase
WHERE
  filecreatedate = TD_TIME_FORMAT(TD_DATE_TRUNC('day',
      TD_SCHEDULED_TIME(),
--      1595293200,
--dev環境でcount=0にならない日付を入れた(20200605)
      'JST'),
    'yyyyMMdd',
    'JST')