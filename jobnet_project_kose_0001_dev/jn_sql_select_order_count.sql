-- @TD engine_version: 350
SELECT
  COUNT(filecreatedate) AS count
FROM
  kosedmp_prd_secure."order"
WHERE
  filecreatedate = TD_TIME_FORMAT(TD_DATE_TRUNC('day',
      -- TD_SCHEDULED_TIME(),
     1591318800,
--dev環境でcount=0にならない日付を入れた(20200605)
      'JST'),
    'yyyyMMdd',
    'JST')