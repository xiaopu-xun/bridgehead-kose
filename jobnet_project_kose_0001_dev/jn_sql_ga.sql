-- @TD engine_version: 350
SELECT
  transaction_id,
  medium,
  source,
  source_medium,
  channel_grouping,
  date,
  view_id
FROM
  kosedmp_prd_secure.ga_info
WHERE
  TD_TIME_RANGE(TD_TIME_PARSE(CAST(date_parse(date,
          '%Y%m%d') AS VARCHAR),
      'JST'),
    TD_TIME_FORMAT(TD_TIME_ADD(TD_DATE_TRUNC('day',
          TD_SCHEDULED_TIME(),
          'JST'),
        '-1d',
        'JST'),
      'yyyy-MM-dd',
      'JST'),
    TD_TIME_FORMAT(TD_DATE_TRUNC('day',
        TD_SCHEDULED_TIME(),
        'JST'),
      'yyyy-MM-dd',
      'JST'),
    'JST')