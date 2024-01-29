SELECT
  hash_customer_code,
  aoneuid,
  td_global_id,
  td_user_agent,
  td_ip,
  td_client_id,
  MAX(time) AS create_ut
FROM
  td_web_cookie
WHERE
  td_client_id != ''
  AND TD_TIME_RANGE(time,
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
GROUP BY
  hash_customer_code,
  aoneuid,
  td_global_id,
  td_user_agent,
  td_ip,
  td_client_id