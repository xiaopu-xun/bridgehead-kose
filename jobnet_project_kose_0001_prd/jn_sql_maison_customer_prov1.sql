SELECT
  td_ssc_id,
  td_url,
  td_referrer,
  time,
  hash_customer_code,
  --前回アクセス時間
  COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time,
  --前回アクセス時間との差
  time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval,
  --前回アクセス時間との差が30分以上か
  CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
FROM
  td_web_cookie
WHERE
  --maison kose へのアクセスに限定、及び、日付範囲指定
  TD_TIME_RANGE(td_web_cookie.time,
    '2019-11-08', -- from(この値を含む)
    NULL, -- to(この値を含まない)
    'JST')
  AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
ORDER BY
  td_ssc_id,
  td_url,
  td_referrer,
  time