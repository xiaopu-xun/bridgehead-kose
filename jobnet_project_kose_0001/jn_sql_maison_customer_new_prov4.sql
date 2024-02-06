SELECT
  td_ssc_id
FROM
  (SELECT
    td_ssc_id,
    ROW_NUMBER() OVER (PARTITION BY td_ssc_id ORDER BY time DESC) AS row_number
  FROM
    td_web_cookie
  WHERE
    TD_TIME_RANGE(td_web_cookie.time,
      '2019-11-08', -- from(この値を含む)
      NULL, -- to(この値を含まない)
      'JST')
    AND td_web_cookie.time > TD_TIME_ADD(${range.from}, '-30d')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  )
--timeで並び替えた先頭行のみ使用する
WHERE row_number = 1