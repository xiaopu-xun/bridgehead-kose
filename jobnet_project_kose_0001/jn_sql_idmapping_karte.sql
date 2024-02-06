SELECT
  hash_customer_code as customer_code_hash,
  karte_id
FROM
  (SELECT
    hash_customer_code,
    karte_id,
    ROW_NUMBER() OVER (PARTITION BY karte_id ORDER BY time DESC) AS row_number
  FROM
    td_web_cookie
  WHERE
    --hash_customer_codeおよびkarte_idが入力有、及び、日付範囲指定
    hash_customer_code <> ''
    AND hash_customer_code IS NOT NULL
    AND karte_id <> ''
    AND karte_id IS NOT NULL
    AND
    TD_TIME_RANGE(td_web_cookie.time,
      '2019-11-08', -- from(この値を含む)
      NULL, -- to(この値を含まない)
      'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  )
--timeで並び替えた先頭行のみ使用する
WHERE row_number = 1