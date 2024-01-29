-- @TD engine_version: 350
SELECT * FROM kosedmp_prd_secure.td_web_cookie twc
WHERE
 TD_TIME_RANGE(twc.time,
   '2019-11-08', -- from(この値を含む)
   NULL, -- to(この値を含まない)
   'JST'
 )
AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
AND twc.hash_customer_code <> ''
AND twc.hash_customer_code IS NOT NULL