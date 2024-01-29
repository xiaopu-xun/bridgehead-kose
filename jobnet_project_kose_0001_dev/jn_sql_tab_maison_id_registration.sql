-- @TD engine_version: 350
WITH
first_order AS(
  SELECT
  A.customer_code_hash,
  FIRST_VALUE(order_code) OVER (PARTITION BY A.customer_code_hash ORDER BY A.checkout_timestamp) as order_code
  FROM kosedmp_prd_secure.segment_common_order A
  LEFT JOIN kosedmp_prd_secure.segment_common_after_regist B
  ON A.customer_code_hash = B.customer_code_hash
  WHERE B.system_code= 'F'
  AND B.systemcreatedate <> ''
  AND A.checkout_timestamp <> ''
  AND
   --登録日付日付範囲指定
  TD_TIME_RANGE((CAST(B.systemcreatedate AS BIGINT) / 1000),
    '2019-11-08', -- from(この値を含む)【固定】
    NULL, -- to(この値を含まない)
    'JST')
  GROUP BY A.customer_code_hash,A.checkout_timestamp,A.order_code
),
--マイハダ購入注文番号作成
maihada_order AS(
  SELECT A.order_code
  FROM kosedmp_prd_secure.segment_common_order_detail A 
  LEFT JOIN kosedmp_prd_secure.segment_common_order B
  ON A.order_code = B.order_code
  LEFT JOIN kosedmp_prd_secure.segment_common_item_mst C
  ON A.sku_code = C.hinmoku_cd
  WHERE C.hanbai_class_name = 'マイハダ'
  GROUP BY A.order_code
),

-- ここから集計
 maihada_no_change_table_today AS (
  /***1:会員数 米肌移行会員かつパスワード変更されていない(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '1' AS customer_flg,
    COUNT(1) AS num
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- 「登録日 ＜ '2019/10/25' 」かつ「最終ログイン情報 ＜ '2019/11/08'」(つまりアクセス情報なし)
  system_code = 'F'
  AND ar.systemcreatedate <> ''
  AND TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
      NULL, -- from(この値を含む)
      '2019-10-25', -- to(この値を含まない)
      'JST'
      )
  -- アクセス無し
  AND NOT EXISTS
     (
       SELECT * FROM kosedmp_prd_secure.td_web_cookie twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
       AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
       AND twc.hash_customer_code <> ''
       AND twc.hash_customer_code IS NOT NULL
       AND ar.customer_code_hash = twc.hash_customer_code
     )

UNION ALL

  /***1:会員数 米肌移行会員かつパスワード変更されていない(昨日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '1' AS customer_flg,
    COUNT(1) * -1 AS num
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- 「登録日 ＜ '2019/10/25' 」かつ「最終ログイン情報 ＜ '2019/11/08'」(つまりアクセス情報なし)
  system_code = 'F'
  AND ar.systemcreatedate <> ''
  AND TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
      NULL, -- from(この値を含む)
      '2019-10-25', -- to(この値を含まない)
      'JST'
      )
  -- アクセス無し
  AND NOT EXISTS
     (
       SELECT * FROM kosedmp_prd_secure.td_web_cookie twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
       AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
       AND twc.hash_customer_code <> ''
       AND twc.hash_customer_code IS NOT NULL
       AND ar.customer_code_hash = twc.hash_customer_code
     )
),

maihada_change_table_today AS (
/***2:会員数 米肌移行会員かつパスワード変更されている(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '2' AS customer_flg,
    COUNT(1) AS num
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- (「登録日 ＜ '2019/10/25' 」かつ「最終アクセス日 ≧ '2019/11/08'」)　ないし　(「'2019/10/25 ≦ 登録日 <'2019/11/8'」)
  system_code = 'F'
  AND ar.systemcreatedate <> ''
  AND
  (
    (
      TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
        NULL, -- from(この値を含む)
        '2019-10-25', -- to(この値を含まない)
        'JST'
      )
      AND
        EXISTS
      (
       SELECT * FROM kosedmp_prd_secure.td_web_cookie twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
       AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
       AND twc.hash_customer_code <> ''
       AND twc.hash_customer_code IS NOT NULL
       AND ar.customer_code_hash = twc.hash_customer_code
       )
    )
    OR
    TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
      '2019-10-25', -- from(この値を含む)
      '2019-11-08', -- to(この値を含まない)
      'JST'
    )
  )

UNION ALL

/***2:会員数 米肌移行会員かつパスワード変更されている(前日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '2' AS customer_flg,
    COUNT(1) * -1 AS num
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- (「登録日 ＜ '2019/10/25' 」かつ「最終アクセス日 ≧ '2019/11/08'」)　ないし　(「'2019/10/25 ≦ 登録日 <'2019/11/8'」)
  system_code = 'F'
  AND ar.systemcreatedate <> ''
  AND
  (
    (
      TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
        NULL, -- from(この値を含む)
        '2019-10-25', -- to(この値を含まない)
        'JST'
      )
      AND
        EXISTS
      (
       SELECT * FROM kosedmp_prd_secure.td_web_cookie twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
       AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
       AND twc.hash_customer_code <> ''
       AND twc.hash_customer_code IS NOT NULL
       AND ar.customer_code_hash = twc.hash_customer_code
       )
    )
    OR
    TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
      '2019-10-25', -- from(この値を含む)
      '2019-11-08', -- to(この値を含まない)
      'JST'
    )
  )

),

customer_first_maihada_table_today AS (
/***3:会員数 新規会員 初回にマイハダ購入(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '3' AS customer_flg,
    COUNT(1) AS num
  FROM kosedmp_prd_secure.segment_common_after_regist A
  WHERE   --アクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      '2019-11-08', -- from(この値を含む)【固定】
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST'
    )
  AND system_code = 'F'
  -- 初回購入時に米肌商品を購入している
  AND EXISTS
    (SELECT * FROM first_order B
     LEFT JOIN maihada_order C
     ON B.order_code = C.order_code
     WHERE A.customer_code_hash = B.customer_code_hash
     AND C.order_code IS NOT NULL)
  AND  TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST'
    )
),

customer_first_not_maihada_table_today AS (
/***4:会員数 新規会員 初回にマイハダ以外購入(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '4' AS customer_flg,
    COUNT(1) AS num
  FROM kosedmp_prd_secure.segment_common_after_regist A
  WHERE   --アクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      '2019-11-08', -- from(この値を含む)【固定】
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST'
    )
  AND system_code = 'F'
  -- 初回購入時に米肌商品を購入している
  AND EXISTS
    (SELECT * FROM first_order B
     LEFT JOIN maihada_order C
     ON B.order_code = C.order_code
     WHERE A.customer_code_hash = B.customer_code_hash
     AND C.order_code IS NULL)
  AND  TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
         TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
         TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
         'JST'
       )
),

customer_not_first_buy_table_today AS (
/***5:会員数 新規会員 未購入(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '5' AS customer_flg,
    COUNT(1) AS num
  FROM kosedmp_prd_secure.segment_common_after_regist A
  WHERE   --アクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      '2019-11-08', -- from(この値を含む)【固定】
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST')
  AND system_code = 'F'
  -- 初回購入時に米肌商品を購入している
  AND NOT EXISTS
    (SELECT * FROM first_order B
     WHERE A.customer_code_hash = B.customer_code_hash
     AND B.order_code IS NOT NULL)
  AND  TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
         TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
         TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
         'JST'
       )
),
-- 集計した情報のサマリー処理
customer_summary_table_today AS (
  SELECT "date",customer_flg,num FROM maihada_no_change_table_today
UNION ALL
  SELECT "date",customer_flg,num FROM maihada_change_table_today
UNION ALL
  SELECT "date",customer_flg,num FROM customer_first_maihada_table_today
UNION ALL
  SELECT "date",customer_flg,num FROM customer_first_not_maihada_table_today
UNION ALL
  SELECT "date",customer_flg,num FROM customer_not_first_buy_table_today
)

SELECT
  "date",
  SUM(CASE WHEN customer_flg = '1' THEN num ELSE 0 END) AS maihada_no_change_num,
  SUM(CASE WHEN customer_flg = '2' THEN num ELSE 0 END) AS maihada_change_num,
  SUM(CASE WHEN customer_flg = '3' THEN num ELSE 0 END) AS first_maihada_num,
  SUM(CASE WHEN customer_flg = '4' THEN num ELSE 0 END) AS first_not_maihada_num,
  SUM(CASE WHEN customer_flg = '5' THEN num ELSE 0 END) AS not_buy_num,
  SUM(num) AS id_num
FROM  customer_summary_table_today
GROUP BY "date"