-- @TD engine_version: 350
-- 作成レコード用のマスタ
WITH age_mst as (
  SELECT
    ROW_NUMBER() OVER (ORDER BY time) AS age
  FROM kosedmp_prd_secure.customer
  LIMIT 200
),
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
  LEFT JOIN segment_common_item_mst C
  ON A.sku_code = C.hinmoku_cd
  WHERE C.hanbai_class_name = 'マイハダ'
  GROUP BY A.order_code
),

-- ここから集計
maihada_no_change_table_today AS (
  /***1:会員数 米肌移行会員かつパスワード変更されていない(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    systemcreatedate,
    '1' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- 「登録日 ＜ '2019/10/25' 」かつ「最終ログイン情報 ＜ '2019/11/08'」(つまりアクセス情報なし)
  system_code = 'F'
  AND ar.status = 'VALID'
  AND ar.systemcreatedate <> ''
  AND TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
      NULL, -- from(この値を含む)
      '2019-10-25', -- to(この値を含まない)
      'JST'
      )
  AND ar.birthday <> ''
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
),
maihada_no_change_table_yesterday AS (
  /***1:会員数 米肌移行会員かつパスワード変更されていない(昨日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    systemcreatedate,
    '11' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- 「登録日 ＜ '2019/10/25' 」かつ「最終ログイン情報 ＜ '2019/11/08'」(つまりアクセス情報なし)
  system_code = 'F'
  AND ar.status = 'VALID'
  AND ar.systemcreatedate <> ''
  AND TD_TIME_RANGE((CAST(ar.systemcreatedate AS BIGINT) / 1000),
      NULL, -- from(この値を含む)
      '2019-10-25', -- to(この値を含まない)
      'JST'
      )
  AND ar.birthday <> ''
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
    systemcreatedate,
    '2' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- (「登録日 ＜ '2019/10/25' 」かつ「最終アクセス日 ≧ '2019/11/08'」)　ないし　(「'2019/10/25 ≦ 登録日 <'2019/11/8'」)
  system_code = 'F'
  AND ar.status = 'VALID'
  AND ar.systemcreatedate <> ''
  AND ar.birthday <> ''
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
),

maihada_change_table_yesterday AS (
/***2:会員数 米肌移行会員かつパスワード変更されている(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    systemcreatedate,
    '12' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist ar
  WHERE
  -- (「登録日 ＜ '2019/10/25' 」かつ「最終アクセス日 ≧ '2019/11/08'」)　ないし　(「'2019/10/25 ≦ 登録日 <'2019/11/8'」)
  system_code = 'F'
  AND ar.status = 'VALID'
  AND ar.systemcreatedate <> ''
  AND ar.birthday <> ''
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
    systemcreatedate,
    '3' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist A
  WHERE   --アクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      '2019-11-08', -- from(この値を含む)【固定】
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST'
    )
  AND A.system_code = 'F'
  AND A.status = 'VALID'
  AND A.birthday <> ''
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
    systemcreatedate,
    '4' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist A
  WHERE   --アクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      '2019-11-08', -- from(この値を含む)【固定】
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST'
    )
  AND A.system_code = 'F'
  AND A.status = 'VALID'
  AND A.birthday <> ''
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
    systemcreatedate,
    '5' AS customer_flg,
    sex,
    birthday
  FROM kosedmp_prd_secure.segment_common_after_regist A
  WHERE   --アクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE((CAST(A.systemcreatedate AS BIGINT) / 1000),
      '2019-11-08', -- from(この値を含む)【固定】
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST')
  AND A.system_code = 'F'
  AND A.status = 'VALID'
  AND A.birthday <> ''
  -- 初回購入時をしていない
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
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM maihada_no_change_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM maihada_no_change_table_yesterday
UNION ALL
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM maihada_change_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM maihada_change_table_yesterday
UNION ALL
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM customer_first_maihada_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM customer_first_not_maihada_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg,sex,birthday FROM customer_not_first_buy_table_today
),

-- 誕生日から年齢へ計算、性別を集計用の文字列へ変換
"convert" AS (
  SELECT
    "date"
    ,customer_flg
    ,CASE sex
        WHEN 'M' THEN '男性'
        WHEN 'F' THEN '女性'
        ELSE 'その他' END -- 性別未選択？
    AS sex
    ,ROUND(
        (
          CAST(TD_TIME_FORMAT(${range.from}, 'yyyyMMdd') AS integer) -
          CAST(TD_TIME_FORMAT(CAST(birthday AS bigint) / 1000, 'yyyyMMdd', 'JST') as integer)
        ) / 10000
      ) AS age
  FROM customer_summary_table_today

  UNION ALL
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    '0' AS customer_flg,
     CASE
        WHEN age / 2 % 2 = 0 THEN '男性'
        WHEN age / 2 % 2 = 1 THEN '女性' END
      AS sex
    , age / 2 AS age
  FROM age_mst
),
-- 計算した年齢を年代に変換
convert_age AS (
  SELECT
    "date"
    ,customer_flg
    ,sex
    , CASE
        WHEN age BETWEEN 15 AND 17 THEN '15～17歳'
        WHEN age BETWEEN 18 AND 19 THEN '18～19歳'
        WHEN age BETWEEN 20 AND 24 THEN '20～24歳'
        WHEN age BETWEEN 25 AND 29 THEN '20代後半'
        WHEN age BETWEEN 30 AND 34 THEN '30代前半'
        WHEN age BETWEEN 35 AND 39 THEN '30代後半'
        WHEN age BETWEEN 40 AND 44 THEN '40代前半'
        WHEN age BETWEEN 45 AND 49 THEN '40代後半'
        WHEN age BETWEEN 50 AND 54 THEN '50代前半'
        WHEN age BETWEEN 55 AND 59 THEN '50代後半'
        WHEN age BETWEEN 60 AND 99 THEN '60歳以上'
        ELSE 'その他' END
      AS age1
    , CASE
        WHEN age BETWEEN 15 AND 19 THEN '10代'
        WHEN age BETWEEN 20 AND 29 THEN '20代'
        WHEN age BETWEEN 30 AND 39 THEN '30代'
        WHEN age BETWEEN 40 AND 49 THEN '40代'
        WHEN age BETWEEN 50 AND 59 THEN '50代'
        WHEN age BETWEEN 60 AND 99 THEN '60代以上'
        ELSE 'その他' END
      AS age2
  FROM "convert"
)
SELECT
  "date",
  sex,
  age1,
  age2,
  
  SUM(CASE WHEN customer_flg = '1' THEN 1 WHEN customer_flg = '11' THEN -1 ELSE 0 END) AS maihada_no_change_num,
  SUM(CASE WHEN customer_flg = '2' THEN 1 WHEN customer_flg = '12' THEN -1 ELSE 0 END) AS maihada_change_num,
  SUM(CASE WHEN customer_flg = '3' THEN 1 ELSE 0 END) AS first_maihada_num,
  SUM(CASE WHEN customer_flg = '4' THEN 1 ELSE 0 END) AS first_not_maihada_num,
  SUM(CASE WHEN customer_flg = '5' THEN 1 ELSE 0 END) AS not_buy_num,
  SUM(
    CASE
     WHEN customer_flg = '1' OR customer_flg = '2' OR customer_flg = '3' OR customer_flg = '4' OR customer_flg = '5' THEN 1
     WHEN customer_flg = '11' OR customer_flg = '12' THEN -1
     ELSE 0 END
  ) AS membership_num
FROM  convert_age
GROUP BY "date",sex,age1,age2
ORDER BY "date",sex,age1,age2