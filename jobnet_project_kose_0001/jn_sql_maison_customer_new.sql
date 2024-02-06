/** セッション条件設定 **/
WITH
master_temporary_table AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY time) AS row_number
  FROM segment_common_after_regist
  LIMIT 36
),
maison_customer_master AS (
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS date
    , CASE
        WHEN row_number % 9 = 0 THEN 'organic search'
        WHEN row_number % 9 = 1 THEN 'paid search'
        WHEN row_number % 9 = 2 THEN 'sns'
        WHEN row_number % 9 = 3 THEN 'referral'
        WHEN row_number % 9 = 4 THEN 'direct'
        WHEN row_number % 9 = 5 THEN 'email'
        WHEN row_number % 9 = 6 THEN 'affiliates'
        WHEN row_number % 9 = 7 THEN 'display'
        WHEN row_number % 9 = 8 THEN 'other' END
      AS conductor
    , CASE
        WHEN (row_number - 1) % 4 IN(1) THEN 1 ELSE 0 END
      AS access_flg
    , CASE
        WHEN (row_number - 1) % 4 IN(2) THEN 1 ELSE 0 END
      AS login_flg
    , CASE
        WHEN (row_number - 1) % 4 IN(3) THEN 1 ELSE 0 END
      AS action_flg
    , 0 AS membership
  FROM master_temporary_table
),
session_referrer_table AS(
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    tab_maison_customer_new_prov1 datum_access_flg
),
session_referrer_table_with_min_time AS(
  SELECT
    *,
    --ユーザーセッションキーごとの最小timeを設定
    MIN(time) OVER (PARTITION BY user_session ORDER BY time) AS min_time,
    --同一セッションキーになる場合の判別キーを設定
    ROW_NUMBER() OVER (PARTITION BY user_session ORDER BY time) AS session_row_number
  FROM
    session_referrer_table
),
session_referrer_table_only_min_time AS(
  SELECT
    *
  FROM
    session_referrer_table_with_min_time
  WHERE
    --ユーザーセッションキーごとの最小timeを設定
    time = min_time
    --同一キーで重複がある場合は先頭1件に絞る
    AND session_row_number = 1
),
-- 登録完了のユーザのtd_ssc_idを取得
entrycomplete_customer_table AS
(
  SELECT
    user_session
  FROM
    session_referrer_table_with_min_time
  WHERE
    regexp_extract(td_url,'https://maison.kose.co.jp/site/customer/entrycomplete.aspx') IS NOT NULL

  ORDER BY
    user_session
),
session_conductor_table AS(
  SELECT
    *
  FROM
    session_referrer_table_only_min_time
  WHERE
    --ユーザーセッションキーごとの最小timeを設定
    session_referrer_table_only_min_time.user_session IN
     (SELECT user_session FROM entrycomplete_customer_table)
),
conductoer_td_scc_id_table AS(
/*** 登録時の流入元計測 ***/
SELECT
   CASE
      WHEN REGEXP_LIKE(A.td_referrer, 'mail.yahoo|mail.google|android.gm') THEN 'email'
      WHEN REGEXP_LIKE(A.td_referrer, 'yahoo|google|bing|search.auone|search.smt.docomo') THEN 'organic search'
      WHEN REGEXP_LIKE(A.td_referrer, 'cpc|ppc|paid') THEN 'paid search'
      WHEN REGEXP_LIKE(A.td_referrer, 'line|facebook|fbclid|mkfb|twitter|t.co|mktw|instagram') THEN 'sns'
      WHEN REGEXP_LIKE(A.td_referrer, 'monipla|my-best') OR REGEXP_LIKE(A.td_url,'affiliate') THEN 'affiliates'
      WHEN REGEXP_LIKE(A.td_referrer, 'display|cpm|banner') THEN 'display'
      WHEN REGEXP_LIKE(A.td_referrer, 'kose') OR A.td_referrer = '' THEN 'direct'
      WHEN A.td_referrer <> '' THEN 'referral'
      ELSE 'other' END
    AS conductor,
    B.customer_code_hash,
    A.td_ssc_id,
    A.time AS entry_time,
    A.td_url,
    A.td_referrer
FROM session_conductor_table A
LEFT JOIN tab_maison_customer_new_prov2 B
ON A.td_ssc_id = B.td_ssc_id

),
last_order AS(
  SELECT
    customer_code_hash
  FROM  segment_common_order
    WHERE segment_common_order.checkout_timestamp <> ''
    AND segment_common_order.checkout_timestamp IS NOT NULL
    AND  (CAST(segment_common_order.checkout_timestamp AS bigint)/ 1000) > TD_TIME_ADD(${range.from}, '-90d')
  GROUP BY customer_code_hash
),

-- ここまでデータ準備
-- 以下のパターンで作成
-- 1 0 0 当日のアクセス1に該当する数
-- 0 1 0 当日のログイン1に該当する数
-- 0 0 1 当日のアクション1に該当する数
-- 0 0 0 全て0というのは3つのフラグすべてに上記に当てはまらない人ではなく、全体の母数を導線別に単純に設定

customer_data_table AS (
SELECT
  segment_common_after_regist.customer_code_hash,
  conductoer_td_scc_id_table.conductor AS conductor,
  1 AS access_flg,
  0 AS login_flg,
  0 AS action_flg,
  1 AS membership
  FROM segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
    LEFT JOIN tab_maison_customer_new_prov4 last_access_time_cookie
    ON conductoer_td_scc_id_table.td_ssc_id = last_access_time_cookie.td_ssc_id
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND last_access_time_cookie.td_ssc_id IS NOT NULL

UNION ALL

SELECT
  segment_common_after_regist.customer_code_hash,
  conductoer_td_scc_id_table.conductor AS conductor,
  0 AS access_flg,
  1 AS login_flg,
  0 AS action_flg,
  1 AS membership
  FROM segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
    LEFT JOIN tab_maison_customer_new_prov3 last_login_time_cookie
    ON conductoer_td_scc_id_table.td_ssc_id = last_login_time_cookie.td_ssc_id
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND last_login_time_cookie.td_ssc_id IS NOT NULL

UNION ALL

SELECT
  segment_common_after_regist.customer_code_hash,
  conductoer_td_scc_id_table.conductor AS conductor,
  0 AS access_flg,
  0 AS login_flg,
  1 AS action_flg,
  1 AS membership
  FROM segment_common_after_regist
    LEFT JOIN last_order
    ON segment_common_after_regist.customer_code_hash = last_order.customer_code_hash
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND last_order.customer_code_hash IS NOT NULL

UNION ALL

SELECT
  segment_common_after_regist.customer_code_hash,
  conductoer_td_scc_id_table.conductor AS conductor,
  0 AS access_flg,
  0 AS login_flg,
  0 AS action_flg,
  1 AS membership
  FROM segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
),
maison_customer AS (
SELECT
  customer_data_table.customer_code_hash as customer_code_hash,
  maison_customer_master.date AS date
  , maison_customer_master.conductor AS conductor
  , maison_customer_master.access_flg AS access_flg
  , maison_customer_master.login_flg AS login_flg
  , maison_customer_master.action_flg AS action_flg
  , CASE WHEN customer_data_table.membership IS NULL THEN 0 ELSE customer_data_table.membership END
      AS membership
FROM maison_customer_master
LEFT JOIN customer_data_table
ON maison_customer_master.conductor = customer_data_table.conductor
AND maison_customer_master.access_flg = customer_data_table.access_flg
AND maison_customer_master.login_flg = customer_data_table.login_flg
AND maison_customer_master.action_flg = customer_data_table.action_flg
),
----------会員種別用-----------
first_order AS(
  SELECT
  A.customer_code_hash,
  FIRST_VALUE(order_code) OVER (PARTITION BY A.customer_code_hash ORDER BY A.checkout_timestamp) as order_code
  FROM segment_common_order A
  LEFT JOIN segment_common_after_regist B
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
  FROM segment_common_order_detail A 
  LEFT JOIN segment_common_order B
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
    customer_code_hash
  FROM segment_common_after_regist ar
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
       SELECT * FROM tab_maison_customer_new_prov5 twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
       AND ar.customer_code_hash = twc.hash_customer_code
     )
),
maihada_no_change_table_yesterday AS (
  /***1:会員数 米肌移行会員かつパスワード変更されていない(昨日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    systemcreatedate,
    '11' AS customer_flg,
    customer_code_hash
  FROM segment_common_after_regist ar
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
       SELECT * FROM tab_maison_customer_new_prov5 twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
       AND ar.customer_code_hash = twc.hash_customer_code
     )
),

maihada_change_table_today AS (
/***2:会員数 米肌移行会員かつパスワード変更されている(本日集計分) ***/
  SELECT
    TD_TIME_FORMAT(${range.from}, 'yyyy/MM/dd', 'JST') AS "date",
    systemcreatedate,
    '2' AS customer_flg,
    customer_code_hash
  FROM segment_common_after_regist ar
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
       SELECT * FROM tab_maison_customer_new_prov5 twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
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
    customer_code_hash
  FROM segment_common_after_regist ar
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
       SELECT * FROM tab_maison_customer_new_prov5 twc
       WHERE
         TD_TIME_RANGE(twc.time,
           '2019-11-08', -- from(この値を含む)
           TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
           'JST'
         )
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
    customer_code_hash
  FROM segment_common_after_regist A
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
    customer_code_hash
  FROM segment_common_after_regist A
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
    customer_code_hash
  FROM segment_common_after_regist A
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
customer_summary_table AS (
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM maihada_no_change_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM maihada_no_change_table_yesterday
UNION ALL
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM maihada_change_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM maihada_change_table_yesterday
UNION ALL
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM customer_first_maihada_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM customer_first_not_maihada_table_today
UNION ALL
  SELECT "date",systemcreatedate,customer_flg, customer_code_hash FROM customer_not_first_buy_table_today
)

SELECT 
  maison_customer.date AS date
  , maison_customer.conductor AS conductor
  , maison_customer.access_flg AS access_flg
  , maison_customer.login_flg AS login_flg
  , maison_customer.action_flg AS action_flg
  , SUM(CASE WHEN maison_customer.membership IS NULL THEN 0 ELSE maison_customer.membership END)
      AS membership
  , SUM(CASE WHEN customer_flg = '1' THEN 1 WHEN customer_flg = '11' THEN -1 ELSE 0 END) AS maihada_no_change_num
  , SUM(CASE WHEN customer_flg = '2' THEN 1 WHEN customer_flg = '12' THEN -1 ELSE 0 END) AS maihada_change_num
  , SUM(CASE WHEN customer_flg = '3' THEN 1 ELSE 0 END) AS first_maihada_num
  , SUM(CASE WHEN customer_flg = '4' THEN 1 ELSE 0 END) AS first_not_maihada_num
  , SUM(CASE WHEN customer_flg = '5' THEN 1 ELSE 0 END) AS not_buy_num
FROM maison_customer
LEFT JOIN customer_summary_table
on customer_summary_table.customer_code_hash = maison_customer.customer_code_hash
GROUP BY maison_customer.date, maison_customer.conductor, maison_customer.access_flg, maison_customer.login_flg, maison_customer.action_flg