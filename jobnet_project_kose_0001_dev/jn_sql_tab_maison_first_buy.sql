-- @TD engine_version: 350
/** セッション条件設定 **/
WITH
master_temporary_table AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY time) AS row_number
  FROM kosedmp_prd_secure.segment_common_after_regist
  LIMIT 9
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
    , 0 AS initial_num
    , CASE
        WHEN (row_number - 1) % 4 IN(2) THEN 1 ELSE 0 END
      AS login_flg
    , CASE
        WHEN (row_number - 1) % 4 IN(3) THEN 1 ELSE 0 END
      AS action_flg
    , 0 AS membership
  FROM master_temporary_table
),
 datum_access_flg AS (
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
    kosedmp_prd_secure.td_web_cookie td_web_cookie
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
),
session_referrer_table AS(
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg
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
maison_ssc_id_to_customr_code AS(
  SELECT
    hash_customer_code as customer_code_hash,
    td_ssc_id
  FROM
    (SELECT
      hash_customer_code,
      td_ssc_id,
      ROW_NUMBER() OVER (PARTITION BY hash_customer_code ORDER BY time DESC) AS row_number
    FROM
      kosedmp_prd_secure.td_web_cookie
    WHERE
      --hash_customer_codeおよびkarte_idが入力有、及び、日付範囲指定
      hash_customer_code <> ''
      AND hash_customer_code IS NOT NULL
      AND td_ssc_id <> ''
      AND td_ssc_id IS NOT NULL
      AND
      TD_TIME_RANGE(td_web_cookie.time,
        '2019-11-08', -- from(この値を含む)
        NULL, -- to(この値を含まない)
        'JST')
      AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
    )
  --timeで並び替えた先頭行のみ使用する
  WHERE row_number = 1
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
FROM session_referrer_table_only_min_time A
LEFT JOIN maison_ssc_id_to_customr_code B
ON A.td_ssc_id = B.td_ssc_id

),
first_order AS(
  SELECT
  customer_code_hash,
  checkout_timestamp
  FROM
  (
  SELECT
    customer_code_hash,
    CAST(segment_common_order.checkout_timestamp AS bigint)/ 1000 AS checkout_timestamp,
    ROW_NUMBER() OVER (PARTITION BY customer_code_hash ORDER BY time DESC) AS row_number
  FROM  kosedmp_prd_secure.segment_common_order segment_common_order
    WHERE segment_common_order.checkout_timestamp <> ''
    AND segment_common_order.checkout_timestamp IS NOT NULL
  )
  WHERE row_number = 1

),

customer_data_table AS (
SELECT
  conductoer_td_scc_id_table.conductor AS conductor,
  count(1) AS initial_num
  FROM kosedmp_prd_secure.segment_common_after_regist segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
    LEFT JOIN first_order
    ON segment_common_after_regist.customer_code_hash = first_order.customer_code_hash
    AND TD_TIME_RANGE(CAST(first_order.checkout_timestamp AS bigint),
          TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'),
          TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'),
          'JST'
        )
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND first_order.customer_code_hash IS NOT NULL
  GROUP BY conductoer_td_scc_id_table.conductor
)

SELECT
  maison_customer_master.date AS date
  , maison_customer_master.conductor AS conductor
  , CASE WHEN customer_data_table.initial_num IS NULL THEN 0 ELSE customer_data_table.initial_num END
      AS initial_num
FROM maison_customer_master
LEFT JOIN customer_data_table
ON maison_customer_master.conductor = customer_data_table.conductor