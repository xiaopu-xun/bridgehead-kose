-- @TD engine_version: 350
/** セッション条件設定 **/
WITH
master_temporary_table AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY time) AS row_number
  FROM kosedmp_prd_secure.segment_common_after_regist
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
    tab_maison_customer_prov1
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
LEFT JOIN tab_maison_customer_prov2 B
ON A.td_ssc_id = B.td_ssc_id

),
last_order AS(
  SELECT
    customer_code_hash
  FROM  kosedmp_prd_secure.segment_common_order segment_common_order
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
  conductoer_td_scc_id_table.conductor AS conductor,
  1 AS access_flg,
  0 AS login_flg,
  0 AS action_flg,
  count(1) AS membership
  FROM kosedmp_prd_secure.segment_common_after_regist segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
    LEFT JOIN tab_maison_customer_prov3
    ON conductoer_td_scc_id_table.td_ssc_id = tab_maison_customer_prov3.td_ssc_id
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND tab_maison_customer_prov3.td_ssc_id IS NOT NULL
  GROUP BY conductoer_td_scc_id_table.conductor

UNION ALL

SELECT
  conductoer_td_scc_id_table.conductor AS conductor,
  0 AS access_flg,
  1 AS login_flg,
  0 AS action_flg,
  count(1) AS membership
  FROM kosedmp_prd_secure.segment_common_after_regist segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
    LEFT JOIN tab_maison_customer_prov4
    ON conductoer_td_scc_id_table.td_ssc_id = tab_maison_customer_prov4.td_ssc_id
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND tab_maison_customer_prov4.td_ssc_id IS NOT NULL
  GROUP BY conductoer_td_scc_id_table.conductor

UNION ALL

SELECT
  conductoer_td_scc_id_table.conductor AS conductor,
  0 AS access_flg,
  0 AS login_flg,
  1 AS action_flg,
  count(1) AS membership
  FROM kosedmp_prd_secure.segment_common_after_regist segment_common_after_regist
    LEFT JOIN last_order
    ON segment_common_after_regist.customer_code_hash = last_order.customer_code_hash
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
    AND last_order.customer_code_hash IS NOT NULL
  GROUP BY conductoer_td_scc_id_table.conductor

UNION ALL

SELECT
  conductoer_td_scc_id_table.conductor AS conductor,
  0 AS access_flg,
  0 AS login_flg,
  0 AS action_flg,
  count(1) AS membership
  FROM kosedmp_prd_secure.segment_common_after_regist segment_common_after_regist
    LEFT JOIN conductoer_td_scc_id_table
    ON segment_common_after_regist.customer_code_hash =  conductoer_td_scc_id_table.customer_code_hash
  WHERE
    segment_common_after_regist.system_code = 'F'
    AND conductoer_td_scc_id_table.td_ssc_id IS NOT NULL
  GROUP BY conductoer_td_scc_id_table.conductor

)

SELECT
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