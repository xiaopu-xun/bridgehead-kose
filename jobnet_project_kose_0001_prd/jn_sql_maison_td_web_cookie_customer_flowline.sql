WITH td_web_cookie_medium AS(
-- 期間・サイトを限定 カラム限定・追加
SELECT
  time
  ,td_ssc_id
  ,hash_customer_code
  ,td_url
  -- 新規作成カラム
  ,TD_TIME_FORMAT(td_web_cookie.time,'yyyy/MM/dd','JST') as cookie_time
  ,CASE
      WHEN REGEXP_LIKE(td_referrer, 'mail.yahoo|mail.google|android.gm') THEN 'email'
      WHEN REGEXP_LIKE(td_referrer, 'yahoo|google|bing|search.auone|search.smt.docomo') THEN 'organic search'
      WHEN REGEXP_LIKE(td_referrer, 'cpc|ppc|paid') THEN 'paid search'
      WHEN REGEXP_LIKE(td_referrer, 'line|facebook|fbclid|mkfb|twitter|t.co|mktw|instagram') THEN 'sns'
      WHEN REGEXP_LIKE(td_referrer, 'monipla|my-best') OR REGEXP_LIKE(td_url,'affiliate') THEN 'affiliates'
      WHEN REGEXP_LIKE(td_referrer, 'display|cpm|banner') THEN 'display'
      WHEN REGEXP_LIKE(td_referrer, 'kose') OR td_referrer = '' THEN 'direct'
      WHEN td_referrer <> '' THEN 'referral'
      ELSE 'other' END
    AS medium
FROM td_web_cookie
WHERE
  --maison kose へのアクセスに限定、及び、日付範囲指定
  TD_TIME_RANGE(td_web_cookie.time,
    TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
    'JST')
  AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
)

/*** <master_maison_td_web_cookie作成> ***/
,master_temporary_table AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY time) AS row_number
  FROM td_web_cookie
  LIMIT 9
)
,master_temporary_medium AS (
  SELECT
    CASE
        WHEN row_number % 9 = 0 THEN 'organic search'
        WHEN row_number % 9 = 1 THEN 'paid search'
        WHEN row_number % 9 = 2 THEN 'sns'
        WHEN row_number % 9 = 3 THEN 'referral'
        WHEN row_number % 9 = 4 THEN 'direct'
        WHEN row_number % 9 = 5 THEN 'email'
        WHEN row_number % 9 = 6 THEN 'affiliates'
        WHEN row_number % 9 = 7 THEN 'display'
        WHEN row_number % 9 = 8 THEN 'other' END
      AS medium
  FROM master_temporary_table
)

,master_maison_td_web_cookie AS(
  select
    distinct t.cookie_time as cookie_time
    ,m.medium
  from 
    td_web_cookie_medium t
    ,master_temporary_medium m
  order by t.cookie_time ,m.medium
)
/*** </ master_maison_td_web_cookie作成> ***/

/*** <PV/UU数> ***/
,pv_uu AS(
SELECT
  cookie_time
  , medium
  , COUNT(*) AS pv_num -- "PV数"
  , COUNT(DISTINCT td_ssc_id) AS user_num -- "サイトUU数"
FROM td_web_cookie_medium
GROUP BY
  cookie_time
  ,medium
order by
  cookie_time
  ,medium
)
/*** </PV数> ***/


/*** <ページ別訪問数> ***/

/*** セッションフラグ生成 ***/
,datum_access_flg AS (
  SELECT 
    medium,
    td_ssc_id,
    time,
    cookie_time,
    td_url,
    --前回アクセス時間
    COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time,
    --前回アクセス時間との差
    time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval,
    --前回アクセス時間との差が30分以上か(30分以上を1セッションとする（セッションの始まり）)
    CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
  FROM
    td_web_cookie_medium
  -- ORDER BY td_ssc_id,time
)

/** ユーザーセッションキー生成 **/
,datum_access_session AS (
  SELECT
    *,
    -- td_ssc_id + session_flg でユーザーセッションキーを生成（同一ユーザー・同一セッション内アクセスで 重複値）
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg
)

/*** セッション内最小time,セッション内連番追加 ***/
,datum_access_session_with_min_time AS(
  SELECT
    *,
    MIN(time) OVER (PARTITION BY user_session ORDER BY time) AS min_time,
    ROW_NUMBER() OVER (PARTITION BY user_session ORDER BY time) AS session_row_number
  FROM
    datum_access_session
)

/*** セッション内最小timeに限定 ***/
,datum_access_session_only_min_time AS(
  SELECT 
    *
  FROM
    datum_access_session_with_min_time
  WHERE 
    --ユーザーセッションキーごとの最小timeを設定
    time = min_time
    --同一キーで重複がある場合は先頭1件に絞る
    AND session_row_number = 1
)

/*** 同一セッション内のURLアクセス重複排除***/
,tmp_distinct_url as( 
select
  cookie_time,
  user_session,
  td_url
from datum_access_session
group by  cookie_time, user_session,td_url
-- order by cookie_time, user_session, td_url
)

/*** セッション内重複URL排除後、同一セッションの最小timeのメディアを付与 ***/
,tmp_distinct_url_with_session_medium as( 
select
  a.*
  , b.medium as medium
from tmp_distinct_url a
  inner join datum_access_session_only_min_time b 
  on a.user_session = b.user_session
-- order by a.cookie_time, a.user_session, a.td_url   desc
)

/*** 日付/メディアでグループ集計 ***/
,session_by_page AS (
select
  cookie_time
  , medium
  , count(td_url)  as session_by_page -- "ページ別訪問数"
from tmp_distinct_url_with_session_medium
  group by cookie_time,medium
)
/*** </ページ別訪問数> ***/


/*** <平均滞在時間> ***/

/** セッションフラグ作成 datum_access_flg  **/
/** ユーザーセッションキー生成 datum_access_session **/
/** ユーザーセッションキー から 直帰の排除（0秒ではセッション時間を計算できないため） **/
,datum_access_flg_exc_bounce AS (
  select
    *
    from datum_access_session
    WHERE
      --接続時間0(直帰)を除く
      access_interval <> 0
)

/** セッション情報生成 **/
,session_info AS(
  SELECT
    cookie_time,
    user_session,
    MAX(time) - MIN(time) AS use_time
  FROM
    datum_access_flg_exc_bounce
  GROUP BY
    cookie_time,
    user_session
  -- order by cookie_time, user_session
)

/*** セッション情報生成後、同一セッション内の最小timeのメディアを付与 ***/
,session_info_with_session_medium as( 
select
  a.*
  ,b.medium as medium
from session_info a
  inner join datum_access_session_only_min_time b 
  on a.user_session = b.user_session
order by a.cookie_time, a.user_session, medium
)

,session_ave_stay AS (
SELECT 
  cookie_time
  , medium
  , AVG(use_time) AS session_ave_stay -- "セッションの平均滞在時間(秒)"
FROM
  session_info_with_session_medium
GROUP BY cookie_time, medium
)
/*** </平均滞在時間> ***/

/*** <Session初回訪問数> ***/
-- /** セッションフラグ作成_リリース時点から **/
,datum_access_flg_from_release AS (
  SELECT 
    td_ssc_id
    ,td_url
    ,time
    -- 前回アクセス時間との差が30分以上か
    ,CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
    ,CASE
      WHEN REGEXP_LIKE(td_referrer, 'mail.yahoo|mail.google|android.gm') THEN 'email'
      WHEN REGEXP_LIKE(td_referrer, 'yahoo|google|bing|search.auone|search.smt.docomo') THEN 'organic search'
      WHEN REGEXP_LIKE(td_referrer, 'cpc|ppc|paid') THEN 'paid search'
      WHEN REGEXP_LIKE(td_referrer, 'line|facebook|fbclid|mkfb|twitter|t.co|mktw|instagram') THEN 'sns'
      WHEN REGEXP_LIKE(td_referrer, 'monipla|my-best') OR REGEXP_LIKE(td_url,'affiliate') THEN 'affiliates'
      WHEN REGEXP_LIKE(td_referrer, 'display|cpm|banner') THEN 'display'
      WHEN REGEXP_LIKE(td_referrer, 'kose') OR td_referrer = '' THEN 'direct'
      WHEN td_referrer <> '' THEN 'referral'
      ELSE 'other' END
    AS medium
  FROM
    td_web_cookie 
  WHERE
    --maison kose へのアクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE(td_web_cookie.time,
    '2019-11-08', -- from(この値を含む)【固定】
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
    'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  -- ORDER BY
  --   td_ssc_id,
  --   td_url,
  --   time
)

,datum_access_session_from_release AS(
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg_from_release
)

,datum_access_session_from_release_with_min_time AS(
  SELECT
    *,
    --ユーザーセッションキーごとの最小timeを設定
    MIN(time) OVER (PARTITION BY user_session ORDER BY time) AS min_time,
    --同一セッションキーになる場合の判別キーを設定
    ROW_NUMBER() OVER (PARTITION BY user_session ORDER BY time) AS session_row_number
  FROM
    datum_access_session_from_release
)

,datum_access_session_from_release_only_min_time AS(
  SELECT 
    *
  FROM
    datum_access_session_from_release_with_min_time
  WHERE
    --ユーザーセッションキーごとの最小timeを設定
    time = min_time
    --同一キーで重複がある場合は先頭1件に絞る
    AND session_row_number = 1
)

,datum_access_session_fromTo_designation AS (
  SELECT *
  FROM  datum_access_session_from_release_only_min_time
    WHERE
    --maison kose へのアクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE(datum_access_session_from_release_only_min_time.min_time,
      TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
      TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
)

/*** Session初回訪問数 ***/
,first_session_num AS (
SELECT 
  TD_TIME_FORMAT(min_time,'yyyy/MM/dd','JST') as cookie_time
  , medium
  , count(distinct (case when session_flg=0 then user_session end)) as first -- "FIRST"
FROM
  datum_access_session_fromTo_designation
GROUP BY TD_TIME_FORMAT(min_time,'yyyy/MM/dd','JST'), medium
)
/*** </Session初回訪問数> ***/

select
  CAST(m.cookie_time AS varchar) as cookie_time
  ,CAST(m.medium AS varchar) as "source/medium"
  ,CAST(m.medium AS varchar) as medium
  ,CASE WHEN pu.pv_num IS NULL THEN 0 ELSE pu.pv_num END
  AS pv_num -- "PV数"
  ,CASE WHEN sbp.session_by_page IS NULL THEN 0 ELSE sbp.session_by_page END
  AS session_num_by_page -- "ページ別訪問数"
  ,CASE WHEN pu.user_num IS NULL THEN 0 ELSE pu.user_num END
  AS user_num -- "UU数"
  ,CASE WHEN fsn.first IS NULL THEN 0 ELSE fsn.first END
  AS first_session_num -- "初回訪問数（新規ユーザー）"
  ,CASE WHEN sas.session_ave_stay IS NULL THEN 0 ELSE sas.session_ave_stay END
  AS ave_use_time -- "セッションの平均滞在時間(秒)"

from master_maison_td_web_cookie m
  LEFT JOIN pv_uu pu
  ON m.cookie_time = pu.cookie_time AND m.medium = pu.medium
  LEFT JOIN session_by_page sbp
  ON m.cookie_time = sbp.cookie_time AND m.medium = sbp.medium
  LEFT JOIN first_session_num fsn
  ON m.cookie_time = fsn.cookie_time AND m.medium = fsn.medium
  LEFT JOIN session_ave_stay sas
  ON m.cookie_time = sas.cookie_time AND m.medium = sas.medium

ORDER BY cookie_time, medium;