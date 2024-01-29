/*** PV,UU数 ***/
WITH pv_uu AS(
SELECT 
  TD_TIME_FORMAT(td_web_cookie.time,'yyyy/MM/dd','JST') as cookie_time
, COUNT(*) AS pv -- "PV数"
, COUNT(DISTINCT td_ssc_id) AS uu -- "サイトUU数"
FROM td_web_cookie 
WHERE 
--maison kose へのアクセスに限定、及び、日付範囲指定
TD_TIME_RANGE(td_web_cookie.time,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
  'JST')
AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
GROUP BY TD_TIME_FORMAT(td_web_cookie.time,'yyyy/MM/dd','JST')
-- ORDER BY cookie_time;
)
,

/*** ページ別訪問数 ***/
datum_access_flg AS (
  SELECT 
    td_ssc_id,
    time,
    td_url,
    --前回アクセス時間
    COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time,
    --前回アクセス時間との差
    time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval,
    --前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
    CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
  FROM
    td_web_cookie 
  WHERE
    --maison kose へのアクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE(td_web_cookie.time,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
    'JST')
  AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  ORDER BY
    td_ssc_id,time
)
,
/** ユーザーセッションキー生成 **/
datum_access_session AS (
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg
)
,
/*** Session数 ***/
tmp_url as(
SELECT
  td_ssc_id,
  TD_TIME_FORMAT(time,'yyyy/MM/dd','JST') as cookie_time,
  user_session,
  td_url
FROM
  datum_access_session
order by 
  td_ssc_id,
  cookie_time,
  user_session,
  td_url
)
,
tmp_distinct_url as( 
select
  cookie_time,
  user_session,
  td_url,
  -- ページごとセッション内PV
  count(td_url) as pv_by_usersession_by_page 
from tmp_url
group by  cookie_time, user_session, td_url
order by cookie_time, user_session, td_url, pv_by_usersession_by_page   desc
)
,
session_by_page AS (
select
  cookie_time
  , count(pv_by_usersession_by_page )  as session_by_page -- "ページ別訪問数"
from tmp_distinct_url
  group by cookie_time

order by cookie_time
)
,


/*** 平均滞在時間 ***/
/** セッション条件設定 **/
datum_access_flg_s AS (
  --td_ssc_idの日付ごとの集計
  SELECT 
  td_ssc_id,
  time,
  --前回アクセス時間
  COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time,
  --前回アクセス時間との差
  time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval,
  --前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
  CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
  FROM td_web_cookie 
  WHERE 
  --maison kose へのアクセスに限定、及び、日付範囲指定
  TD_TIME_RANGE(td_web_cookie.time,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
    'JST')
  AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  ORDER BY td_ssc_id,time
),
/** ユーザーセッションキー生成 **/
datum_access_session_s AS (
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg_s
  WHERE
    --接続時間0(直帰)を除く
    access_interval <> 0
),
/** セッション情報生成 **/
session_info AS(
  SELECT
    TD_TIME_FORMAT(time,'yyyy/MM/dd','JST') as cookie_time,
    user_session,
    MAX(time) - MIN(time) AS use_time
  FROM
    datum_access_session_s
  GROUP BY
    TD_TIME_FORMAT(time,'yyyy/MM/dd','JST'),
    user_session
)
,
tmp_test_ave_stay AS (
SELECT 
  cookie_time
  , AVG(use_time) AS tmp_test_ave_stay -- "セッションの平均滞在時間(秒)"
FROM
  session_info
GROUP BY cookie_time
-- order by cookie_time;
)
,


/*** 会員UU数 ***/
-- ログインユーザ数
user_access_brand_tmp_table AS(
  SELECT 
    TD_TIME_FORMAT(A.time,'yyyy/MM/dd','JST') as cookie_time
    -- 会員情報と紐付け
    , hash_customer_code
  FROM td_web_cookie A
  WHERE 
    TD_TIME_RANGE(
      A.time,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL

    -- ログインしている(=空でない)情報のみ取得
    AND hash_customer_code <> ''

  GROUP BY 
    TD_TIME_FORMAT(A.time,'yyyy/MM/dd','JST')
    ,hash_customer_code
)
,
-- 会員情報
user_with AS (
  SELECT 
    u.customer_code_hash
    
  FROM segment_common_after_regist u
  WHERE   
    u.system_code = 'F'
)
,
uu_user AS (
  SELECT
    c.cookie_time
    , COUNT(1) AS uu_user -- "会員UU数"
  FROM user_access_brand_tmp_table c
  INNER JOIN user_with u
    ON u.customer_code_hash = c.hash_customer_code
  GROUP BY c.cookie_time
  -- ORDER BY cookie_time
)
,


/*** 非会員UU数 ***/
-- 非ログインユーザ数
-- 1日
uu_not_user AS (
SELECT 
  TD_TIME_FORMAT(td_web_cookie.time,'yyyy/MM/dd','JST') as cookie_time
, COUNT(DISTINCT td_ssc_id) AS uu_not_user -- "非会員UU数"
FROM td_web_cookie 
WHERE 
--maison kose へのアクセスに限定、及び、日付範囲指定
TD_TIME_RANGE(td_web_cookie.time,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
    'JST')
AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
-- ログインしている(=空でない)情報のみ取得
AND hash_customer_code = ''
GROUP BY TD_TIME_FORMAT(td_web_cookie.time,'yyyy/MM/dd','JST')
ORDER BY cookie_time
)
,


/*** Session初回訪問/リピート別数 ***/
/** セッション条件設定 **/
datum_access_flg_fr AS (
  SELECT 
    td_ssc_id,
    td_url,
    time,
    --前回アクセス時間との差が30分以上か
    CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
  FROM
    td_web_cookie 
  WHERE
    --maison kose へのアクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE(td_web_cookie.time,
  '2019-11-08', -- from(この値を含む)【固定】
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  ORDER BY
    td_ssc_id,
    td_url,
    time
),
session_referrer_table_fr AS(
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg_fr
),
session_referrer_table_with_min_time AS(
  SELECT
    *,
    --ユーザーセッションキーごとの最小timeを設定
    MIN(time) OVER (PARTITION BY user_session ORDER BY time) AS min_time,
    --同一セッションキーになる場合の判別キーを設定
    ROW_NUMBER() OVER (PARTITION BY user_session ORDER BY time) AS session_row_number
  FROM
    session_referrer_table_fr
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
datum_access_session_fr AS (
  SELECT * 
  FROM  session_referrer_table_only_min_time
    WHERE
    --maison kose へのアクセスに限定、及び、日付範囲指定
    TD_TIME_RANGE(session_referrer_table_only_min_time.min_time,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'), -- from(この値を含む)
  TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'), -- to(この値を含まない)
      'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
)
,
/*** Session初回訪問/リピート別数 ***/
first_repeat AS (
SELECT 
  TD_TIME_FORMAT(min_time,'yyyy/MM/dd','JST') as cookie_time
  , count(distinct (case when session_flg=0 then user_session end)) as first -- "FIRST"
  , count(distinct (case when session_flg>0 then user_session end)) as repeat -- "REPEAT"
FROM
  datum_access_session_fr
GROUP BY TD_TIME_FORMAT(min_time,'yyyy/MM/dd','JST')
ORDER BY cookie_time
)


select
-- pv_uu pu
  pu.cookie_time
  ,pu.pv as pv_num -- "PV数"
  ,pu.uu as user_num -- "UU数"
-- session_by_page
  ,sbp.session_by_page as session_num_by_page -- "ページ別訪問数"
-- tmp_test_ave_stay
  ,ttas.tmp_test_ave_stay as ave_use_time -- "セッションの平均滞在時間(秒)"
-- uu_user
  ,uus.uu_user as connect_member_num -- "会員UU数"
-- uu_not_user
  ,uns.uu_not_user as non_member_num -- "非会員UU数"
-- first_repeat
  ,fr.first as first_session_num -- "初回訪問数"
  ,fr.repeat as repeat_session_num -- "リピート数"

from pv_uu pu
INNER JOIN session_by_page sbp
ON sbp.cookie_time = pu.cookie_time
INNER JOIN tmp_test_ave_stay ttas
ON ttas.cookie_time = pu.cookie_time
INNER JOIN uu_user uus
ON uus.cookie_time = pu.cookie_time
INNER JOIN uu_not_user uns
ON uns.cookie_time = pu.cookie_time
INNER JOIN first_repeat fr
ON fr.cookie_time = pu.cookie_time

ORDER BY cookie_time;