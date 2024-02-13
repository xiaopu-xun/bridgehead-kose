/*
tmp_bi_coockie_date --coockie情報を取得する途中テーブル
*/

WITH  datum_access_flg_F AS ( --メゾンコーセー

SELECT
	'F' as system_code,
	td_ssc_id,
	time,
	COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time, --前回アクセス時間
	time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval, --前回アクセス時間との差
	CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
		--前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
FROM
	td_web_cookie
WHERE
	TD_TIME_RANGE(time,
	  '${start_date}', -- from(この値を含む)
	  '${end_date}', -- to(この値を含まない)
	  'JST')

AND regexp_extract(td_url,'https://maison.kose.co.jp') IS NOT NULL

ORDER BY
	td_ssc_id, time
),



datum_access_flg_D AS ( --フローラノーティス
SELECT
	'D' as system_code,
	td_global_id,
	time,
	COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time),time) AS prev_time, --前回アクセス時間
	time - COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time), time) AS access_interval, --前回アクセス時間との差
	CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
		--前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
FROM
	td_web_cookie
WHERE
	TD_TIME_RANGE(time,
	  '${start_date}', -- from(この値を含む)
	  '${end_date}', -- to(この値を含まない)
	  'JST')
AND

(
	regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/floranotis/') IS NOT NULL
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/topic/') IS NOT NULL
	OR (
		regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/goods/search.aspx') IS NOT NULL
		AND regexp_extract(td_url,'brand_tree=20') IS NOT NULL
	)
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/customer/bookmark.aspx') IS NOT NULL
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/cart/cart.aspx') IS NOT NULL
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/s/contact.aspx') IS NOT NULL
)

ORDER BY
	td_ssc_id, time
),



datum_access_flg_E AS ( --ジルスチュアート
SELECT
	'E' as system_code,
	td_global_id,
	time,
	COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time),time) AS prev_time, --前回アクセス時間
	time - COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time), time) AS access_interval, --前回アクセス時間との差
	CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
		--前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
FROM
	td_web_cookie
WHERE
	TD_TIME_RANGE(time,
	  '${start_date}', -- from(この値を含む)
	  '${end_date}', -- to(この値を含まない)
	  'JST')
AND
(
	regexp_extract(td_url,'https://www.jillstuart-beauty.com/ja-jp/') IS NOT NULL
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/jillstuart/') IS NOT NULL
	OR (
		regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/goods/search.aspx') IS NOT NULL
		AND regexp_extract(td_url,'brand_tree=10') IS NOT NULL
	)
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/customer/bookmark.aspx') IS NOT NULL
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/cart/cart.aspx') IS NOT NULL
	OR regexp_extract(td_url,'https://www.jillstuart-floranotisjillstuart.com/site/s/contact.aspx') IS NOT NULL
)

ORDER BY
	td_ssc_id, time
),



datum_access_flg_J AS ( --米肌
SELECT
	'J' as system_code,
	td_ssc_id,
	time,
	COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time, --前回アクセス時間
	time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval, --前回アクセス時間との差
	CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
		--前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
FROM
	td_web_cookie
WHERE
	TD_TIME_RANGE(time,
	  '${start_date}', -- from(この値を含む)
	  '${end_date}', -- to(この値を含まない)
	  'JST')
AND regexp_extract(td_url,'https://maihada.jp') IS NOT NULL

ORDER BY
	td_ssc_id, time
),



datum_access_flg_G AS ( --アディクション
SELECT
	'G' as system_code,
	td_ssc_id,
	time,
	COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time, --前回アクセス時間
	time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval, --前回アクセス時間との差
	CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
		--前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
FROM
	td_web_cookie
WHERE
	TD_TIME_RANGE(time,
	  '${start_date}', -- from(この値を含む)
	  '${end_date}', -- to(この値を含まない)
	  'JST')
AND regexp_extract(td_url,'https://www.addiction-beauty.com') IS NOT NULL

ORDER BY
	td_ssc_id, time
),



datum_access_flg_L AS ( --デコルテ
SELECT
	'L' as system_code,
	td_global_id,
	time,
	COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time),time) AS prev_time, --前回アクセス時間
	time - COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time), time) AS access_interval, --前回アクセス時間との差
	CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_global_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
		--前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
FROM
	td_web_cookie
WHERE
	TD_TIME_RANGE(time,
	  '${start_date}', -- from(この値を含む)
	  '${end_date}', -- to(この値を含まない)
	  'JST')
AND regexp_extract(td_url,'https://www.decorte.com') IS NOT NULL

ORDER BY
	td_global_id, time
),




/** ユーザーセッションキー生成 **/
datum_access_session AS (

SELECT
   	*,  --td_ssc_id + session_flg でユーザーセッションキーを生成
	CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS user_session
FROM
	datum_access_flg_F
WHERE
   access_interval <> 0 --接続時間0(直帰)を除く

UNION ALL

SELECT
  	*,  --td_global_id + session_flg でユーザーセッションキーを生成
	CONCAT(td_global_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_global_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS user_session
FROM
	datum_access_flg_D
WHERE
  access_interval <> 0 --接続時間0(直帰)を除く

UNION ALL

SELECT
   	*,  --td_global_id + session_flg でユーザーセッションキーを生成
	CONCAT(td_global_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_global_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS user_session
FROM
	datum_access_flg_E
WHERE
   access_interval <> 0 --接続時間0(直帰)を除く

UNION ALL

SELECT
  	*,  --td_ssc_id + session_flg でユーザーセッションキーを生成
	CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS user_session
FROM
	datum_access_flg_J
WHERE
  access_interval <> 0 --接続時間0(直帰)を除く


UNION ALL

SELECT
   	*,  --td_ssc_id + session_flg でユーザーセッションキーを生成
	CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS user_session
FROM
	datum_access_flg_G
WHERE
   access_interval <> 0 --接続時間0(直帰)を除く


UNION ALL

SELECT
  	*,  --td_global_id + session_flg でユーザーセッションキーを生成
	CONCAT(td_global_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_global_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS user_session
FROM
	datum_access_flg_L
WHERE
  access_interval <> 0 --接続時間0(直帰)を除く
),


/* あらかじめベースとなるシステムコードと日付の対応テーブルを作成しておく。 */
/* レコードが無い日付・システムコードの組合せもカウント数0として表示できるようにする */
tmp_table AS (
  select * from
  (
    SELECT 'B' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'C' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'D' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'E' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'F' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'G' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'J' AS system_code FROM (SELECT 1)
    UNION ALL
    SELECT 'L' AS system_code FROM (SELECT 1)
  ) as tmp_table1
  ,
  (
    SELECT
      CAST(dt AS VARCHAR) AS date
    FROM
      (SELECT 1)
    CROSS JOIN UNNEST(
      sequence(
        CAST('${start_date}' AS DATE),
        -- DATEADD(DATE, -1, CAST('2021-04-01' AS DATE)),
        CAST(DATE_ADD('DAY', -1, CAST('${end_date}' AS DATE)) AS DATE),
        INTERVAL '1' DAY
      )
    )
    AS t(dt)
  ) as tmp_table2
)

SELECT
	tt.date, --日付
	tt.system_code AS system_code, --利用システムコード
	COUNT(DISTINCT t1.td_ssc_id) AS unique_user, --UU数
	COUNT(t1.td_ssc_id) AS page_view, --PV数
	COUNT(DISTINCT t1.user_session) AS session_cnt --セッション数

FROM
  tmp_table tt
LEFT OUTER JOIN
	datum_access_session t1
ON
  tt.date = TD_TIME_FORMAT(t1.time,'yyyy-MM-dd','JST')
AND
  tt.system_code = t1.system_code

GROUP BY
	tt.date,
	tt.system_code

ORDER BY
	tt.date,
	tt.system_code