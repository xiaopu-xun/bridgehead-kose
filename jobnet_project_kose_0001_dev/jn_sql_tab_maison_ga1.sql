-- @TD engine_version: 350
WITH datum_access_flg AS (
  SELECT
    hash_customer_code,
    td_ssc_id,
    time,
    --前回アクセス時間
    COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time),time) AS prev_time,
    --前回アクセス時間との差
    time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time) AS access_interval,
    --前回アクセス時間との差が30分以上か(30分以上を1セッションとする)
    CAST(((time - COALESCE(LAG(time) OVER (PARTITION BY td_ssc_id ORDER BY time), time)) > 30 * 60) AS INT) AS session_flg
  FROM
    kosedmp_prd_secure.td_web_cookie td_web_cookie
  WHERE
    TD_TIME_RANGE(td_web_cookie.time,
      TD_TIME_FORMAT(${range.from}, 'yyyy-MM-dd', 'JST'),
      TD_TIME_FORMAT(${range.to}, 'yyyy-MM-dd', 'JST'),
      'JST')
    AND regexp_extract(td_url,'maison.kose.co.jp') IS NOT NULL
  ORDER BY
    td_ssc_id,time
),
/** ユーザーセッションキー生成 **/
datum_access_session AS (
  SELECT
    *,
    --td_ssc_id + session_flg でユーザーセッションキーを生成
    CONCAT(td_ssc_id,'_',CAST(SUM(session_flg) OVER (PARTITION BY td_ssc_id ORDER BY time, session_flg DESC rows unbounded preceding) AS varchar)) AS   user_session
  FROM
    datum_access_flg
)
SELECT
    TD_TIME_FORMAT(
      ${range.from},
      'yyyy/MM/dd',
      'JST'
    ) AS "date"
  , COUNT(DISTINCT das.td_ssc_id) as user_num
  , COUNT(DISTINCT CASE WHEN scar.time IS NULL THEN das.td_ssc_id END) as non_member_num
  , COUNT(DISTINCT CASE WHEN scar.time IS NOT NULL THEN das.td_ssc_id END) as connect_member_num
FROM datum_access_session das
LEFT JOIN kosedmp_prd_secure.segment_common_after_regist scar
  ON das.hash_customer_code = scar.customer_code_hash
  AND scar.system_code = 'F'
  AND scar.status = 'VALID'