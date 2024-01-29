-- @TD engine_version: 350
-- 顧客別の初回購入、２回目購入の一時テーブル作成
-- システムコード、顧客コード、初回購入年月、初回購入日、２回目購入日、２回目までの日数
WITH repeat_tmp_table AS(
SELECT
scar.system_code, -- システムコード
sco.customer_code_hash, -- 顧客コード
firstorder.row_number as row_number_1, -- 初回購入有無
CAST(firstorder.shipped_timestamp AS bigint)/ 1000 as shipped_timestamp_1, -- 初回購入日(timestamp)
TD_TIME_FORMAT(CAST(firstorder.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM', 'JST') as shipped_ym_1, -- 初回購入年月(yyyy-mm)
TD_TIME_FORMAT(CAST(firstorder.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST') as shipped_ymd_1, -- 初回購入日(yyyy-mm-dd)
secondorder.row_number as row_number_2, -- ２回目購入有無
CAST(secondorder.shipped_timestamp AS bigint)/ 1000 as shipped_timestamp_2, -- ２回目購入日(timestamp)
TD_TIME_FORMAT(CAST(secondorder.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST') as shipped_ymd_2, -- ２回目購入年月日(yyyy-mm-dd)
date_diff('day',
 CAST(TD_TIME_FORMAT(CAST(firstorder.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST') AS DATE),
 CAST(TD_TIME_FORMAT(CAST(secondorder.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST') AS DATE)
 ) as diff_days -- 初回から２回購入までの日付(同日は0)

  FROM kosedmp_prd_secure.segment_common_order sco
-- 初回購入情報一時テーブル(firstorder) start
  LEFT JOIN
(
SELECT
  customer_code_hash,
  shipped_timestamp,
  ROW_NUMBER() OVER( PARTITION BY customer_code_hash ORDER BY shipped_timestamp) AS row_number
FROM kosedmp_prd_secure.segment_common_order
) as firstorder
  ON sco.customer_code_hash = firstorder.customer_code_hash
  AND firstorder.row_number = 1
-- 初回購入情報一時テーブル(firstorder) end
-- ２回目購入情報一時テーブル(secondorder) start
  LEFT JOIN
(
SELECT
  customer_code_hash,
  shipped_timestamp,
  ROW_NUMBER() OVER( PARTITION BY customer_code_hash ORDER BY shipped_timestamp) AS row_number
FROM kosedmp_prd_secure.segment_common_order
) as secondorder
  ON sco.customer_code_hash = secondorder.customer_code_hash
  AND secondorder.row_number = 2
-- ２回目購入情報一時テーブル(secondorder) end

  LEFT JOIN kosedmp_prd_secure.segment_common_after_regist scar
  ON sco.customer_code_hash = scar.customer_code_hash

  WHERE
  firstorder.shipped_timestamp <> ''
  AND firstorder.shipped_timestamp IS NOT NULL
  ORDER BY firstorder.shipped_timestamp,diff_days
)

-- システムコード、年月、２回目までの日付毎に情報を取得
SELECT DISTINCT system_code, -- システムコード
  IF(
    CAST(substr(MAX(shipped_ymd_1) OVER (PARTITION BY system_code,shipped_ym_1,diff_days),6,2) AS bigint)  >= 4,
    substr(MAX(shipped_ymd_1) OVER (PARTITION BY system_code,shipped_ym_1,diff_days),1,4),
    CAST(CAST(substr(MAX(shipped_ymd_1) OVER (PARTITION BY system_code,shipped_ym_1,diff_days),1,4) AS bigint) - 1 AS VARCHAR)
  ) AS year, -- 年度
  MAX(shipped_ym_1) OVER (PARTITION BY system_code,shipped_ym_1,diff_days) AS shipped_ym, -- 初回購入年月
  MAX(diff_days) OVER (PARTITION BY system_code,shipped_ym_1,diff_days) AS diff_days, -- 2回目購入までの日付
  COUNT(1) OVER (PARTITION BY system_code,shipped_ym_1,diff_days) AS repeat_count, -- 人数
  COUNT(1) OVER (PARTITION BY system_code,shipped_ym_1) AS month_total_count -- 同月全人数
  FROM repeat_tmp_table
  WHERE TD_TIME_RANGE(shipped_timestamp_1,
        TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
        TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
        'JST')
  AND system_code = 'E'
  ORDER BY system_code,shipped_ym,diff_days