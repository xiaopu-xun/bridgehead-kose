-- @TD engine_version: 350
WITH customer_analysis_tmp_table AS(
SELECT
  max(C.system_code) as system_code, -- 利用システムコード
  B.customer_code_hash, -- 顧客コード(ハッシュ)
  B.order_code, -- 注文コード
  max(B.user_order_type) as user_order_type, -- 購買.注文タイプ
  A.user_order_detail_type, -- 購買明細.注文タイプ
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN CAST(max(B.return_date) AS bigint)/ 1000
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN CAST(max(B.shipped_timestamp) AS bigint)/ 1000
  END AS shipped_timestamp, -- 出荷日(timestamp)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(max(B.return_date) AS bigint)/ 1000, 'yyyy-MM', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(max(B.shipped_timestamp) AS bigint)/ 1000, 'yyyy-MM', 'JST')
  END AS shipped_ym, -- 出荷日(yyyy-mm)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(max(B.return_date) AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(max(B.shipped_timestamp) AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
  END AS shipped_ymd, -- 出荷日(yyyy-mm-dd)
  sum(A.amount_sku_ex_vat) as amount_sku_ex_vat

FROM
  kosedmp_prd_secure.jill_segment_common_order_detail_items A
  LEFT JOIN kosedmp_prd_secure.jill_segment_common_order B
  ON A.order_code = B.order_code

  LEFT JOIN kosedmp_prd_secure.segment_common_after_regist C
  ON B.customer_code_hash = C.customer_code_hash

WHERE
  B.shipped_timestamp <> ''
  AND TD_TIME_RANGE(CAST(B.shipped_timestamp AS bigint)/ 1000,
    TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
    'JST')
GROUP BY
  B.customer_code_hash,
  B.order_code,
  A.user_order_detail_type
), customer_count_tmp_table AS(
-- 顧客別,出荷日別,購入回数
SELECT
customer_code_hash,
shipped_ymd,
min(buy_count) AS buy_count
FROM (
  SELECT
  customer_code_hash,
  shipped_ymd,
  SUM(
    CASE
      WHEN user_order_type = 'RETURN_ORDER' THEN -1
      WHEN user_order_type = 'NORMAL_ORDER' THEN 1
    END
  )
  OVER (PARTITION BY customer_code_hash ORDER BY shipped_ymd,user_order_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS buy_count
  FROM customer_analysis_tmp_table
  ORDER BY customer_code_hash
)
GROUP BY customer_code_hash,shipped_ymd
ORDER BY customer_code_hash,shipped_ymd

), tmp_date_list_table AS(
 SELECT
 CAST(dt AS VARCHAR) as dt
 FROM  (SELECT 1)
 CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
)

SELECT
  system_code,
  shipped_ymd,
  SUM(total_amount) AS total_amount,
  SUM(CASE WHEN buy_count <= 1 THEN total_amount ELSE 0 END) AS total_amount_1,
  SUM(CASE WHEN buy_count = 2  THEN total_amount ELSE 0 END) AS total_amount_2,
  SUM(CASE WHEN buy_count = 3  THEN total_amount ELSE 0 END) AS total_amount_3,
  SUM(CASE WHEN buy_count = 4 THEN total_amount ELSE 0 END) AS total_amount_4,
  SUM(CASE WHEN buy_count = 5 THEN total_amount ELSE 0 END) AS total_amount_5,
  SUM(CASE WHEN buy_count = 6 THEN total_amount ELSE 0 END) AS total_amount_6,
  SUM(CASE WHEN buy_count = 7 THEN total_amount ELSE 0 END) AS total_amount_7,
  SUM(CASE WHEN buy_count = 8 THEN total_amount ELSE 0 END) AS total_amount_8,
  SUM(CASE WHEN buy_count = 9 THEN total_amount ELSE 0 END) AS total_amount_9,
  SUM(CASE WHEN buy_count = 10 THEN total_amount ELSE 0 END) AS total_amount_10,
  SUM(CASE WHEN buy_count >= 11 THEN total_amount ELSE 0 END) AS total_amount_11
  FROM (
    SELECT
      B.system_code,
      B.shipped_ymd,
      C.buy_count,
      CAST(
        SUM(CASE
              WHEN B.user_order_detail_type = 'RETURN_ORDER' THEN B.amount_sku_ex_vat * -1
              WHEN B.user_order_detail_type = 'NORMAL_ORDER' THEN B.amount_sku_ex_vat
            END
          )
        AS integer) AS total_amount,
      B.user_order_detail_type
      FROM customer_analysis_tmp_table B
      LEFT JOIN customer_count_tmp_table C
      ON B.customer_code_hash = C.customer_code_hash
      AND B.shipped_ymd = C.shipped_ymd
      GROUP BY B.system_code,B.shipped_ymd,C.buy_count,B.user_order_detail_type

      UNION ALL

      SELECT
        'E' as system_code,
        dt as shipped_ymd,
        0 as buy_count,
        0 as total_amount,
        'NORMAL_ORDER'  as user_order_detail_type

      FROM tmp_date_list_table


  )
WHERE system_code = 'E'
GROUP BY system_code,shipped_ymd
ORDER BY system_code,shipped_ymd