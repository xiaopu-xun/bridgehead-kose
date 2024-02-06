WITH regulary_tmp_table AS(
SELECT
  C.system_code,
  A.amount_sku_ex_vat,
  A.user_order_detail_type,
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN CAST(B.return_date AS bigint)/ 1000
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN CAST(B.shipped_timestamp AS bigint)/ 1000
  END AS shipped_timestamp, -- 出荷日(timestamp)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(B.return_date AS bigint)/ 1000, 'yyyy-MM', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(B.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM', 'JST')
  END AS shipped_ym, -- 出荷日(yyyy-mm)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(B.return_date AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(B.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
  END AS shipped_ymd -- 出荷日(yyyy-mm-dd)

FROM
  jill_segment_common_order_detail_items A
  LEFT JOIN jill_segment_common_order B
  ON A.order_code = B.order_code

  LEFT JOIN segment_common_after_regist C
  ON B.customer_code_hash = C.customer_code_hash

WHERE
  B.shipped_timestamp <> ''
  AND TD_TIME_RANGE(CAST(B.shipped_timestamp AS bigint)/ 1000,
    TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
    'JST')
), tmp_date_list_table AS(
 SELECT
 CAST(dt AS VARCHAR) as dt
 FROM  (SELECT 1)
 CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
)

SELECT
  -- system_code,
  shipped_ymd,
  SUM(total_amount) AS total_amount
  FROM (
  SELECT
    -- system_code,
    shipped_ymd,
    CAST(
      SUM(CASE
            WHEN user_order_detail_type = 'RETURN_ORDER' THEN amount_sku_ex_vat * -1
            WHEN user_order_detail_type = 'NORMAL_ORDER' THEN amount_sku_ex_vat
          END
         )
      AS integer) AS total_amount,
    user_order_detail_type
    FROM regulary_tmp_table
    GROUP BY system_code,shipped_ymd,user_order_detail_type

  UNION ALL

  SELECT
    -- 'E' as system_code,
    dt as shipped_ymd,
    0 as total_amount,
    '' as user_order_detail_type
  FROM tmp_date_list_table
  )
-- GROUP BY system_code,shipped_ymd
-- ORDER BY system_code,shipped_ymd
GROUP BY shipped_ymd
ORDER BY shipped_ymd