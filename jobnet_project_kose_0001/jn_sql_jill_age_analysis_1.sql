WITH order_with AS ( 
  SELECT
    AR.system_code,                            -- 利用システムコード
    OD.amount_sku_ex_vat, 
    OD.user_order_detail_type,                 -- 購買明細.注文タイプ
    CASE 
      WHEN OD.user_order_detail_type = 'RETURN_ORDER' THEN 
        CASE WHEN O.return_date = '' THEN TD_TIME_FORMAT(CAST(O.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
        ELSE TD_TIME_FORMAT(CAST(O.return_date AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
        END
      WHEN OD.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(O.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
    END AS shipped_ymd,                        -- 出荷日(yyyy-mm-dd)
    date_diff('year', CAST( from_unixtime(CAST(AR.birthday AS bigint) / 1000, 'Asia/Tokyo') AS DATE) , CURRENT_DATE) as age
  FROM
    jill_segment_common_order_detail_items OD 
    LEFT JOIN jill_segment_common_order O 
      ON OD.order_code = O.order_code 
    LEFT JOIN segment_common_after_regist AR 
      ON O.customer_code_hash = AR.customer_code_hash 
  WHERE
    O.shipped_timestamp <> ''
    AND AR.birthday <> ''
    AND OD.amount_sku_ex_vat IS NOT NULL
    AND TD_TIME_RANGE(CAST(O.shipped_timestamp AS bigint) / 1000,
--       TD_TIME_FORMAT(1517410800, 'yyyy-MM-dd', 'JST'),
--       TD_TIME_FORMAT(1582988400,   'yyyy-MM-dd', 'JST'),
       TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
       TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST'),
       'JST'
    ) 
)

-- 日付ゼロ埋め用
, tmp_date_list_table AS(
  SELECT
    CAST(dt AS VARCHAR) as dt
  FROM (SELECT 1)
  CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(1517410800, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
)

SELECT
      'E' as system_code,
      dt as aggregate_day,
      0 as total_amount,
      0 as total_count,
      '0' as age_range
    FROM tmp_date_list_table
UNION ALL
SELECT
 system_code
,shipped_ymd as aggregate_day
      , CAST(SUM(
          CASE 
            WHEN amount_sku_ex_vat IS NULL               THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER' THEN amount_sku_ex_vat * - 1 
            WHEN user_order_detail_type = 'NORMAL_ORDER' THEN amount_sku_ex_vat
            ELSE 0
          END
        ) AS bigint) AS total_amount
,COUNT(age) as total_count
,CASE 
   WHEN age >= 0 and age < 10  THEN '1'
   WHEN age >= 10 and age < 20 THEN '2'
   WHEN age >= 20 and age < 30 THEN '3'
   WHEN age >= 30 and age < 40 THEN '4'
   WHEN age >= 40 and age < 50 THEN '5'
   WHEN age >= 50 and age < 60 THEN '6'
   WHEN age >= 60              THEN '7'
   ELSE '0'
 END AS age_range
FROM 
 order_with
GROUP BY system_code, shipped_ymd, age