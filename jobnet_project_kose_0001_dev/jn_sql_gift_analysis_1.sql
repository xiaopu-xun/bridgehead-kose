-- @TD engine_version: 350
WITH order_with AS ( 
  SELECT
    AR.system_code
    , OD.amount_sku_ex_vat
    , OD.user_order_detail_type
    , CASE 
        WHEN OD.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT( CAST(O.return_date       AS bigint) / 1000, 'yyyy-MM-dd', 'JST') 
        WHEN OD.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT( CAST(O.shipped_timestamp AS bigint) / 1000, 'yyyy-MM-dd', 'JST') 
        ELSE null
      END AS shipped_ymd                          -- 出荷日(yyyy-mm-dd)
    , CASE 
        WHEN gsm.sku_code IS NOT NULL                THEN '2'
        WHEN gsm.sku_code IS NULL AND O.gift <> '1'  THEN '1'
        WHEN gsm.sku_code IS NULL AND O.gift = '1'   THEN '3'
        ELSE '0'
      END AS kind
  FROM
    kosedmp_prd_secure.jill_segment_common_order_detail OD 
    LEFT JOIN kosedmp_prd_secure.jill_segment_common_order O
      ON OD.order_code = O.order_code 
    LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AR
      ON O.customer_code_hash = AR.customer_code_hash 
    LEFT JOIN kosedmp_prd_secure.gift_set_mst gsm 
      ON  OD.sku_code     = gsm.sku_code 
      AND OD.jan_code     = gsm.jan_code 
      AND OD.product_code = gsm.product_code 
  WHERE
    O.shipped_timestamp <> '' 
    AND TD_TIME_RANGE( 
      CAST(O.shipped_timestamp AS bigint) / 1000
      , TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST')
      , TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST')
      , 'JST'
    )
) 
, 

-- 日付ゼロ埋め用
tmp_date_list_table AS ( 
  SELECT
    CAST(dt AS VARCHAR) as dt 
  FROM
    (SELECT 1) 
    CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
) 
-- 
SELECT
  -- system_code,
  shipped_ymd AS aggregate_day
  , kind
  , IF ( 
    SUM(total_amount) IS NULL
    , 0
    , CAST(SUM(total_amount) AS bigint)
  ) AS sales_amount 
FROM
  ( 
    SELECT
        system_code
      , shipped_ymd
      , CAST( SUM( 
          CASE 
            WHEN user_order_detail_type = 'RETURN_ORDER' THEN amount_sku_ex_vat * - 1 
            WHEN user_order_detail_type = 'NORMAL_ORDER' THEN amount_sku_ex_vat 
            ELSE 0
          END
        ) AS integer) AS total_amount
      , kind
    FROM order_with
    GROUP BY
      system_code
      , shipped_ymd
      , kind

    UNION ALL

    SELECT
      'E' as system_code
      , dt as shipped_ymd
      , 0 as total_amount
      , NULL AS kind
    FROM
      tmp_date_list_table
  ) 
GROUP BY
--  system_code,
    shipped_ymd
  , kind
ORDER BY
--  system_code,
    shipped_ymd
  , kind