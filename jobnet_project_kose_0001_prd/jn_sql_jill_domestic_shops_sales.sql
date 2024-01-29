-- 得意先コード新旧対比
WITH store_code_with AS (
  SELECT
    DISTINCT main.*
  FROM (
      SELECT
        IF(con.store_id_new IS NULL,mst.store_code,con.store_id_new) AS store_id,
        mst.store_code,mst.type,mst.name,mst.name_abbrev,mst.pref_id
      FROM jill_store_mst mst
        LEFT JOIN store_mst_contrast con
          ON con.store_id_old = mst.store_code
        UNION ALL
      SELECT
        IF(con.store_id_new IS NULL,mst.store_code,con.store_id_new) AS store_id,
        IF(con.store_id_new IS NULL,mst.store_code,con.store_id_new) AS store_code,
        mst.type,mst.name,mst.name_abbrev,mst.pref_id
      FROM jill_store_mst mst
        LEFT JOIN store_mst_contrast con
          ON con.store_id_old = mst.store_code
   ) main ) ,
--ORDER BY 1,2

-- JILL購買データ
order_with AS ( 
  SELECT
    AR.system_code
    , OD.amount_sku_ex_vat
    , OD.user_order_detail_type
    
    -- 出荷日(yyyy-mm-dd)
    , CASE 
        WHEN OD.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(O.return_date AS bigint)       / 1000, 'yyyy-MM-dd', 'JST') 
        WHEN OD.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(O.shipped_timestamp AS bigint) / 1000, 'yyyy-MM-dd', 'JST') 
        ELSE null 
      END AS shipped_ymd
    , O.customer_code_hash
    , p.id as state_cd_ec                         -- O.state : 購入者(ECの場合の発送先(ユーザー))住所（都道府県）
    , O.store_code                                -- 店舗コードより店舗マスタでEC/店舗 を確認。
  FROM
    jill_segment_ec_kpad_order_detail OD 
    LEFT JOIN jill_segment_ec_kpad_order O 
      ON OD.order_code = O.order_code 
    LEFT JOIN segment_common_after_regist AR 
      ON O.customer_code_hash = AR.customer_code_hash 
    LEFT JOIN prefecture_mst p 
      ON p.name = O.state 
  WHERE
    O.shipped_timestamp <> '' 
    AND TD_TIME_RANGE( 
      CAST(O.shipped_timestamp AS bigint) / 1000,
      TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
      TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST'),
      'JST'
    )
    AND NOT (OD.user_order_detail_type = 'RETURN_ORDER' AND O.return_date ='')
) ,

-- 金額算出
total_amount_with AS ( 
  SELECT
    shipped_ymd
    , SUM(total_amount) AS sales_amount
    , state_cd_ec
    , store_code 
    , min(customer_code_hash_count) AS buyer_count -- 購入者数
  FROM
    ( 
      SELECT
        system_code
        , shipped_ymd
        , CAST(SUM( 
            CASE 
              WHEN amount_sku_ex_vat IS NULL               THEN 0 
              WHEN user_order_detail_type = 'RETURN_ORDER' THEN amount_sku_ex_vat * - 1 
              WHEN user_order_detail_type = 'NORMAL_ORDER' THEN amount_sku_ex_vat 
              ELSE 0 
              END
          ) AS bigint
        ) AS total_amount
        , state_cd_ec
        , store_code 
        , COUNT(DISTINCT customer_code_hash) AS customer_code_hash_count
      FROM
        order_with 
      GROUP BY
        system_code
        , shipped_ymd
        , state_cd_ec
        , store_code
    ) 
  GROUP BY
    shipped_ymd
    , state_cd_ec
    , store_code 
--  ORDER BY
--    shipped_ymd
--    , state_cd_ec
--    , store_code
),

-- 店舗情報取得
storeinfo_with AS ( 
  SELECT
    t.shipped_ymd
    , SUM(t.sales_amount) as sales_amount
    , t.state_cd_ec
    , MIN(t.buyer_count) as buyer_count
    , SC.store_id as store_code
    , MIN(SC.name_abbrev) as name_abbrev
    , MIN(SC.type) as type
    , MIN(SC.pref_id) as state_cd_store 
  FROM
    total_amount_with t 
    INNER JOIN store_code_with SC
      ON t.store_code = SC.store_code 
  GROUP BY
    t.shipped_ymd
    , t.state_cd_ec
    , SC.store_id
)

-- 都道府県情報付与
--- 購入が店舗の場合 :店舗の都道府県/
--- 購入がECの場合   :発送先の都道府県
select
  s.date         AS date         ,
  s.sales_amount AS sales_amount ,
  s.buyer_count  AS buyer_count  ,
  s.store_code   AS store_code   ,
  s.store_type   AS store_type   ,
  s.state_code   AS state_code   ,
  p.name         AS state_name
from
  ( 
    select
        shipped_ymd as date
      , store_code
--      , name_abbrev as store_name
      , case type WHEN 1 then '店舗'         ELSE 'EC'        END as store_type
      , case type WHEN 1 then state_cd_store ELSE state_cd_ec END as state_code
      , sum(buyer_count) as buyer_count
      , sum(sales_amount) as sales_amount
    from
      storeinfo_with
    group by 1,2,3,4
  ) s
  LEFT JOIN prefecture_mst p 
    ON p.id = s.state_code 
--order by
--  date
--  , store_code
--  , state_code