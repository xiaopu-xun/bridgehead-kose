-- @TD engine_version: 350
SELECT
  TD_TIME_FORMAT(CAST(o.checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM', 'JST') AS order_month
  , o.order_code                          AS order_code
  , ''                                    AS solution -- アドエビスからのデータ連携の目処がたっていないため、一旦空。
  , o.order_method                        AS order_method
  , o.customer_code_hash                  AS customer_code
  , CAST(SUM(od.price_ex_vat) AS integer) AS price 
FROM
  kosedmp_prd_secure.segment_common_order_detail od 
  LEFT JOIN kosedmp_prd_secure.segment_common_order o 
    ON o.order_code = od.order_code 
  LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst eim 
    ON eim.item_cd = od.item_code 
  LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS ar 
    ON ar.customer_code_hash = o.customer_code_hash 
WHERE
  eim.category_nm LIKE '%米肌%' 
  AND (ar.system_code = 'F' OR ar.system_code = 'J') 

  AND TD_TIME_RANGE( 
    CAST(o.checkout_timestamp AS BIGINT) / 1000
--  , '2018-09-24 00:00:00'
--  , '2018-10-08 23:59:00'
    , TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST')
    , TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST')
    , 'JST'
  ) 

GROUP BY
  TD_TIME_FORMAT(CAST(o.checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM', 'JST')
  , o.order_code
  , o.order_method
  , o.customer_code_hash
ORDER BY 
  order_month
  , customer_code
  , order_code