-- @TD engine_version: 350
SELECT
    TD_TIME_FORMAT((CAST(sco.checkout_timestamp AS BIGINT) / 1000), 'yyyy/MM/dd', 'JST') AS "date"
  , 'EC' as order_place
  , REGEXP_EXTRACT(eim.category_nm, '([^|]+)\|.+', 1) AS brand
  , COALESCE(REGEXP_EXTRACT(eim.category_nm, '([^|]+)\|([^|]+)\|?([^|]+)?\|?', 2), '') AS category1
  , COALESCE(REGEXP_EXTRACT(eim.category_nm, '([^|]+)\|([^|]+)\|?([^|]+)?\|?', 3), '') AS category2
  , CASE scod.periodical_item WHEN '1' THEN '0' ELSE '1' END as auto_action_flg
  , CAST(eim.selling_price_inc_tax as double) as unit_price
  , SUM(scod.quantity) as quantity
  , CAST(eim.selling_price_inc_tax as double) * SUM(scod.quantity) AS sales_amount
FROM kosedmp_prd_secure.segment_common_order_detail scod
LEFT JOIN kosedmp_prd_secure.segment_common_order sco
  ON scod.order_code = sco.order_code
LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst eim
  ON scod.item_code = eim.item_cd
LEFT JOIN kosedmp_prd_secure.segment_common_after_regist scar
  ON sco.customer_code_hash = scar.customer_code_hash
  AND scar.system_code = 'F'
WHERE
  sco.checkout_timestamp <> ''
  AND
  TD_TIME_RANGE((CAST(sco.checkout_timestamp AS BIGINT) / 1000),
    TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
    'JST')
  AND eim.item_cd IS NOT NULL
  AND scar.customer_code_hash IS NOT NULL
  AND sco.canceled_timestamp = ''
GROUP BY
  TD_TIME_FORMAT((CAST(sco.checkout_timestamp AS BIGINT) / 1000), 'yyyy/MM/dd', 'JST')
  , REGEXP_EXTRACT(eim.category_nm, '([^|]+)\|.+', 1)
  , COALESCE(REGEXP_EXTRACT(eim.category_nm, '([^|]+)\|([^|]+)\|?([^|]+)?\|?', 2), '')
  , COALESCE(REGEXP_EXTRACT(eim.category_nm, '([^|]+)\|([^|]+)\|?([^|]+)?\|?', 3), '')
  , scod.periodical_item
  , eim.selling_price_inc_tax