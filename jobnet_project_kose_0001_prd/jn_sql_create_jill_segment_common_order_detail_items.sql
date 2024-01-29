-- jn_job_jill_limititem_0001_prd を参考に、
-- jn_job_jill_segment_0002_prd の 「セット品分解版order_detail」 を作成。
-- jill_segment_common_order_detail run後に動くことを想定。

-- セット品を分解してアイテム単位にする
-- 単品をUNION
WITH set_item_separate_with AS(

  SELECT
    * 
  FROM
    -- セット品
    ( 
      SELECT
          A.item_code
        , A.component_serial_number
        , A.component_code
        , A.component_name
        , B.hinmoku_cd
        , B.cut_syohin_name
        , B.limited_kbn
        , B.price as component_item_price 
      FROM
        segment_common_set_mst A 
        INNER JOIN segment_common_item_mst B 
          ON A.component_code = B.hinmoku_cd 
      WHERE
        B.daihan_class_name = 'ジルスチュアート' 
      ORDER BY
        1, 2
    ) 
  UNION ALL
   -- 単品
   ( 
    SELECT
        NULL AS item_code
      , NULL AS component_serial_number
      , NULL AS component_code
      , NULL AS component_name
      , hinmoku_cd
      , cut_syohin_name
      , limited_kbn
      , price as component_item_price 
    FROM
      segment_common_item_mst 
    WHERE
      daihan_class_name = 'ジルスチュアート' 
      AND hinmoku_cd not in( select item_code from segment_common_set_mst) -- セットでないもの
    ORDER BY
      1, 2
  ) 
  ORDER BY
    1, 2, 3, 4
)
-- オーダー集計

-- 単品
SELECT
    A.order_code
  , A.order_detail_code
  , A.sku_code
  , A.sku_name
  , D.hinmoku_cd      AS item_code -- 単品
  , D.cut_syohin_name AS item_name -- 単品
  , A.quantity
  , A.returned_quantity
  , A.price_ex_vat
  , A.price_in_vat
  , A.price_std_ex_vat
  , A.price_std_in_vat
  , A.jan_code
  , A.product_code
  , A.variation_code
  , A.size_code
  , A.size
  , A.color_code
  , A.color
  , A.addon_services
  , A.quantity * A.price_ex_vat AS amount_sku_ex_vat
  , A.amount_sku_in_vat
  , A.amount_addon_service_ex_vat
  , A.amount_addon_service_in_vat
  , A.selling_price_ex_vat
  , A.selling_price_in_vat
  , A.lang
  , A.points
  , A.order_type
  , A.periodical_item
  , A.set_item
  , A.item_type
  , A.status
  , A.product_type
  , A.kose_product
  , A.user_order_detail_type
  , A.create_date
  , A.update_date
  , A.create_user
  , A.update_user
  , A.filecreatedate
  , D.limited_kbn --限定品区分
FROM jill_segment_common_order_detail as A
INNER JOIN set_item_separate_with D -- セット分解ITEM
  ON D.item_code IS NULL            -- セットでないもの
 AND D.hinmoku_cd = A.sku_code      -- セットでないものは単品紐付け

-- INNER JOIN OR より UNION の方が早い
UNION ALL

-- セット品は分解品を見る
SELECT
    A.order_code
  , A.order_detail_code
  , A.sku_code
  , A.sku_name
  , D.component_code  AS item_code -- 単品
  , D.component_name  AS item_name -- 単品
  , A.quantity
  , A.returned_quantity
  , D.component_item_price AS price_ex_vat --単体金額(税抜)
  , A.price_in_vat
  , A.price_std_ex_vat
  , A.price_std_in_vat
  , A.jan_code
  , A.product_code
  , A.variation_code
  , A.size_code
  , A.size
  , A.color_code
  , A.color
  , A.addon_services
  , A.quantity * D.component_item_price AS amount_sku_ex_vat
  , A.amount_sku_in_vat
  , A.amount_addon_service_ex_vat
  , A.amount_addon_service_in_vat
  , A.selling_price_ex_vat
  , A.selling_price_in_vat
  , A.lang
  , A.points
  , A.order_type
  , A.periodical_item
  , A.set_item
  , A.item_type
  , A.status
  , A.product_type
  , A.kose_product
  , A.user_order_detail_type
  , A.create_date
  , A.update_date
  , A.create_user
  , A.update_user
  , A.filecreatedate
  , D.limited_kbn --限定品区分
FROM jill_segment_common_order_detail as A
INNER JOIN set_item_separate_with D -- セット分解ITEM
  ON D.item_code IS NOT NULL
 AND D.item_code = A.sku_code       -- セットはセットマスタ

ORDER BY 1,2,3