--jn_job_jill_segment_0002_prd より、 「jill_segment_ec_kpad_order」を変更したもの。
SELECT
  DISTINCT
    A.order_code
  , A.order_detail_code
  , A.sku_code
  , A.sku_name
  , A.item_code
  , A.item_name
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
  , A.amount_sku_ex_vat
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
FROM order_detail as A
  LEFT JOIN segment_common_item_mst as B
  ON A.sku_code = B.hinmoku_cd

  LEFT JOIN jill_segment_ec_kpad_order as C
  ON C.order_code = A.order_code
  AND C.filecreatedate = A.filecreatedate

WHERE
-- JILL汎用購買情報に存在する
  C.order_code IS NOT NULL
  AND 
-- 「NORMAL_ORDER」のもの
-- 「RETURN_ORDER」のもの
  (
    A.user_order_detail_type = 'NORMAL_ORDER'
    OR A.user_order_detail_type = 'RETURN_ORDER'
  )
  -- ジルスチュアートのアイテムを購入している
  AND B.daihan_class_name = 'ジルスチュアート'