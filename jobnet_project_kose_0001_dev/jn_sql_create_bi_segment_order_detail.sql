-- @TD engine_version: 350
/* 重複と注文毎の最新レコードのみとする抽出サブクエリ（既存のcommon_segment_0003参照）*/
WITH tmp_distinct_order_detail AS (
  SELECT
      DISTINCT order_code
    , order_detail_code
    , sku_code
    , sku_name
    , item_code
    , item_name
    , quantity
    , returned_quantity
    , price_ex_vat
    , price_in_vat
    , price_std_ex_vat
    , price_std_in_vat
    , jan_code
    , product_code
    , variation_code
    , size_code
    , size
    , color_code
    , color
    , addon_services
    , amount_sku_ex_vat
    , amount_sku_in_vat
    , amount_addon_service_ex_vat
    , amount_addon_service_in_vat
    , selling_price_ex_vat
    , selling_price_in_vat
    , lang
    , points
    , order_type
    , periodical_item
    , set_item
    , item_type
    , status
    , product_type
    , kose_product
    , user_order_detail_type
    , create_date
    , update_date
    , create_user
    , update_user
    , filecreatedate
  FROM kosedmp_prd_secure.order_detail main
  WHERE
    EXISTS(
      SELECT 1
      FROM bi_segment_order sub
        WHERE
          main.order_code = sub.order_code
        AND
          main.filecreatedate = sub.filecreatedate
    )
)

/* bi_segment_order_detailテーブル作成のメインクエリ */
SELECT
    tdod.*
  , CASE WHEN bso.system_code = 'E' THEN --- Eの中で、
      CASE WHEN scim.daihan_class_name = 'フローラノーティス' THEN 'D' --フローラノーティスを購入している
      ELSE 'E' END --ジルスチュアート・その他を購入している
    WHEN bso.system_code = 'F' THEN 'F' -- Fの人はFのまま
    WHEN bso.system_code = 'G' THEN 'G' -- Gの人はGのまま
    WHEN bso.system_code = 'J' THEN 'J'  -- Jの人はJのまま
    ELSE NULL END AS td_system_code
  , bso.customer_code_hash
  , bso.checkout_date
  , bso.shipped_date
FROM
  tmp_distinct_order_detail tdod
INNER JOIN
  bi_segment_order bso
ON
  tdod.order_code = bso.order_code
LEFT JOIN
  kosedmp_prd_secure.segment_common_item_mst scim
ON
  tdod.sku_code = scim.hinmoku_cd