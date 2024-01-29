-- @TD engine_version: 350
SELECT
    order_code
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
    FROM "segment_probance_order" sub
      WHERE
        main.order_code = sub.order_code
      AND
        main.filecreatedate = sub.filecreatedate
  )
  AND(
    main.user_order_detail_type = 'NORMAL_ORDER'
    OR main.user_order_detail_type = 'RETURN_ORDER'
  )