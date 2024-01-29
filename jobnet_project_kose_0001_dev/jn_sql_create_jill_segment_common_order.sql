-- @TD engine_version: 350
SELECT
    A.order_code
  , A.customer_code_hash
  , A.registered_flag
  , A.status
  , A.sex
  , A.birthday
  , A.lang
  , A.zip_code
  , A.country
  , A.state
  , A.customer_currency_code
  , A.timezone
  , A.company
  , A.department
  , A.ec_category1
  , A.ec_category2
  , A.ec_lank
  , A.kpadclientclass
  , A.age
  , A.order_type
  , A.payment_executor
  , A.auto_allocation_flag
  , A.status_locked_flag
  , A.currency_code
  , A.store_code
  , A.point_details
  , A.delivery_info_code
  , A.delivery_address_lang
  , A.delivery_address_zip_code
  , A.delivery_address_country
  , A.delivery_address_state
  , A.delivery_address_company
  , A.delivery_address_department
  , A.delivery_wish_timestamp
  , A.delivery_plan_term
  , A.mailbox_flag
  , A.sku_ex_vat
  , A.sku_in_vat
  , A.addon_service_ex_vat
  , A.addon_service_in_vat
  , A.discount_ex_vat
  , A.discount_in_vat
  , A.point_ex_vat
  , A.point_in_vat
  , A.adjustment_ex_vat
  , A.adjustment_in_vat
  , A.delivery_fee_ex_vat
  , A.delivery_fee_in_vat
  , A.total_ex_vat
  , A.total_in_vat
  , A.discount_details
  , A.payment_type
  , A.user_invoices_included_flag
  , A.user_peyment_type
  , A.user_peyment_method
  , A.cart_code
  , A.periodical_purchase_codes
  , A.receipt_type
  , A.payment_expiry_timestamp
  , A.checkout_timestamp
  , A.accept_target_timestamp
  , A.accepted_timestamp
  , A.paid_timestamp
  , A.shipped_timestamp
  , A.arrived_timestamp
  , A.canceled_timestamp
  , A.shipment_notified_flag
  , A.order_addon_services
  , A.user_order_type
  , A.user_order_status
  , A.order_remarks
  , A.confirm_timestamp
  , A.shipping_designation_plan_timestamp
  , A.shipping_designation_timestamp
  , A.sales_varification_timestamp
  , A.tax_rate
  , A.sender_zip_code
  , A.sender_country
  , A.sender_state
  , A.sender_company
  , A.sender_department
  , A.gift
  , A.order_terminal
  , A.order_method
  , A.original_order_code
  , A.return_date
  , A.return_pattern
  , A.refund_method
  , A.receipt_number
  , A.distributor_code
  , A.sales_staff_code
  , A.sales_category
  , A.deal_type
  , A.import_bill_code
  , A.datasource
  , A.create_date
  , A.update_date
  , A.create_user
  , A.update_user
  , A.filecreatedate
FROM kosedmp_prd_secure."order" A
  INNER JOIN kosedmp_prd_secure.segment_common_after_regist B
  ON A.customer_code_hash = B.customer_code_hash
  AND B.system_code IN ('E') -- DAC_KOSE_DMP-52 20191017現在、Eのみ。変更予定： ('E', 'B')

WHERE
-- 有効な注文
  A.status = 'VALID'
-- キャンセルされていない
  AND A.canceled_timestamp = ''
  AND A.shipped_timestamp <> ''
  AND (
-- 出荷日が2019-05-26まではECbeing切り替え前のため「RETURN_ORDER」は抽出対象外
    (
      TD_TIME_RANGE(CAST(A.shipped_timestamp AS bigint)/ 1000,
        NULL,
        '2019-05-27',
      'JST')
      AND A.user_order_type <> 'RETURN_ORDER'
    )
    OR
-- 出荷日が2019-05-27以降で「NORMAL_ORDER」のもの
    (
      TD_TIME_RANGE(CAST(A.shipped_timestamp AS bigint)/ 1000,
        '2019-05-27',
        NULL,
      'JST')
      AND A.user_order_type = 'NORMAL_ORDER'
    )
    OR
-- 返品日が2019-05-27以降で「RETURN_ORDER」のもの
    (
      A.return_date <> ''
      AND 
      TD_TIME_RANGE(CAST(A.return_date AS bigint)/ 1000,
        '2019-05-27',
        NULL,
      'JST')
      AND A.user_order_type = 'RETURN_ORDER'
    )
  )
-- ファイル作成日が注文番号内で最新のもの
  AND A.filecreatedate = (
    SELECT
      MAX(C.filecreatedate)
    FROM kosedmp_prd_secure."order" C
    WHERE C.order_code = A.order_code
  )