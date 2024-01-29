-- @TD engine_version: 350
SELECT
  order_code
  , customer_code_hash
  , registered_flag
  , status
  , sex
  , birthday
  , lang
  , zip_code
  , country
  , state
  , customer_currency_code
  , timezone
  , company
  , department
  , ec_category1
  , ec_category2
  , ec_lank
  , kpadclientclass
  , age
  , order_type
  , payment_executor
  , auto_allocation_flag
  , status_locked_flag
  , currency_code
  , store_code
  , point_details
  , delivery_info_code
  , delivery_address_lang
  , delivery_address_zip_code
  , delivery_address_country
  , delivery_address_state
  , delivery_address_company
  , delivery_address_department
  , delivery_wish_timestamp
  , delivery_plan_term
  , mailbox_flag
  , sku_ex_vat
  , sku_in_vat
  , addon_service_ex_vat
  , addon_service_in_vat
  , discount_ex_vat
  , discount_in_vat
  , point_ex_vat
  , point_in_vat
  , adjustment_ex_vat
  , adjustment_in_vat
  , delivery_fee_ex_vat
  , delivery_fee_in_vat
  , total_ex_vat
  , total_in_vat
  , discount_details
  , payment_type
  , user_invoices_included_flag
  , user_peyment_type
  , user_peyment_method
  , cart_code
  , periodical_purchase_codes
  , receipt_type
  , payment_expiry_timestamp
  , checkout_timestamp
  , accept_target_timestamp
  , accepted_timestamp
  , paid_timestamp
  , shipped_timestamp
  , arrived_timestamp
  , canceled_timestamp
  , shipment_notified_flag
  , order_addon_services
  , user_order_type
  , user_order_status
  , order_remarks
  , confirm_timestamp
  , shipping_designation_plan_timestamp
  , shipping_designation_timestamp
  , sales_varification_timestamp
  , tax_rate
  , sender_zip_code
  , sender_country
  , sender_state
  , sender_company
  , sender_department
  , gift
  , order_terminal
  , order_method
  , original_order_code
  , return_date
  , return_pattern
  , refund_method
  , receipt_number
  , distributor_code
  , sales_staff_code
  , sales_category
  , deal_type
  , import_bill_code
  , datasource
  , create_date
  , update_date
  , create_user
  , update_user
  , filecreatedate
  , derivation_order_info
  , attributes_media_code
FROM kosedmp_prd_secure."order" main
WHERE
  status = 'VALID'
  AND canceled_timestamp = ''
  AND user_order_type <> 'RETURN_ORDER'
  AND filecreatedate = (
    SELECT
      MAX(sub.filecreatedate)
    FROM kosedmp_prd_secure."order" sub
    WHERE sub.order_code = main.order_code
  )