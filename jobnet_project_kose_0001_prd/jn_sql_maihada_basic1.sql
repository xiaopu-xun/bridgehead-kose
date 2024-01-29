SELECT
    order_date
  , order_code
  , order_detail_code
  , customer_code
  , category
  , product_name
  , order_method
  , trial_type
  , coupon_code
  , age
  , subscription_flg
  , first_buy_flg
  , auto_buy_flg
  , honpin_flg
  , price
  , quantity
FROM
  tmp_maihada_reporting_basic1
ORDER BY
  1,2,3