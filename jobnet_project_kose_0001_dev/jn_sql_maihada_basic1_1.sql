-- @TD engine_version: 350
WITH order_with AS (
  SELECT
    o.customer_code_hash
    -- , TD_TIME_FORMAT(CAST(o.checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM/dd', 'JST') AS order_date
    , o.checkout_timestamp
    , o.order_code
    , od.order_detail_code

      --ユーザごとの購買回数カウント
    , rank() OVER (PARTITION BY o.customer_code_hash ORDER BY o.checkout_timestamp, o.order_code) AS order_count

      --定期orNotごとの購買カウント
    , rank() OVER (PARTITION BY o.customer_code_hash, od.sku_code, eim.subscription_item ORDER BY o.checkout_timestamp, o.order_code) AS subscription_count

    , eim.item_no_2
    , eim.subscription_item
    , eim.category_nm
    , od.price_ex_vat
    , od.quantity
    , od.sku_name
    , o.order_method
    , CASE WHEN ar.birthday='' THEN null ELSE CAST(from_unixtime(CAST(ar.birthday AS bigint) / 1000, 'Asia/Tokyo') AS DATE) END AS birthday

  FROM kosedmp_prd_secure.segment_common_order_detail od
    LEFT JOIN kosedmp_prd_secure.segment_common_order o
      ON o.order_code = od.order_code
    LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst eim
      ON eim.item_cd = od.item_code 
    LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS ar 
      ON ar.customer_code_hash = o.customer_code_hash

  WHERE eim.category_nm LIKE '%米肌%'
    AND (ar.system_code = 'F' OR ar.system_code = 'J')

--  ORDER BY 1,2,3,4,5,6
)

--
SELECT
    TD_TIME_FORMAT(CAST(checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM/dd', 'JST') AS order_date
  , order_code
  , order_detail_code
  , customer_code_hash                                             AS customer_code
  , category_nm                                                    AS category
  , sku_name                                                       AS product_name
  , order_method
  , CASE item_no_2
      WHEN 'PRBQ' THEN '肌潤トライアル'
      WHEN 'PRTG' THEN '肌潤トライアル'
      WHEN 'PRBP' THEN '活潤トライアル'
      WHEN 'PRTI' THEN '活潤トライアル'
      WHEN 'PRTT' THEN '美白トライアル'
      WHEN 'PRTU' THEN '美白トライアル'
      WHEN 'PRLB' THEN 'FDトライアル'
      WHEN 'Z5SPROW' THEN 'オイルモニター'
      ELSE ''
    END                                                            AS trial_type
  , ''                                                             AS coupon_code
  , CASE
      WHEN birthday IS NULL        THEN ''
      WHEN CURRENT_DATE < birthday THEN ''
      ELSE CAST(date_diff('year', birthday, CURRENT_DATE) AS VARCHAR)
    END                                                            AS age
  , IF (subscription_item = '1', 1, 0)                             AS subscription_flg
  , IF (order_count = 1, 1, 0)                                     AS first_buy_flg

  -- 『「subscription_item = '1'(定期購入)」 且つ 「(全購入のうちの初回ではなく、) "定期購入"2回目」以降』
  , IF (subscription_item = '1' AND
        1 < subscription_count, 1, 0)                              AS auto_buy_flg

  , CASE item_no_2
      WHEN 'PRBQ' THEN '0'
      WHEN 'PRTG' THEN '0'
      WHEN 'PRBP' THEN '0'
      WHEN 'PRTI' THEN '0'
      WHEN 'PRTT' THEN '0'
      WHEN 'PRTU' THEN '0'
      WHEN 'PRLB' THEN '0'
      WHEN 'Z5SPROW' THEN '0'
      ELSE '1'
    END                                                            AS honpin_flg
  , price_ex_vat                                                   AS price
  , quantity
FROM
  order_with
WHERE
  -- 最後にSELECTする箇所では期間を絞りたい (https://dac-esys.backlog.jp/view/DAC_KOSE_DMP-53#comment-62791415)
  -- maihada_tableau_report_basic2 にて前年集計がある。「2ヶ月前?現在」と「14ヶ月前?12ヶ月前」のような指定。
  (
  TD_TIME_RANGE(
    CAST(checkout_timestamp AS BIGINT) / 1000
--    , '2018-09-24 00:00:00'
--    , '2018-10-08 23:59:00'
    , TD_TIME_FORMAT(${start_date1}, 'yyyy-MM-dd', 'JST')
    , TD_TIME_FORMAT(${end_date1},   'yyyy-MM-dd', 'JST')
    , 'JST'
  )
  OR
  TD_TIME_RANGE(
    CAST(checkout_timestamp AS BIGINT) / 1000
--    , '2019-09-24 00:00:00'
--    , '2019-10-08 23:59:00'
    , TD_TIME_FORMAT(${start_date2}, 'yyyy-MM-dd', 'JST')
    , TD_TIME_FORMAT(${end_date2},   'yyyy-MM-dd', 'JST')
    , 'JST'
  )
  )

--ORDER BY 1,2,3