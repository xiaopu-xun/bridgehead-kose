-- @TD engine_version: 350
WITH order_with AS (
  SELECT
    AR.system_code
    , OD.amount_sku_ex_vat
    , OD.user_order_detail_type
    -- 出荷日(yyyy-mm-dd)
    , CASE
        WHEN OD.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT( CAST(O.return_date       AS bigint) / 1000, 'yyyy-MM-dd', 'JST')
        WHEN OD.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT( CAST(O.shipped_timestamp AS bigint) / 1000, 'yyyy-MM-dd', 'JST')
        ELSE null
      END AS shipped_ymd

    -- 刻印 は order_detailを見る
    , CASE
        WHEN OD.addon_services IS NOT NULL
         AND OD.addon_services <> '0'
         AND OD.addon_services <> ''
         AND OD.addon_services <> '{"addon_services":null}' THEN CAST(json_parse(replace(substr(OD.addon_services,1,LENGTH(OD.addon_services)-1), '{"addon_services":','')) AS ARRAY<JSON>)
        ELSE NULL
      END AS asJsonRecords_order_detail

    -- ギフトラップ は orderを見る
    , CASE
        WHEN O.order_addon_services IS NOT NULL
         AND O.order_addon_services <> '0'
         AND O.order_addon_services <> ''
         AND O.order_addon_services <> '{"order_addon_services":null}' THEN CAST(json_parse(replace(substr(O.order_addon_services,1,LENGTH(O.order_addon_services)-1), '{"order_addon_services":','')) AS ARRAY<JSON>)
        ELSE NULL
      END AS asJsonRecords_order

    , gsm.sku_code
    , O.gift
    , OD.quantity
    , OD.order_code
    , OD.order_detail_code

  FROM
    kosedmp_prd_secure.jill_segment_common_order_detail_items OD
    LEFT JOIN kosedmp_prd_secure.jill_segment_common_order O
      ON OD.order_code = O.order_code
    LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AR
      ON O.customer_code_hash = AR.customer_code_hash
    LEFT JOIN kosedmp_prd_secure.gift_set_mst gsm
      ON  OD.sku_code     = gsm.sku_code
      AND OD.jan_code     = gsm.jan_code
      AND OD.product_code = gsm.product_code
  WHERE
    O.shipped_timestamp <> ''
    AND TD_TIME_RANGE(
      CAST(O.shipped_timestamp AS bigint) / 1000
      , TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST')
      , TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST')
      , 'JST'
    )
),

addon_services_with AS (
  SELECT
    system_code           ,
    shipped_ymd           ,
    amount_sku_ex_vat     ,
    user_order_detail_type,
    sku_code              ,
    gift                  ,
    quantity              ,
    order_code            ,
    order_detail_code     ,

    -- アドオンサービス名
    CAST(json_extract_scalar(record, '$.addon_service_name') AS VARCHAR) addon_name,
    -- アドオンサービス価格
    CAST(json_extract_scalar(record, '$.price_ex_vat') AS BIGINT) addon_price,
    -- アドオンサービス数量
    CAST(json_extract_scalar(record, '$.quantity') AS BIGINT) addon_quantity,
    -- アドオンサービスインデックス
    CAST(json_extract_scalar(record, '$.index') AS BIGINT) addon_index
  FROM
    order_with
    CROSS JOIN UNNEST(asJsonRecords_order_detail)        AS t(record)
),

order_addon_services_with AS (
  SELECT
    system_code           ,
    shipped_ymd           ,
    amount_sku_ex_vat     ,
    user_order_detail_type,
    sku_code              ,
    gift                  ,
    quantity              ,
    order_code            ,
    order_detail_code     ,

    -- アドオンサービス名
    CAST(json_extract_scalar(record, '$.addon_service_name') AS  VARCHAR)addon_name,
    -- アドオンサービス価格
    CAST(json_extract_scalar(record, '$.price_ex_vat') AS BIGINT) addon_price,
    -- アドオンサービス数量
    CAST(json_extract_scalar(record, '$.quantity') AS BIGINT) addon_quantity,
    -- アドオンサービスインデックス
    CAST(json_extract_scalar(record, '$.index') AS BIGINT) addon_index
  FROM
    order_with
    CROSS JOIN UNNEST(asJsonRecords_order) AS t(record)
),

-- 日付ゼロ埋め用
tmp_date_list_table AS (
  SELECT
    CAST(dt AS VARCHAR) as dt
  FROM
    (SELECT 1)
    CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
),

-- 金額算出
total_amount_with AS (
  SELECT
    system_code,
    shipped_ymd
    , SUM(total_amount)   AS sales_amount
    , SUM(sales_quantity) AS sales_quantity
    , sku_code
    , order_code
    , gift
    , order_detail_code
  FROM
    (
    SELECT
        system_code
      , shipped_ymd
      , CAST( SUM(
          CASE
            WHEN amount_sku_ex_vat IS NULL               THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER' THEN amount_sku_ex_vat * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER' THEN amount_sku_ex_vat
            ELSE 0
          END
        ) AS bigint) AS total_amount
      , CAST( SUM(
          CASE
            WHEN quantity IS NULL                        THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER' THEN quantity * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER' THEN quantity
            ELSE 0
          END
        ) AS bigint) AS sales_quantity
      , sku_code
      , order_code
      , gift
      , order_detail_code

    FROM order_with
    GROUP BY
      system_code
      , shipped_ymd
      , sku_code
      , order_code
      , gift
      , order_detail_code

    UNION ALL

    SELECT
      'E' as system_code
      , dt as shipped_ymd
      , 0 as total_amount
      , 0 as quantity
      , null as sku_code
      , null as order_code
      , null as gift
      , null as order_detail_code
    FROM
      tmp_date_list_table
  )
GROUP BY
    system_code,
    shipped_ymd
    , sku_code
    , order_code
    , gift
    , order_detail_code
ORDER BY
    system_code,
    shipped_ymd
),

-- 全体の売上情報
sales_all_with AS (
  SELECT
    system_code,
    shipped_ymd,
    '1' AS kind,
    -- 売上金額
    sum(sales_amount) AS sales_amount,
    -- 売上数量
    sum(sales_quantity) AS sales_quantity,
    -- サービス手数料
    0 AS service_amount,
    -- サービス数量
    0 AS service_quantity,
    -- 無料除くサービス数量
    0 AS not_free_service_quantity
  FROM
    total_amount_with
  GROUP BY
    system_code,
    shipped_ymd
  ORDER BY
    system_code,
    shipped_ymd
),

-- ギフトセットの売上情報
sales_giftset_with AS (
  SELECT
    system_code,
    shipped_ymd,
    '2' AS kind,
    -- 売上金額
    sum(sales_amount) AS sales_amount,
    -- 売上数量
    sum(sales_quantity) AS sales_quantity,
    -- サービス手数料
    0 AS service_amount,
    -- サービス数量
    0 AS service_quantity,
    -- 無料除くサービス数量
    0 AS not_free_service_quantity
  FROM
    total_amount_with
  WHERE
    -- ギフトセットマスタに登録されている
    sku_code IS NOT NULL
  GROUP BY
    system_code,
    shipped_ymd
  ORDER BY
    system_code,
    shipped_ymd
),

-- ギフトラップの売上情報
sales_giftwrap_with AS (
  SELECT
    sco.system_code,
    sco.shipped_ymd,
    '3' AS kind,
    -- 売上金額
    sum(sco.sales_amount) AS sales_amount,
    -- 売上数量
    sum(sco.sales_quantity) AS sales_quantity,
    -- サービス手数料(購買情報詳細の重複計上分は除外)
    sum(service_amount) AS service_amount,
    -- サービス数量(購買情報詳細の重複計上分は除外)
    sum(service_quantity) AS service_quantity,
    -- 無料除くサービス数量(購買情報詳細の重複計上分およびアドオンサービスで価格は0のものは除外)
    sum(not_free_service_quantity) AS not_free_service_quantity

  FROM total_amount_with sco,
    (
    SELECT
        system_code
      , shipped_ymd
      , order_code

      -- サービス手数料(購買情報詳細の重複計上分は除外)
      , CAST( SUM(
          CASE
            WHEN IF(order_detail_code='1',addon_price,0) IS NULL   THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER'                  THEN IF(order_detail_code='1',addon_price,0) * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER'                  THEN IF(order_detail_code='1',addon_price,0)
            ELSE 0
          END
        ) AS bigint) AS service_amount

      -- サービス数量(購買情報詳細の重複計上分は除外)
      , CAST( SUM(
          CASE
            WHEN IF(order_detail_code='1',addon_quantity,0) IS NULL THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER'                   THEN IF(order_detail_code='1',addon_quantity,0) * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER'                   THEN IF(order_detail_code='1',addon_quantity,0)
            ELSE 0
          END
        ) AS bigint) AS service_quantity

      -- 無料除くサービス数量(購買情報詳細の重複計上分およびアドオンサービスで価格は0のものは除外)
      , CAST( SUM(
          CASE
            WHEN IF(order_detail_code='1' AND addon_price > 0,addon_quantity,0) IS NULL THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER'                  THEN IF(order_detail_code='1' AND addon_price > 0,addon_quantity,0) * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER'                  THEN IF(order_detail_code='1' AND addon_price > 0,addon_quantity,0)
            ELSE 0
          END
        ) AS bigint) AS not_free_service_quantity
    FROM
       order_addon_services_with
    WHERE
      -- アドオン名が設定されている(アドオンが結びつかなかったデータを対象外にする)
      addon_name <> ''
      -- 対象とするアドオンサービスをギフトとする
      AND addon_name = 'ギフト'
    GROUP BY
      system_code, shipped_ymd, order_code
    ) sco_as
  WHERE
    sco.order_code  = sco_as.order_code
    AND sco.shipped_ymd = sco_as.shipped_ymd
    -- ギフトセットマスタに登録されている
    AND sco.sku_code IS NULL
    -- ギフトフラグが'1'
    AND sco.gift = '1'
  GROUP BY
    sco.system_code,
    sco.shipped_ymd
  ORDER BY
    system_code,
    shipped_ymd
),

-- 刻印の売上情報
sales_carved_seal_with AS (
  SELECT
    sco.system_code,
    sco.shipped_ymd,
    '4' AS kind,
    -- 売上金額
    sum(sco.sales_amount) AS sales_amount,
    -- 売上数量
    sum(sco.sales_quantity) AS sales_quantity,
    -- サービス手数料
    sum(service_amount) AS service_amount,
    -- サービス数量
    sum(service_quantity) AS service_quantity,
    -- 無料除くサービス数量(アドオンサービスで価格は0のものは除外)
    sum(not_free_service_quantity) AS not_free_service_quantity

  FROM total_amount_with sco,
    (
    SELECT
        system_code
      , shipped_ymd
      , order_code
      , order_detail_code

      -- サービス手数料
      , CAST( SUM(
          CASE
            WHEN addon_price IS NULL   THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER'                  THEN addon_price * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER'                  THEN addon_price
            ELSE 0
          END
        ) AS bigint) AS service_amount

      -- サービス数量
      , CAST( SUM(
          CASE
            WHEN addon_quantity IS NULL THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER'                   THEN addon_quantity * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER'                   THEN addon_quantity
            ELSE 0
          END
        ) AS bigint) AS service_quantity

      -- 無料除くサービス数量(アドオンサービスで価格は0のものは除外)
      , CAST( SUM(
          CASE
            WHEN IF(addon_price > 0,addon_quantity,0) IS NULL THEN 0
            WHEN user_order_detail_type = 'RETURN_ORDER'                  THEN IF(addon_price > 0,addon_quantity,0) * - 1
            WHEN user_order_detail_type = 'NORMAL_ORDER'                  THEN IF(addon_price > 0,addon_quantity,0)
            ELSE 0
          END
        ) AS bigint) AS not_free_service_quantity
    FROM
       addon_services_with
    WHERE
      -- アドオン名が設定されている(アドオンが結びつかなかったデータを対象外にする)
      addon_name <> ''
      -- 対象とするアドオンサービスを刻印とする
      AND addon_name = '刻印'
    GROUP BY
      system_code, shipped_ymd, order_code, order_detail_code
    ) sco_as
  WHERE
        sco.order_code  = sco_as.order_code
    AND sco.order_detail_code = sco_as.order_detail_code
    AND sco.shipped_ymd = sco_as.shipped_ymd
  GROUP BY
    sco.system_code,
    sco.shipped_ymd
  ORDER BY
    system_code,
    shipped_ymd
)

SELECT
  system_code,
  shipped_ymd AS aggregate_day,
  kind,
  sum(sales_amount) AS sales_amount,
  sum(sales_quantity) AS sales_quantity,
  sum(service_amount) AS service_amount,
  sum(service_quantity) AS service_quantity,
  sum(not_free_service_quantity) AS not_free_service_quantity
FROM
  (
  -- 全体の売上情報
  SELECT
    system_code, shipped_ymd, kind, sales_amount, sales_quantity, service_amount, service_quantity, not_free_service_quantity
  FROM sales_all_with

  UNION ALL

  -- ギフトセットの売上情報
  SELECT
    system_code, shipped_ymd, kind, sales_amount, sales_quantity, service_amount, service_quantity, not_free_service_quantity
  FROM sales_giftset_with

  UNION ALL

  -- ギフトラップの売上情報
  SELECT
    system_code, shipped_ymd, kind, sales_amount, sales_quantity, service_amount, service_quantity, not_free_service_quantity
  FROM sales_giftwrap_with

  UNION ALL

  -- 刻印の売上情報
  SELECT
    system_code, shipped_ymd, kind, sales_amount, sales_quantity, service_amount, service_quantity, not_free_service_quantity
  FROM sales_carved_seal_with
  )
WHERE system_code = 'E'
GROUP BY
  system_code,
  shipped_ymd,
  kind
ORDER BY
  system_code,
  shipped_ymd,
  kind