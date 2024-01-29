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

    , OD.order_code

  FROM
    jill_segment_common_order_detail_items OD
    LEFT JOIN jill_segment_common_order O
      ON OD.order_code = O.order_code
    LEFT JOIN segment_common_after_regist AR
      ON O.customer_code_hash = AR.customer_code_hash
  WHERE
    O.shipped_timestamp <> ''
    AND TD_TIME_RANGE(
      CAST(O.shipped_timestamp AS bigint) / 1000
      , TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST')
      , TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST')
      , 'JST'
    )
),

-- 日付ゼロ埋め用
tmp_date_list_table AS (
  SELECT
    CAST(dt AS VARCHAR) as dt
  FROM
    (SELECT 1)
    CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
)

--
SELECT
    -- system_code,
    shipped_ymd AS aggregate_day
  , traffic_source
  , CAST(SUM(total_amount) AS bigint) AS sales_amount
FROM
  (
    SELECT
        o.system_code
      , o.shipped_ymd

      , CASE
          WHEN g.channel_grouping <> '(Other)'          THEN channel_grouping
          WHEN g.source = 'mail'                        THEN 'Mail'
          WHEN (g.source = 'LINE' OR g.source = 'line') THEN 'LINE'
          ELSE 'Other'
        END AS traffic_source

      , CAST( SUM(
          CASE
            WHEN o.amount_sku_ex_vat IS NULL               THEN 0
            WHEN o.user_order_detail_type = 'RETURN_ORDER' THEN o.amount_sku_ex_vat * - 1
            WHEN o.user_order_detail_type = 'NORMAL_ORDER' THEN o.amount_sku_ex_vat
            ELSE 0
          END
        ) AS bigint) AS total_amount
    FROM order_with o
    INNER JOIN
      ga_info g
      ON o.order_code = g.transaction_id
    GROUP BY
      system_code
      , o.shipped_ymd
      , g.channel_grouping
      , g.source

    -- ゼロ埋め
    UNION ALL

    SELECT
        'E'  as system_code
      , dt   as shipped_ymd
      , null as traffic_source
      , 0    as total_amount
    FROM
      tmp_date_list_table
  )
WHERE system_code = 'E'
GROUP BY
--  system_code,
    shipped_ymd
  , traffic_source
ORDER BY
--  system_code,
    shipped_ymd