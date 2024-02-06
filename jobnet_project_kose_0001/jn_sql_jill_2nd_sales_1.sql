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
    , O.customer_code_hash

  FROM
    jill_segment_common_order_detail OD
    LEFT JOIN jill_segment_common_order O
      ON OD.order_code = O.order_code
    LEFT JOIN segment_common_after_regist AR
      ON O.customer_code_hash = AR.customer_code_hash

  WHERE
    O.shipped_timestamp <> ''
    AND CAST(shipped_timestamp AS bigint) / 1000 >= ${starttime}
),

-- 金額算出
total_amount_with AS (
  SELECT
    shipped_ymd
    , SUM(total_amount)   AS sales_amount
    , customer_code_hash
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
      , customer_code_hash

    FROM order_with
    GROUP BY
      system_code
      , shipped_ymd
      , customer_code_hash
  )
  GROUP BY
      shipped_ymd,
      customer_code_hash
  ORDER BY
      shipped_ymd,
      customer_code_hash
),


-- 1st購入日
first_sales_with AS (
  select
    customer_code_hash
    , MIN(shipped_ymd) as first_shipped_ymd
  from total_amount_with
  where sales_amount > 0 --購入額プラスのみ
  GROUP BY
    customer_code_hash
  order by
    customer_code_hash
),

-- repeat購入日
repeat_sales_with AS (
  select
    o.customer_code_hash
  , o.shipped_ymd
  , first_shipped_ymd
  , case
      when shipped_ymd > first_shipped_ymd then shipped_ymd
      else null
    end as repeat_shipped_ymd
  from
    total_amount_with o
  , first_sales_with  first
  where
    sales_amount > 0 --購入額プラスのみ
    AND o.customer_code_hash = first.customer_code_hash
  order by
    customer_code_hash
  , shipped_ymd
  , repeat_shipped_ymd
),

-- 2nd購入日
second_sales_with AS (
  select
    d.customer_code_hash
    , MIN(d.repeat_shipped_ymd) as second_shipped_ymd
  from
    -- repeat日
    repeat_sales_with d

  GROUP BY
    customer_code_hash
  order by
    customer_code_hash
)


-- 初回購入月毎の、2回目購入月と購入者数
SELECT
    first_shipped_month
  , CASE WHEN second_shipped_month IS NULL THEN '' ELSE second_shipped_month END AS second_shipped_month -- nullだと digのupdate unique が効かないため変換。
  , count(customer_code_hash) as customer_count
from
  (
    -- 購入者ごとの初回、2回目購入日
    SELECT
      first.customer_code_hash              as customer_code_hash
      , SUBSTRING(first_shipped_ymd,  1, 7) as first_shipped_month  -- yyyy-MM-dd → yyyy-MM
      , SUBSTRING(second_shipped_ymd, 1, 7) as second_shipped_month
    from
      -- 1st購入日
      first_sales_with first
      ,
      -- 2nd購入日
      second_sales_with second
    where
      first.customer_code_hash = second.customer_code_hash
    order by
      customer_code_hash
      , first_shipped_ymd
      , second_shipped_ymd
  )
group by
  first_shipped_month
  , second_shipped_month
order by
  first_shipped_month
  , second_shipped_month