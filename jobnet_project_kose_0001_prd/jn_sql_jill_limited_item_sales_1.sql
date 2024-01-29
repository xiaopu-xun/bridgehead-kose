-- 処理概要
-- ジルスチュアートの商品の中から通常品、限定品の合計を求める。
-- 通常品：アイテムマスタの限定品区分=「通常品」
-- 既存品：アイテムマスタの限定品区分=「限定品」
-- セットアイテムの場合はセットを分割してそれぞれのアイテムごとの発売日を利用する。
-- senden_kbn=「期間宣伝物」は集計対象外

-- set_item_separate_tmp_table
-- セットアイテムコード,アイテムコード,限定品区分,金額

-- limit_item_tmp_table
-- 利用システムコード,発送日時,限定品区分,注文タイプ,注文数,単体金額
-- アイテムコード,発売日,限定品区分,金額,注文タイプ

-- 上記を結合して以下のテーブル作成
-- 利用システムコード,発送日時,限定品区分,アイテムコード,注文タイプ,金額

-- 上記をグルーピングして以下のテーブル作成
-- 利用システムコード,発送日時,通常品合計,既存品合計


WITH  set_item_separate_tmp_table AS(
  -- セットアイテムを分解してアイテム単位にする
  SELECT
    A.item_code,
    max(C.price) as set_price,
    A.component_code,
    max(B.hatubai_date) as hatubai_date,
    max(B.senden_kbn) as senden_kbn,
    max(B.limited_kbn) as limited_kbn,
    max(B.price) as component_item_price
  FROM segment_common_set_mst A
    LEFT JOIN segment_common_item_mst B
    ON A.component_code = B.hinmoku_cd
    LEFT JOIN segment_common_item_mst C
    ON A.item_code = C.hinmoku_cd

  WHERE C.daihan_class_name = 'ジルスチュアート'

  GROUP BY A.item_code,A.component_code
  ORDER BY A.item_code,A.component_code

), limit_item_tmp_table AS(
-- セット品ではないもの
SELECT
  C.system_code, -- 利用システムコード
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN CAST(B.return_date AS bigint)/ 1000
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN CAST(B.shipped_timestamp AS bigint)/ 1000
  END AS shipped_timestamp, -- 出荷日(timestamp)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(B.return_date AS bigint)/ 1000, 'yyyy-MM', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(B.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM', 'JST')
  END AS shipped_ym, -- 出荷日(yyyy-mm)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(B.return_date AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(B.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
  END AS shipped_ymd, -- 出荷日(yyyy-mm-dd)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT( TD_TIME_ADD(CAST(B.return_date AS bigint      ) / 1000, '-30d', 'JST'), 'yyyy-MM-dd', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT( TD_TIME_ADD(CAST(B.shipped_timestamp AS bigint) / 1000, '-30d', 'JST'), 'yyyy-MM-dd', 'JST')
  END AS newitem_limit_timestamp, -- 新商品判定用日付(yyyy-mm-dd)

  A.sku_code, --アイテムコード
  A.user_order_detail_type, -- 購買明細.注文タイプ
  E.limited_kbn, --限定品区分
  A.quantity, --数量
  A.price_ex_vat,  --単体金額(税抜)
  A.quantity * A.price_ex_vat AS amount_sku_ex_vat

FROM
  jill_segment_common_order_detail_items A
  LEFT JOIN jill_segment_common_order B
  ON A.order_code = B.order_code

  LEFT JOIN segment_common_after_regist C
  ON B.customer_code_hash = C.customer_code_hash

  -- LEFT JOIN set_item_separate_tmp_table D
  -- ON A.sku_code = D.item_code

  LEFT JOIN segment_common_item_mst E
  ON A.sku_code = E.hinmoku_cd


WHERE
  A.set_item <> '1'
  AND B.shipped_timestamp <> ''
  AND TD_TIME_RANGE(CAST(B.shipped_timestamp AS bigint)/ 1000,
    TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
    'JST')
  AND E.senden_kbn <> '期間宣伝物' --期間宣伝物を対象にする場合は条件から外す
  AND NOT EXISTS (
    SELECT 1 FROM set_item_separate_tmp_table D
    WHERE A.sku_code = D.item_code
  )

  UNION ALL

-- セット品のもの
SELECT
  C.system_code, -- 利用システムコード
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN CAST(B.return_date AS bigint)/ 1000
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN CAST(B.shipped_timestamp AS bigint)/ 1000
  END AS shipped_timestamp, -- 出荷日(timestamp)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(B.return_date AS bigint)/ 1000, 'yyyy-MM', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(B.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM', 'JST')
  END AS shipped_ym, -- 出荷日(yyyy-mm)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT(CAST(B.return_date AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT(CAST(B.shipped_timestamp AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
  END AS shipped_ymd, -- 出荷日(yyyy-mm-dd)
  CASE
    WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN TD_TIME_FORMAT( TD_TIME_ADD(CAST(B.return_date AS bigint      ) / 1000, '-30d', 'JST'), 'yyyy-MM-dd', 'JST')
    WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN TD_TIME_FORMAT( TD_TIME_ADD(CAST(B.shipped_timestamp AS bigint) / 1000, '-30d', 'JST'), 'yyyy-MM-dd', 'JST')
  END AS newitem_limit_timestamp, -- 新商品判定用日付(yyyy-mm-dd)

  A.sku_code, --アイテムコード
  A.user_order_detail_type, -- 購買明細.注文タイプ
  D.limited_kbn, --限定品区分
  A.quantity, --数量
  D.component_item_price,  --単体金額(税抜)
  A.quantity * D.component_item_price AS amount_sku_ex_vat

FROM
  jill_segment_common_order_detail_items A
  LEFT JOIN jill_segment_common_order B
  ON A.order_code = B.order_code

  LEFT JOIN segment_common_after_regist C
  ON B.customer_code_hash = C.customer_code_hash

  INNER JOIN set_item_separate_tmp_table D
  ON A.sku_code = D.item_code

  LEFT JOIN segment_common_item_mst E
  ON A.sku_code = E.hinmoku_cd


WHERE
  A.set_item = '1'
  AND  B.shipped_timestamp <> ''
  AND TD_TIME_RANGE(CAST(B.shipped_timestamp AS bigint)/ 1000,
    TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST'),
    TD_TIME_FORMAT(${end_date}, 'yyyy-MM-dd', 'JST'),
    'JST')
  AND D.item_code IS NOT NULL

), tmp_date_list_table AS(
 SELECT
 CAST(dt AS VARCHAR) as dt
 FROM  (SELECT 1)
 CROSS JOIN unnest(sequence(cast(TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST') as date), current_date, interval '1' day)) as t(dt)
)

SELECT
  system_code,
  shipped_ymd,
  SUM(normal_item_sales_amount) AS normal_item_sales_amount,
  SUM(limited_item_sales_amount) AS limited_item_sales_amount
  FROM (

    SELECT
      A.system_code,
      A.shipped_ymd,
      CAST(
        SUM(CASE
              WHEN A.user_order_detail_type = 'RETURN_ORDER' THEN A.amount_sku_ex_vat * -1
              WHEN A.user_order_detail_type = 'NORMAL_ORDER' THEN A.amount_sku_ex_vat
            END
          )
        AS integer) AS total_amount,
      CAST(
        SUM(CASE
              WHEN A.user_order_detail_type = 'RETURN_ORDER' AND A.limited_kbn = '通常品'
                THEN A.amount_sku_ex_vat * -1
              WHEN A.user_order_detail_type = 'NORMAL_ORDER' AND A.limited_kbn = '通常品'
                THEN A.amount_sku_ex_vat
              ELSE 0
            END
          )
        AS integer) AS normal_item_sales_amount,
      CAST(
        SUM(CASE
              WHEN A.user_order_detail_type = 'RETURN_ORDER' AND A.limited_kbn = '限定品'
                THEN A.amount_sku_ex_vat * -1
              WHEN A.user_order_detail_type = 'NORMAL_ORDER' AND A.limited_kbn = '限定品'
                THEN A.amount_sku_ex_vat
              ELSE 0
            END
          )
        AS integer) AS limited_item_sales_amount

      FROM limit_item_tmp_table A

      GROUP BY
      A.system_code,
      A.shipped_ymd,
      A.user_order_detail_type

      UNION ALL

      SELECT
        'E' as system_code,
        dt as shipped_ymd,
        0 as total_amount,
        0 as normal_item_sales_amount,
        0 as limited_item_sales_amount

      FROM tmp_date_list_table
  )
GROUP BY system_code,shipped_ymd
ORDER BY system_code,shipped_ymd