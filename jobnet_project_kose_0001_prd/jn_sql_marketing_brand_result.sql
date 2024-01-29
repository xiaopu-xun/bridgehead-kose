WITH brand_result_tmp AS(

SELECT
  --年度の算出
  CASE
  WHEN TD_TIME_FORMAT(CAST(t2.checkout_timestamp AS BIGINT) / 1000, 'MM','JST') IN ('01','02','03')
  THEN CAST(TD_TIME_FORMAT(CAST(t2.checkout_timestamp AS BIGINT) / 1000, 'YYYY','JST') AS INTEGER)-1
  ELSE CAST(TD_TIME_FORMAT(CAST(t2.checkout_timestamp AS BIGINT) / 1000, 'yyyy','JST') AS INTEGER)
	END AS year,
  --年月日
  TD_TIME_FORMAT(CAST(t2.checkout_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd','JST') AS checkout_day,
  --月（フィルター用）
  TD_TIME_FORMAT(CAST(t2.checkout_timestamp AS BIGINT) / 1000, 'MM','JST') AS month,
  --ブランド
  (CASE WHEN t1.td_system_code = 'D' THEN 'フローラノーティス'
  WHEN t1.td_system_code = 'E' THEN 'ジルスチュアート'
  WHEN t1.td_system_code = 'G' THEN 'アディクション'
  WHEN t1.td_system_code = 'J' THEN '米肌'
  WHEN t1.td_system_code = 'F' THEN
    CASE WHEN substring(t3.category_nm from 1 for position('|' in t3.category_nm)-1) != ''
          AND substring(t3.category_nm from 1 for position('|' in t3.category_nm)-1) IS NOT NULL
    THEN substring(t3.category_nm from 1 for position('|' in t3.category_nm)-1)
	  ELSE '商品マスタ無し' END
  ELSE NULL END) AS brand, --ブランド
  '0' AS brand_order_by,
  --受注売上（数量×税抜き販売価格）
  (CASE WHEN t1.user_order_detail_type = 'NORMAL_ORDER'
  THEN COALESCE(t1.quantity, 0) * COALESCE(t1.price_ex_vat, 0)
  WHEN t1.user_order_detail_type = 'RETURN_ORDER'
  THEN COALESCE(t1.quantity, 0) * COALESCE(t1.price_ex_vat, 0) * -1
  ELSE 0 END) AS price_result

FROM
  --汎用セグメント購買データテーブル
	bi_segment_order_detail t1

  --注文コードで汎用セグメント購買明細データテーブルと紐付け
LEFT OUTER JOIN
  bi_segment_order t2
ON
	t1.order_code = t2.order_code

  --商品コードでecbeingアイテムマスタテーブルと紐付け
LEFT OUTER JOIN
  ecbeing_item_mst t3
ON
	t1.item_code = t3.item_cd
  
WHERE t1.td_system_code = 'F'
)

SELECT
 	year,
 	checkout_day AS date,
 	month,
 	brand,
 	brand_order_by,
	SUM(price_result) AS price_result
FROM
	brand_result_tmp
WHERE
	brand IS NOT NULL
GROUP BY
	1,2,3,4,5
ORDER BY
	2,4
;