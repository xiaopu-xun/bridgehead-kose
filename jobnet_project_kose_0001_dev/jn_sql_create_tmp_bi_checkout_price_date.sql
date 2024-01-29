-- @TD engine_version: 350
SELECT
	x1.checkout_date, --受注日
	x1.td_system_code, --利用システムコード
	(CASE WHEN x1.td_system_code = 'D' THEN 'フローラノーティス'
	WHEN x1.td_system_code = 'E' THEN 'ジルスチュアート'
	WHEN x1.td_system_code = 'G' THEN 'アディクション'
	WHEN x1.td_system_code = 'J' THEN '米肌'
  WHEN x1.td_system_code = 'F' THEN
    CASE WHEN substring(x2.category_nm from 1 for position('|' in x2.category_nm)-1) != ''
          AND substring(x2.category_nm from 1 for position('|' in x2.category_nm)-1) IS NOT NULL
    THEN substring(x2.category_nm from 1 for position('|' in x2.category_nm)-1)
	  ELSE '商品マスタ無し' END
  ELSE NULL END) AS brand, --ブランド
	SUM(CASE WHEN x1.user_order_detail_type = 'NORMAL_ORDER'
	THEN COALESCE(x1.quantity, 0) * COALESCE(x1.price_ex_vat, 0)
	WHEN x1.user_order_detail_type = 'RETURN_ORDER'
	THEN COALESCE(x1.quantity, 0) * COALESCE(x1.price_ex_vat, 0) * -1
  ELSE 0 END)	AS price --売上金額

FROM
	kosedmp_prd_secure.bi_segment_order_detail x1
LEFT OUTER JOIN
  kosedmp_prd_secure.ecbeing_item_mst x2
ON
	x1.item_code = x2.item_cd
WHERE
  x1.price_ex_vat > 0
AND
  x1.checkout_date <> ''
GROUP BY
  1,2,3
ORDER BY
  1,2,3