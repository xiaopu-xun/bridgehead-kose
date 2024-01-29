-- @TD engine_version: 350
WITH
--商品区分を判定する。
tmp_kubun_flug AS (select
hinmoku_cd,
hanbai_class_name,
category_class_name,
func_class1_name,
case when hinmoku_cd LIKE 'PR-T%' THEN '定期' --PR－T
    when hinmoku_cd = 'PRTG' THEN 'トライアル'
    when hinmoku_cd = 'PRTU' THEN 'トライアル'
    when hinmoku_cd = 'PRTI' THEN 'トライアル'
    else '本品'
end as kubun_flug
from
kosedmp_prd_secure.segment_common_item_mst)
--メインの抽出処理
SELECT
  td_system_code AS system_code,
  t1.shipped_date as "date",
  item_code AS syohin_fg,
  item_name AS hanbai_name_jpn,
  quantity AS order_quantity,
  price_ex_vat AS price,
    COALESCE(quantity, 0) * COALESCE(price_ex_vat, 0)   AS sales_amount,
    category_class_name as category1,
    func_class1_name as category2,
    t2.kubun_flug as kbn,
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
  t1.order_code AS order_number
FROM
  kosedmp_prd_secure.bi_segment_order_detail t1
LEFT OUTER JOIN
  tmp_kubun_flug t2
ON
  t1.item_code = t2.hinmoku_cd
LEFT OUTER JOIN
  kosedmp_prd_secure.ecbeing_item_mst t3
ON
	t1.item_code = t3.item_cd
WHERE
  t1.price_ex_vat > 0
AND
  user_order_detail_type = 'NORMAL_ORDER'
AND
  t1.shipped_date BETWEEN '${start_date}' AND '${end_date}'