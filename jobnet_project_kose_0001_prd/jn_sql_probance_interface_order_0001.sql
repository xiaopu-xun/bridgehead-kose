WITH join_table AS(				--「顧客」「購買」「明細」データ結合　条件：システムコード「F」
	SELECT
		spod.order_code,
		spod.order_detail_code,
		spod.sku_code,
		spod.item_code,
		spod.item_name,
		spod.price_ex_vat,
		spod.price_in_vat,
		spod.quantity,
		spod.user_order_detail_type,
		spo.customer_code_hash,
		spo.checkout_timestamp,
		spo.shipped_timestamp,
		spo.canceled_timestamp,
		spo.return_date,
		spo.order_method,
		spo.attributes_media_code,
		spo.user_peyment_method,
		spo.sku_ex_vat,
		spo.sku_in_vat,
		spo.total_ex_vat,
		spo.total_in_vat,
		CAST(JSON_EXTRACT(JSON_ARRAY_GET(JSON_EXTRACT(spo.point_details, '$.point_details.points'), 0), '$.attributes.earned_points') AS VARCHAR) AS earned_points,
		scar.system_code
	FROM
		segment_probance_order_detail spod
	LEFT JOIN
		segment_probance_order spo
	ON
		spo.order_code = spod.order_code
	LEFT JOIN
		segment_common_after_regist scar
	ON
		scar.customer_code_hash = spo.customer_code_hash
	WHERE
		scar.system_code IN ('F', 'J')
),first_order_date AS(					--初期購買日時
	SELECT
		spod.order_code,
		spod.sku_code,
		so.customer_code_hash,
		so.checkout_timestamp,
		ROW_NUMBER() OVER( PARTITION BY spod.sku_code, so.customer_code_hash ORDER BY so.checkout_timestamp ) AS row_number
	FROM
		segment_probance_order_detail spod
	LEFT JOIN
		(
		SELECT
			spo.order_code,
			spo.customer_code_hash,
			spo.checkout_timestamp
		FROM
			segment_probance_order spo
		LEFT JOIN
			segment_common_after_regist scar
		ON
			scar.customer_code_hash = spo.customer_code_hash
		GROUP BY
			spo.order_code,
			spo.customer_code_hash,
			spo.checkout_timestamp
		) AS so
	ON
		so.order_code = spod.order_code
	GROUP BY
		spod.order_code,
		spod.sku_code,
		so.customer_code_hash,
		so.checkout_timestamp
),last_order_date AS(					--最終購買日時
	SELECT
		spod.order_code,
		spod.sku_code,
		so.customer_code_hash,
		so.checkout_timestamp,
		ROW_NUMBER() OVER( PARTITION BY spod.sku_code, so.customer_code_hash ORDER BY so.checkout_timestamp DESC ) AS row_number
	FROM
		segment_probance_order_detail spod
	LEFT JOIN
		(
		SELECT
			spo.order_code,
			spo.customer_code_hash,
			spo.checkout_timestamp
		FROM
			segment_probance_order spo
		LEFT JOIN
			segment_common_after_regist scar
		ON
			scar.customer_code_hash = spo.customer_code_hash
		GROUP BY
			spo.order_code,
			spo.customer_code_hash,
			spo.checkout_timestamp
		) AS so
	ON
		so.order_code = spod.order_code
	GROUP BY
		spod.order_code,
		spod.sku_code,
		so.customer_code_hash,
		so.checkout_timestamp
)

SELECT
	jt.order_code AS ORDER_NO,
	jt.customer_code_hash AS CST_ID,
	IF(jt.checkout_timestamp IS NULL, '',
		IF(jt.checkout_timestamp = '', '',
			TD_TIME_FORMAT(CAST(jt.checkout_timestamp AS BIGINT)/ 1000, 'yyyy-MM-dd HH:mm:ss', 'JST'))
	) AS ORDER_DATE,
	IF(jt.shipped_timestamp IS NULL, '',
		IF(jt.shipped_timestamp = '', '',
			TD_TIME_FORMAT(CAST(jt.shipped_timestamp AS BIGINT)/ 1000, 'yyyy-MM-dd', 'JST'))
	) AS SHIP_DEL_DATE,
	jt.item_code AS CM_CD,
	REGEXP_REPLACE(jt.item_name,',','') AS CM_NM,
	jt.price_ex_vat AS SN_PRICE,
	jt.price_in_vat AS SI_PRICE,
	jt.quantity AS ORDER_QTY,
	IF(jt.canceled_timestamp IS NOT NULL,
		IF(jt.canceled_timestamp = '', '未登録', 'キャンセル')
	, '未登録') AS CANCEL_KB,
	IF(jt.return_date IS NOT NULL,
		IF(jt.return_date <> '', '返品',
			IF(jt.user_order_detail_type = 'RETURN_ORDER', '返品', '未登録')
		), '未登録') AS RETURN_KB,
	'' AS SALES_KB,
	jt.system_code AS SP_CD,
	IF(fod.checkout_timestamp IS NULL, '',
		IF(fod.checkout_timestamp = '', '',
			TD_TIME_FORMAT(CAST(fod.checkout_timestamp AS BIGINT)/ 1000, 'yyyy-MM-dd HH:mm:ss', 'JST'))
	) AS FIRST_BUY_TIME,
	IF(lod.checkout_timestamp IS NULL, '',
		IF(lod.checkout_timestamp = '', '',
			TD_TIME_FORMAT(CAST(lod.checkout_timestamp AS BIGINT)/ 1000, 'yyyy-MM-dd HH:mm:ss', 'JST'))
	) AS LAST_BUY_TIME,
  CASE jt.order_method
    WHEN '0' THEN 'Web注文'
    WHEN '1' THEN 'TEL'
    WHEN '2' THEN 'FAX'
    WHEN '3' THEN 'E-MAIL'
    WHEN '4' THEN 'ハガキ'
    WHEN '5' THEN '定期継続回'
    WHEN '9' THEN 'その他'
    WHEN 'S' THEN '店舗'
    WHEN 'G' THEN '銀座店'
    ELSE ''
  END AS INPUT_DIV,
	jt.earned_points AS ADD_POINT,
	jt.attributes_media_code AS MEDIA_CD,
	jt.user_peyment_method AS PAY_METHOD,
	jt.quantity AS BUY_QTY,
	CAST(jt.sku_ex_vat AS BIGINT) AS BUY_NPRICE,
	CAST(jt.sku_in_vat AS BIGINT) AS BUY_PRICE,
	IF(jt.checkout_timestamp IS NULL, '',
		IF(jt.checkout_timestamp = '', '',
			TD_TIME_FORMAT(CAST(jt.checkout_timestamp AS BIGINT)/ 1000, 'yyyy-MM-dd HH:mm:ss', 'JST'))
	) AS EVENT_DATE,
	CAST(jt.total_ex_vat AS BIGINT) AS SELLING_PRICE_EX_VAT,
	CAST(jt.total_in_vat AS BIGINT) AS SELLING_PRICE_IN_VAT,
  CASE eim.item_no_2
    WHEN 'PRBQ' THEN '肌潤トライアル'
    WHEN 'PRTG' THEN '肌潤トライアル'
    WHEN 'PRBP' THEN '活潤トライアル'
    WHEN 'PRTI' THEN '活潤トライアル'
    WHEN 'PRTT' THEN '美白トライアル'
    WHEN 'PRTU' THEN '美白トライアル'
    ELSE ''
  END AS TRIAL_VAT,
  IF(jt.system_code = 'J', '米肌EC',
    IF(strpos(eim.category_nm,'|') = 0,eim.category_nm,
      SUBSTRING(eim.category_nm,1,strpos(eim.category_nm,'|')-1)
    )
	) AS BRAND
FROM
	join_table jt
LEFT JOIN
	first_order_date fod
ON
	fod.customer_code_hash = jt.customer_code_hash
	AND fod.sku_code = jt.sku_code
	AND fod.row_number = 1
LEFT JOIN
	last_order_date lod
ON
	lod.customer_code_hash = jt.customer_code_hash
	AND lod.sku_code = jt.sku_code
	AND lod.row_number = 1
LEFT JOIN
	ecbeing_item_mst eim
ON
	eim.item_cd = jt.item_code
WHERE
  (eim.item_classification <> '2' OR eim.item_classification IS NULL)