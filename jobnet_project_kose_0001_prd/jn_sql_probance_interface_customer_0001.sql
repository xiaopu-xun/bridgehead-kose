WITH first_buy_time AS (				--初期購買日時
	SELECT
		customer_code_hash,
		checkout_timestamp,
		ROW_NUMBER() OVER( PARTITION BY customer_code_hash ORDER BY checkout_timestamp ) AS row_number
	FROM
		segment_probance_order
),last_buy_time AS(						--最終購買日時
	SELECT
		customer_code_hash,
		checkout_timestamp,
		ROW_NUMBER() OVER( PARTITION BY customer_code_hash ORDER BY checkout_timestamp DESC ) AS row_number
	FROM
		segment_probance_order
),first_order_flg AS(					--初回購入済み区分
	SELECT
		customer_code_hash,
		COUNT(order_code) AS ord_cont
	FROM
		segment_common_order
	GROUP BY
		customer_code_hash
),maison_buy_count AS(					--購入回数（Maison）
	SELECT
		spo.customer_code_hash,
		COUNT(spo.order_code) AS buy_count_maison
	FROM
		(
		SELECT
			order_code,
			customer_code_hash,
			canceled_timestamp
		FROM
			segment_probance_order
		WHERE
			checkout_timestamp <> ''
			AND TRIM(canceled_timestamp) = ''
		) AS spo
	LEFT JOIN
		(
		SELECT
			customer_code_hash,
			system_code
		FROM
			segment_common_after_regist
		GROUP BY
			customer_code_hash,
			system_code
		) AS scar
	ON
		spo.customer_code_hash = scar.customer_code_hash
	WHERE
		scar.system_code = 'F'
	GROUP BY
		spo.customer_code_hash
),maihada_buy_data AS(					--抽出条件："マイハダ" 購買明細データ
	SELECT
		spod.order_code,
		spod.item_code,
		eim.category_nm
	FROM
		segment_probance_order_detail spod
	LEFT JOIN
		ecbeing_item_mst eim
	ON
		eim.item_cd = spod.item_code
	WHERE
		eim.category_nm LIKE '%米肌%'
	GROUP BY
		spod.order_code,
		spod.item_code,
		eim.category_nm
),maihada_buy_count AS(					--購入回数（maihada）
	SELECT
		rslt.customer_code_hash,
		COUNT(DISTINCT rslt.order_code) AS buy_count_maihada
	FROM
		(
		SELECT
			spo.customer_code_hash,
			spo.order_code,
			scar.system_code
		FROM
			segment_probance_order spo
		LEFT JOIN
			segment_common_after_regist scar
		ON
			scar.customer_code_hash = spo.customer_code_hash
		WHERE
			scar.system_code IN ('F', 'J')
			AND spo.checkout_timestamp <> ''
			AND TRIM(spo.canceled_timestamp) = ''
		GROUP BY
			spo.customer_code_hash,
			spo.order_code,
			scar.system_code
		) AS rslt
	LEFT JOIN
		maihada_buy_data mbd
	ON
		mbd.order_code = rslt.order_code
	WHERE
		mbd.category_nm LIKE '%米肌%'
	OR
		rslt.system_code = 'J'
	GROUP BY
		rslt.customer_code_hash
),drphil_buy_data AS(					--抽出条件："ドクターフィル" 購買明細データ
	SELECT
		spod.order_code,
		spod.item_code,
		eim.category_nm
	FROM
		segment_probance_order_detail spod
	LEFT JOIN
		ecbeing_item_mst eim
	ON
		eim.item_cd = spod.item_code
	WHERE
		eim.category_nm LIKE 'ドクターフィル%'
	GROUP BY
		spod.order_code,
		spod.item_code,
		eim.category_nm
),drphil_buy_count AS(					--購入回数（DrPHIL）
	SELECT
		rslt.customer_code_hash,
		COUNT(DISTINCT rslt.order_code) AS buy_count_drphil
	FROM
		(
		SELECT
			spo.customer_code_hash,
			spo.order_code
		FROM
			segment_probance_order spo
		LEFT JOIN
			segment_common_after_regist scar
		ON
			scar.customer_code_hash = spo.customer_code_hash
		WHERE
			scar.system_code = 'F'
			AND spo.checkout_timestamp <> ''
			AND TRIM(spo.canceled_timestamp) = ''
		GROUP BY
			spo.customer_code_hash,
			spo.order_code
		) AS rslt
	LEFT JOIN
		drphil_buy_data dbd
	ON
		dbd.order_code = rslt.order_code
	WHERE
		dbd.category_nm LIKE 'ドクターフィル%'
	GROUP BY
		rslt.customer_code_hash
)

SELECT DISTINCT
	scar.customer_code_hash AS CST_ID,
	IF(scar.state_flag = '', '', scar.state_flag) AS MEM_DIV,
	'' AS CST_LNM,
	'' AS CST_FNM,
	'' AS KN_CST_LNM,
	'' AS KN_CST_FNM,
  CASE scar.sex
    WHEN 'M' THEN '男性'
    WHEN 'F' THEN '女性'
    WHEN 'N' THEN '無回答'
    ELSE ''
  END AS SEIBETSU,
	IF( CAST( scar.birthday AS VARCHAR ) = '', '',
		TD_TIME_FORMAT(
			CAST(scar.birthday AS bigint)/ 1000,
		'yyyy-MM-dd', 'JST')
	) AS BIRTH_DAY,
	'' AS ML_ADDRESS,
	IF(scar.able_tel_flag = '', '未設定',
		IF(scar.able_tel_flag = '0', '電話連絡可',
			IF(scar.able_tel_flag = '1', '電話連絡不可', '未設定'))
	) AS PHONE_OK_DIV,
	IF(scar.ablemail = '', '未設定',
		IF( scar.ablemail = '0', '送信不可',
			IF( scar.ablemail = '1', '送信可', '未設定' ))
	) AS DM_OK_DIV,
	IF(fof.ord_cont > 0, '購入済み', '未購入') AS FIRST_BUY_DIV,
	IF(scar.status = 'VALID', '未退会', '退会済') AS QUIT_DIV,
	IF(CAST(scar.systemcreatedate AS VARCHAR) = '', '',
		TD_TIME_FORMAT(CAST(scar.systemcreatedate AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
	) AS ENT_DATE,
	IF(scar.systemupdatedate = '', '',
		TD_TIME_FORMAT(CAST(scar.systemupdatedate AS bigint)/ 1000, 'yyyy-MM-dd', 'JST')
	) AS QUIT_DATE,
	IF(lbt.checkout_timestamp = '', '',
		TD_TIME_FORMAT(CAST(lbt.checkout_timestamp AS bigint)/ 1000, 'yyyy-MM-dd HH:mm:ss', 'JST')
	) AS LAST_BUY_TIME,
	'' AS LOGIN_TIME,
	'' AS LAST_ACS_TIME,
	SUBSTRING(scar.zip_code, 1, 3) AS POST_NO01,
	SUBSTRING(scar.zip_code, 4, 4) AS POST_NO02,
	scar.state AS TODOFUKEN_NM,
	'' AS VALID_POINT,
	'' AS POINT_EDATE,
	scar.system_code AS SYSTEM_CODE,
	IF(scar.periodical_active_flag = '1', 'アクティブ', '非アクティブ') AS PERIODICAL_BUY_DIV,
	IF(scar.mailmagazine_maison IS NOT NULL,
		IF(scar.mailmagazine_maison = '', '配信不可',
			IF(scar.mailmagazine_maison = '1', '配信可', '配信不可')
	), '配信不可') AS MAG_MAISON,
	IF(scar.mailmagazine_maihada IS NOT NULL,
		IF(scar.mailmagazine_maihada = '', '配信不可',
			IF(scar.mailmagazine_maihada = '1', '配信可', '配信不可')
	), '配信不可') AS MAG_MAIHADA,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(skin_troubles_maihada, '"', ''), ']', '％２２'), '[', '％２２'), ' ', ''), ',' , '％ｃ２') AS SKIN_TROUBLE,
	IF(fbt.checkout_timestamp = '', '',
		TD_TIME_FORMAT(CAST(fbt.checkout_timestamp AS bigint)/ 1000, 'yyyy-MM-dd HH:mm:ss', 'JST')
	) AS FIRST_BUY_TIME,
	IF(maison_bc.buy_count_maison IS NULL, 0, maison_bc.buy_count_maison) AS BUY_COUNT_MAISON,
	IF(maihada_bc.buy_count_maihada IS NULL, 0, maihada_bc.buy_count_maihada) AS BUY_COUNT_MAIHADA,
	IF(scar.mailmagazine_drphil IS NOT NULL,
		IF(scar.mailmagazine_drphil = '', '配信不可',
			IF(scar.mailmagazine_drphil = '1', '配信可', '配信不可')
	), '配信不可') AS MAG_DRPHIL,
	IF(drphil_bc.buy_count_drphil IS NULL, 0, drphil_bc.buy_count_drphil) AS BUY_COUNT_DRPHIL,
	scar.customerrank_maihada AS CUSTOMER_RANK
FROM
	segment_common_after_regist scar
LEFT JOIN
	segment_probance_order spo
ON
	spo.customer_code_hash = scar.customer_code_hash
LEFT JOIN
	first_buy_time fbt
ON
	fbt.customer_code_hash = scar.customer_code_hash
	AND fbt.row_number = 1
LEFT JOIN
	last_buy_time lbt
ON
	lbt.customer_code_hash = scar.customer_code_hash
	AND lbt.row_number = 1
LEFT JOIN
	first_order_flg fof
ON
	fof.customer_code_hash = scar.customer_code_hash
LEFT JOIN
	maison_buy_count maison_bc
ON
	maison_bc.customer_code_hash = scar.customer_code_hash
LEFT JOIN
	maihada_buy_count maihada_bc
ON
	maihada_bc.customer_code_hash = scar.customer_code_hash
LEFT JOIN
	drphil_buy_count drphil_bc
ON
	drphil_bc.customer_code_hash = scar.customer_code_hash
WHERE
	scar.system_code IN ('F', 'J')