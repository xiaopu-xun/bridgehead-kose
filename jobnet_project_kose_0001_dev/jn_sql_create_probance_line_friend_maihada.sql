-- @TD engine_version: 350
SELECT
	lf.uid,
	lf.client_id
FROM
	(
	SELECT
		customer_code_hash,
		sns_line
	FROM
		kosedmp_prd_secure.segment_common_after_regist
	WHERE
		system_code = 'F'
		AND customer_code_hash IS NOT NULL
		AND customer_code_hash <> ''
		AND sns_line IS NOT NULL
		AND sns_line <> ''
	) AS scar
INNER JOIN
	(
	SELECT
		uid,
		client_id
	FROM
		kosedmp_prd_secure.line_friend
	WHERE
		--client_id = 'd12143f775226bdf'			--テスト用のクライアントID(マイハダ)
		client_id = '60dc90cdb4f66411'			--クライアントID(マイハダ)
	) AS lf
ON
	lf.uid = scar.sns_line