SELECT
	lf.uid,
	lf.client_id
FROM
	(
	SELECT
		customer_code_hash,
		sns_line
	FROM
		segment_common_after_regist
	WHERE
		(system_code = 'F' OR system_code = 'J')
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
		line_friend
	WHERE
		client_id = '60dc90cdb4f66411'			--クライアントID(マイハダ)
	) AS lf
ON
	lf.uid = scar.sns_line