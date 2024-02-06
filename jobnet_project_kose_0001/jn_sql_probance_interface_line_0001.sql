WITH uid_join_table AS(						--maihada、maison各抽出結果を結合
	SELECT
		scar.customer_code_hash,
		mihd.uid AS hd_uid,
		misn.uid AS sn_uid,
		mihd.client_id AS mihd_client_id,
		misn.client_id AS misn_client_id
	FROM
		segment_common_after_regist scar
	LEFT JOIN
		probance_line_friend_maihada mihd
	ON
		mihd.uid = scar.sns_line
	LEFT JOIN
		probance_line_friend_maison misn
	ON
		misn.uid = scar.sns_line
	WHERE
		mihd.uid IS NOT NULL
		OR misn.uid IS NOT NULL
)

SELECT DISTINCT
	ujt.customer_code_hash AS CST_ID,
	IF(ujt.hd_uid IS NOT NULL, ujt.hd_uid,
		IF(ujt.sn_uid IS NOT NULL, ujt.sn_uid, '')
	) AS done_line_uid,
	IF(ujt.hd_uid IS NOT NULL, '0', '1') AS done_line_block_flg,
	IF(ujt.sn_uid IS NOT NULL, '0', '1') AS done_maisonkose_line_block_flg
FROM
	uid_join_table ujt