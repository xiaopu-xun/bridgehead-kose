WITH sending_user_data AS(
	SELECT
		*
	FROM
		probance_dms_sending_list pdp
/*
2020/06/09仕様変更対応
https://dac-esys.backlog.jp/view/KOSE_GYOUMU-116#comment-78425865
宅配連携（キャンペーンパラメータ3＝電話番号あり：1）の場合は、配送品が宅配便でDMではないので、不着のフラグを読み込まない
WHERE NOT EXISTS (
		SELECT
			deul.cst_id_hash
		FROM
			dms_exclusion_user_list deul
		WHERE
			deul.cst_id_hash = pdp.cst_id
	)
*/
)

SELECT
	sendingid AS SENDINGID,
	date AS DATE,
	externalid AS EXTERNALID,
	param_campagne1 AS PARAM_CAMPAGNE1,
	param_campagne2 AS PARAM_CAMPAGNE2,
	param_campagne3 AS PARAM_CAMPAGNE3,
	param_message1 AS PARAM_MESSAGE1,
	param_message2 AS PARAM_MESSAGE2,
	param_message3 AS PARAM_MESSAGE3,
	cst_id AS CST_ID,
	post_no01 AS POST_NO01,
	post_no02 AS POST_NO02,
	todofuken_nm AS TODOFUKEN_NM
FROM
	sending_user_data
WHERE
	param_campagne2 = '9'
AND
	param_campagne3 = '1'