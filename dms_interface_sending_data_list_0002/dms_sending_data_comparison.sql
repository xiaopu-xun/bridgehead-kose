WITH prep AS(
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
		dms_sending_data_spot
	EXCEPT
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
		dms_previous_sending_data_spot
)
SELECT
	l.CST_ID AS CST_ID_HASH,
	r.customer_code_3 AS CST_ID,
	r.last_name AS ADDRESS_CST_LNM,
	r.first_name AS ADDRESS_CST_FNM,
	l.POST_NO01 AS POST_NO01,
	l.POST_NO02 AS POST_NO02,
	l.TODOFUKEN_NM AS TODOFUKEN_NM,
	r.city AS ADDRESS01,
	r.address1 AS ADDRESS02,
	r.address2 AS ADDRESS03,
	l.PARAM_CAMPAGNE1 AS DM_CODE,
	'' AS TEL --TELなしセグメントであるため空値になる
FROM
	prep AS l
	INNER JOIN ${td.pii_database}.segment_common_after_regist_pii AS r
	ON l.CST_ID = r.customer_code_hash
WHERE
	l.PARAM_CAMPAGNE1 ='${td.each.DM_CODE}'
