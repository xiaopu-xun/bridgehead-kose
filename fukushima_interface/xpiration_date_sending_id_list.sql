SELECT
	pel.sending_id,
	pel.name,
	pel.segmentname,
	pxdl.date,
	pxdl.externalid,
	pxdl.param_campagne1,
	pxdl.param_campagne2,
	pxdl.param_campagne3,
	pxdl.param_message1,
	pxdl.param_message2,
	pxdl.param_message3,
	pxdl.cst_id,
	pxdl.user_id,
	pxdl.cst_no,
	pxdl.rank_cd,
	pxdl.rsrv_div02,
	pxdl.post_no01,
	pxdl.post_no02,
	pxdl.todofuken_nm,
	pxdl.expiration_date,
	pxdl.empty02,
	pxdl.empty03,
	pxdl.empty04,
	pxdl.empty05,
	pxdl.empty06,
	pxdl.empty07,
	pxdl.empty08,
	pxdl.empty09,
	pxdl.empty10,
	pxdl.empty11,
	pxdl.empty12,
	pxdl.empty13,
	pxdl.empty14,
	pxdl.empty15,
	pxdl.empty16,
	pxdl.empty17,
	pxdl.empty18,
	pxdl.empty19,
	pxdl.empty20
FROM
	probance_extract_list pel
LEFT JOIN
	probance_xpiration_date_dm_list pxdl
ON
	pxdl.sendingid = pel.sending_id
WHERE
	pxdl.sendingid IS NOT NULL