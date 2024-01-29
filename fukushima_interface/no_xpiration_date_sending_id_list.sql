SELECT
	pel.sending_id,
	pel.name,
	pel.segmentname,
	pnxdl.date,
	pnxdl.externalid,
	pnxdl.param_campagne1,
	pnxdl.param_campagne2,
	pnxdl.param_campagne3,
	pnxdl.param_message1,
	pnxdl.param_message2,
	pnxdl.param_message3,
	pnxdl.cst_id,
	pnxdl.user_id,
	pnxdl.cst_no,
	pnxdl.rank_cd,
	pnxdl.rsrv_div02,
	pnxdl.post_no01,
	pnxdl.post_no02,
	pnxdl.todofuken_nm,
	pnxdl.empty01,
	pnxdl.empty02,
	pnxdl.empty03,
	pnxdl.empty04,
	pnxdl.empty05,
	pnxdl.empty06,
	pnxdl.empty07,
	pnxdl.empty08,
	pnxdl.empty09,
	pnxdl.empty10,
	pnxdl.empty11,
	pnxdl.empty12,
	pnxdl.empty13,
	pnxdl.empty14,
	pnxdl.empty15,
	pnxdl.empty16,
	pnxdl.empty17,
	pnxdl.empty18,
	pnxdl.empty19,
	pnxdl.empty20
FROM
	probance_extract_list pel
LEFT JOIN
	probance_no_xpiration_date_dm_list pnxdl
ON
	pnxdl.sendingid = pel.sending_id
WHERE
	pnxdl.sendingid IS NOT NULL