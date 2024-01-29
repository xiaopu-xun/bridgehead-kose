-- @TD engine_version: 350
SELECT DISTINCT
	scar.customer_code_hash AS CST_ID, 
	scar.sns_line AS done_line_uid, 
	'0' AS done_line_block_flg, 
	'0' AS done_maisonkose_line_block_flg 
FROM 
	segment_common_after_regist scar 
WHERE 
    scar.system_code IN ('F', 'J') 
	AND scar.customer_code_hash IS NOT NULL 
	AND scar.customer_code_hash <> '' 
	AND scar.sns_line IS NOT NULL 
	AND scar.sns_line <> ''