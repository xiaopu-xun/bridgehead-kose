SELECT
  customer_code_hash, sns_line
FROM segment_common_after_regist scar
WHERE scar.system_code = 'F'
  AND scar.status = 'VALID'
  AND scar.sns_line <> ''