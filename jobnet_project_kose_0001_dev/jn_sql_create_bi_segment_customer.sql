-- @TD engine_version: 350
/* segment_common_after_registテーブルの項目全て(システムコードはE/F/G/Jのみ) */
SELECT
  *
FROM
  kosedmp_prd_secure.segment_common_after_regist
WHERE
  system_code IN ('E', 'F', 'G', 'J')