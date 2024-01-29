/* segment_common_after_registテーブルの項目全て(システムコードはB/C/E/F/G/J/Lのみ) */
SELECT
  *
FROM
  segment_common_after_regist
WHERE
  system_code IN ('B', 'C', 'E', 'F', 'G', 'J', 'L')