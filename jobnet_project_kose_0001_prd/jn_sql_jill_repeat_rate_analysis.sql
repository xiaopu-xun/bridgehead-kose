SELECT
  system_code, -- システムコード
  year, -- 年度
  shipped_ym, -- 初回購入年月
  IF(
    diff_days IS NULL,
    '',
    CAST(diff_days AS VARCHAR)
  ) AS diff_days, -- 2回目購入までの日付
  repeat_count, -- 人数
  month_total_count, -- 同月全人数
  ROUND(SUM(repeat_count) OVER (PARTITION BY system_code,shipped_ym
                          ORDER BY system_code,shipped_ym,diff_days
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / CAST(month_total_count AS DOUBLE) * 100,1)
                          AS running_sum_repeat_rate -- 累積人数割合
FROM
  tmp_reporting_013
ORDER BY
  system_code,
  shipped_ym,
  diff_days