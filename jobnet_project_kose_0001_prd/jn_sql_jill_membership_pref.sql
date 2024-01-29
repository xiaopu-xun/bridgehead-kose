-- 県ごとの会員数と累計
-- tableau_report_sns_0002 ec_num.sql に倣う
WITH joined_on AS (
  SELECT d, state, membership, SUM(membership) OVER (PARTITION BY state ORDER BY d ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sales_running_sum
  FROM
  (
    SELECT 
      TD_TIME_FORMAT(CAST(systemcreatedate AS bigint)/ 1000,'yyyy-MM-dd','JST') AS d, 
      state,
      count(1) AS membership
    FROM  segment_common_after_regist
    WHERE
      system_code = 'E'
      AND systemcreatedate <> ''
      AND systemcreatedate IS NOT NULL
    GROUP BY 
      1,2
  ) t
  ORDER BY 2,1
)

select 
 j.d                  AS date,
 p.id                 AS pref_id, 
 j.state              AS pref_name,
 j.membership         AS membership_day,
 j.sales_running_sum  AS membership_runningsum
from joined_on j
left join prefecture_mst p -- j.state に null存在
on p.name = j.state
order by 1,2