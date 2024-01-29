WITH segment_common_item_allocation_number AS (
SELECT
  'E' AS system_code,
  ian.year,
  ian.syohin_fg,
  ian.syohin_fg_gosu,
  ian.n_cd,
  ian.hanbai_name_jpn,
  ian.hatubai_date,
  ian.price,
  ian.gosu,
  ian.limited_kbn,
  ian.ec_allocation_number
FROM
  item_allocation_number AS ian
  INNER JOIN (
    SELECT
      MAX(time) AS max_time,
      year
      FROM item_allocation_number
      GROUP BY
       year
    ) AS new_ian
    ON ian.time = new_ian.max_time
    AND ian.year = new_ian.year
)

SELECT
  'E' AS system_code,
  TD_TIME_FORMAT(${start_date}, 'yyyy-MM', 'JST') AS date,
  ian.syohin_fg,
  ian.n_cd,
  ian.hanbai_name_jpn,
  ian.hatubai_date,
  ian.price,
  ian.gosu,
  ian.limited_kbn as kbn,
  IF(
    odim.num IS NULL,
    0,
    odim.num
  ) AS order_quantity
FROM segment_common_item_allocation_number AS ian LEFT
JOIN (
    SELECT
      IF(
        CAST(substr(TD_TIME_FORMAT(${start_date}, 'yyyy-MM', 'JST'),6,2) AS bigint)  >= 4,
        substr(TD_TIME_FORMAT(${start_date}, 'yyyy-MM', 'JST'),1,4),
        CAST(CAST(substr(TD_TIME_FORMAT(${start_date}, 'yyyy-MM', 'JST'),1,4) AS bigint) - 1 AS VARCHAR)
      ) AS aggregate_year,
      ian2.n_cd,
      CAST( SUM(
        CASE
          WHEN scod.user_order_detail_type = 'RETURN_ORDER' THEN scod.quantity * - 1
          WHEN scod.user_order_detail_type = 'NORMAL_ORDER' THEN scod.quantity
          ELSE 0
        END
      ) AS integer) AS num
    FROM
      jill_segment_common_order_detail scod
      LEFT JOIN jill_segment_common_order sco
      ON sco.order_code = scod.order_code

      LEFT JOIN segment_common_item_mst im
      ON im.hinmoku_cd = scod.sku_code

      LEFT JOIN segment_common_item_allocation_number AS ian2
      ON ian2.n_cd = im.n_cd

    WHERE
        sco.shipped_timestamp <> ''
        AND TD_TIME_RANGE(CAST(sco.shipped_timestamp AS bigint)/ 1000,
            TD_TIME_FORMAT(${start_date}, 'yyyy-MM', 'JST'),
            TD_TIME_FORMAT(${end_date}, 'yyyy-MM', 'JST'),
            'JST')
        AND ian2.n_cd IS NOT NULL
    GROUP BY
        ian2.n_cd
    ORDER BY
        ian2.n_cd
) odim
ON ian.n_cd = odim.n_cd
AND ian.year = odim.aggregate_year;