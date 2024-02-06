SELECT
  sm.item_code
, sm.item_name
, sm.component_serial_number
, sm.component_code
, sm.component_name
, sm.item_group
, sm.number
, sm.use_start_date
      FROM set_mst AS sm
   INNER JOIN (
      SELECT MAX(time) AS max_time
          , item_code
      FROM set_mst
      GROUP BY
          item_code
   ) AS new_sm
      ON sm.time = new_sm.max_time
      AND sm.item_code = new_sm.item_code;