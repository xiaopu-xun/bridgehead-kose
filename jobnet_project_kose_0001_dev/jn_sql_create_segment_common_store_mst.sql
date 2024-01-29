-- @TD engine_version: 350
SELECT
  sm.store_id
, sm.store_name
, sm.area_name
, sm.base_name
, sm.branch_name
, sm.channel_name
, sm.zip_code
, sm.company_name_1
, sm.company_name_2
, sm.shipment_restriction_kbn
      FROM kosedmp_prd_secure.store_mst AS sm
   INNER JOIN (
      SELECT MAX(time) AS max_time
          , store_id
      FROM kosedmp_prd_secure.store_mst
      GROUP BY
          store_id
   ) AS new_sm
      ON sm.time = new_sm.max_time
      AND sm.store_id = new_sm.store_id