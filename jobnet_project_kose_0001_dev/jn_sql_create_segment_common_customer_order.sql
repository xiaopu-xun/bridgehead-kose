-- @TD engine_version: 350
WITH gift_set_order AS (
    SELECT
        order_code
    FROM kosedmp_prd_secure.segment_common_order_detail scod
    INNER JOIN kosedmp_prd_secure.gift_set_mst gsm
        ON scod.sku_code = gsm.sku_code
        AND scod.product_code = gsm.product_code
        AND scod.jan_code = gsm.jan_code
    GROUP BY order_code
)
SELECT
      sco.customer_code_hash
    , COUNT(CASE WHEN sco.gift = '1' THEN 1 ELSE NULL END) AS wrapping_buy_count
    , COUNT(CASE WHEN EXISTS(SELECT 1 FROM gift_set_order gso WHERE sco.order_code = gso.order_code) THEN 1 ELSE NULL END) AS giftset_buy_count
    , MAX(CAST(sco.accepted_timestamp AS bigint)) AS last_buy_timestamp
    , COUNT(1) as buytimes_ec
FROM kosedmp_prd_secure.segment_common_order AS sco
WHERE
    EXISTS (
        SELECT 1
        FROM kosedmp_prd_secure.segment_common_after_regist AS scar
        WHERE
            sco.customer_code_hash = scar.customer_code_hash
    )
GROUP BY
    sco.customer_code_hash