-- @TD engine_version: 350
SELECT
    client_id,
    "uid" AS sns_line
FROM kosedmp_prd_secure.line_friend main
WHERE
    client_id LIKE '%'
AND
    NOT EXISTS(
        SELECT 1
        FROM kosedmp_prd_secure.customer sub
        WHERE
            main.uid = sub.sns_line
    )