SELECT
    client_id,
    "uid" AS sns_line
FROM line_friend main
WHERE
    client_id LIKE '%'
AND
    NOT EXISTS(
        SELECT 1
        FROM customer sub
        WHERE
            main.uid = sub.sns_line
    )