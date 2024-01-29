SELECT
    COUNT(*) AS count
FROM
    "${target_table}"
WHERE
    TD_TIME_RANGE(
        time,
        TD_TIME_FORMAT(TD_SCHEDULED_TIME(), 'yyyy-MM-dd 00:00:00', 'JST'),
        TD_TIME_FORMAT(TD_TIME_ADD(TD_SCHEDULED_TIME(), '1d', 'JST'), 'yyyy-MM-dd 00:00:00', 'JST'),
        'JST'
    )
