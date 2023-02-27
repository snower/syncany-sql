#!/bin/env syncany-sql

SELECT
    a.site_id,
    b.name AS site_name,
    IF(c.site_amount > 0, c.site_amount, 0) AS site_amount,
    MAX(a.timeout_at) AS timeout_at,
    MAX(a.vip_timeout_at) AS vip_timeout_at,
    now() as `created_at?`
FROM
    (SELECT
        YIELD_DATA(sites) AS site_id,
            IF(vip_type = '2', GET_VALUE(rules, 0, 'timeout_time'), '') AS timeout_at,
            IF(vip_type = '1', GET_VALUE(rules, 0, 'timeout_time'), '') AS vip_timeout_at
    FROM
        `data/demo.json`
    WHERE
        start_date >= '2021-01-01') a
        JOIN
    `data/sites.json` b ON a.site_id = b.site_id
        JOIN
    (SELECT
        site_id, SUM(amount) AS site_amount
    FROM
        `data/orders.json`
    WHERE
        status <= 0
    GROUP BY site_id) c ON a.site_id = c.site_id
GROUP BY a.site_id;