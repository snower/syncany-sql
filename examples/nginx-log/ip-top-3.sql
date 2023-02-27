#!/bin/env syncany-sql

SELECT
    ip, cnt
FROM
    (SELECT
        seg0 AS ip, COUNT(*) AS cnt
    FROM
        `file://data/access.log?sep= `
    GROUP BY seg0) a
ORDER BY cnt DESC
LIMIT 3;