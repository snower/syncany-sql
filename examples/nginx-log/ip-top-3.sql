#!/bin/env syncany-sql

SELECT seg0 AS ip, COUNT(*) AS cnt FROM `file://data/access.log?sep= ` GROUP BY seg0 ORDER BY cnt DESC LIMIT 3;