SELECT a.order_id, b.name, c.goods_name, count(order_id) as cnt, sum(a.amount) as total_amount, a.first_create_time AS order_time, a.next_create_time AS unorder_time
FROM
    (SELECT oha.`id`, oha.`order_id`, oha.`history_type`, oha.`uid`, oha.`goods_id`, oha.`amount`, oha.`status`, oha.`create_time`,
        LEAD(oha.history_type) OVER (PARTITION BY oha.order_id ORDER BY  oha.create_time) AS next_history_type,
        LEAD(oha.create_time) OVER (PARTITION BY oha.order_id ORDER BY  oha.create_time) AS next_create_time,
        IFNULL(MAX(ohb.create_time), oha.create_time) AS first_create_time
    FROM `data/order_historys.json` oha
    LEFT JOIN `data/order_historys.json` ohb ON ohb.order_id=oha.order_id AND ohb.create_time<=oha.create_time AND ohb.history_type>0
    GROUP BY  oha.id ) a
LEFT JOIN `data/users.json` b on a.uid=b.uid
LEFT JOIN `data/goodses.json` c on a.goods_id=c.goods_id
WHERE (a.next_history_type>0 OR a.next_history_type is null) group by order_id ORDER BY order_id;

select oha.`id`, oha.`order_id`,oha.`history_type`,
    LEAD(oha.history_type) OVER (PARTITION BY oha.order_id ORDER BY  oha.create_time) AS next_history_type,
    LEAD(oha.create_time) OVER (PARTITION BY oha.order_id ORDER BY  oha.create_time) AS next_create_time,
    IFNULL(MAX(ohb.create_time), oha.create_time) AS first_create_time
FROM `data/order_historys.json` oha
LEFT JOIN `data/order_historys.json` ohb ON ohb.order_id=oha.order_id AND ohb.create_time<=oha.create_time AND ohb.history_type>0
Where order_id=1 group by oha.id;