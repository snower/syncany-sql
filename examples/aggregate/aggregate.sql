
use `aggregate_customize`;


select count(*) as cnt, sum(amount) as total_amount, avg(amount) as avg_amount, min(amount) as min_amount, max(amount) as max_amount from `data/orders.json` where status=0;

select uid, count(*) as cnt, sum(amount) as total_amount, avg(amount) as avg_amount, min(amount) as min_amount, max(amount) as max_amount from `data/orders.json` where status=0 group by uid;

select distinct uid, order_id, amount from `data/orders.json` where status=0;

select count(distinct uid) as ucnt from `data/orders.json` where status=0;

select uid, count(distinct goods_id) as gcnt from `data/orders.json` where status=0 group by uid;

select uid, count(distinct goods_id) as gcnt from `data/orders.json` where status=0 group by uid having uid=1;

select uid, count(distinct goods_id) as gcnt from `data/orders.json` where status=0 group by uid having gcnt>1;

select uid, sum(amount) / count(*) / 100 as avg_amount from `data/orders.json` where status=0 group by uid;

select b.name, c.goods_name, count(*) as cnt from `data/orders.json` a join `data/users.json` b on a.uid=b.uid
    join `data/goodses.json` c on a.goods_id=c.goods_id where a.status=0;

select b.name, c.goods_name, count(*) as cnt from `data/orders.json` a join `data/users.json` b on a.uid=b.uid
    join `data/goodses.json` c on a.goods_id=c.goods_id where a.status=0 group by b.name, c.goods_name;

select distinct b.name, c.goods_name from `data/orders.json` a join `data/users.json` b on a.uid=b.uid
    join `data/goodses.json` c on a.goods_id=c.goods_id where a.status=0;

select b.name, c.goods_name, count(distinct b.name) as cnt from `data/orders.json` a join `data/users.json` b on a.uid=b.uid
    join `data/goodses.json` c on a.goods_id=c.goods_id where a.status=0 group by c.goods_name;

-- 自定义聚合计算函数
select aggregate_unique(uid) as uids, aggregate_join(order_id) as order_ids from `data/orders.json` where status=0;

select uid, aggregate_unique(goods_id) as goods_ids, aggregate_join(order_id) as order_ids from `data/orders.json` where status=0 group by uid;

select uid, length(aggregate_join(order_id)) / 100 as avg_amount from `data/orders.json` where status=0 group by uid;

select b.name, c.goods_name, aggregate_unique(b.name) as names, aggregate_join(c.goods_name) as goods_namees from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid
    join `data/goodses.json` c on a.goods_id=c.goods_id where a.status=0;

select b.name, c.goods_name, aggregate_unique(b.name) as names, aggregate_join(c.goods_name) as goods_namees from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid
    join `data/goodses.json` c on a.goods_id=c.goods_id where a.status=0 group by b.name, c.goods_name;