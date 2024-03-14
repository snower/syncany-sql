use `window_customize`;

select order_id, uid, goods_id, count(*) over() as order_cnt, count(*) over(partition by uid) as uorder_cnt,
count(distinct goods_id) over(partition by uid) as goods_cnt from `data/orders.json` where status=0;

select order_id, uid, goods_id, count(*) over(order by order_id desc) as order_cnt, count(*) over(partition by uid order by order_id desc) as uorder_cnt,
count(distinct goods_id) over(partition by uid order by order_id desc) as goods_cnt from `data/orders.json` where status=0;

select order_id, uid, goods_id, row_number() over(order by amount desc) as rn, rank() over(order by amount desc) as rk,
dense_rank() over(order by amount desc) as drk, percent_rank() over(order by order_id desc) as prk,
cume_dist() over(order by amount desc) as cd from `data/orders.json` where status=0;

select order_id, uid, goods_id, row_number() over(partition by uid order by amount desc) as rn, rank() over(partition by uid order by amount desc) as rk,
dense_rank() over(partition by uid order by amount desc) as drk, percent_rank() over(partition by uid order by amount desc) as prk,
cume_dist() over(partition by uid order by amount desc) as cd from `data/orders.json` where status=0;

select order_id, uid, goods_id, row_number() over(partition by uid order by amount desc) + rank() over(partition by uid order by amount desc) as a,
dense_rank() over(partition by uid order by amount desc) * percent_rank() over(partition by uid order by amount desc) / 10 as b,
cume_dist() over(partition by uid order by amount desc) * 100 as c from `data/orders.json` where status=0;

select a.order_id, b.name, c.goods_name, count(*) over() as order_cnt, count(*) over(partition by b.name) as uorder_cnt,
count(distinct c.goods_name) over(partition by b.name) as goods_cnt from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where status=0;

select a.order_id, b.name, c.goods_name, count(*) over(order by order_id desc) as order_cnt, count(*) over(partition by b.name order by order_id desc) as uorder_cnt,
count(distinct c.goods_name) over(partition by b.name order by order_id desc) as goods_cnt from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where status=0;

select a.order_id, b.name, c.goods_name, row_number() over(order by amount desc) as rn, rank() over(order by amount desc) as rk,
dense_rank() over(order by amount desc) as drk, percent_rank() over(order by order_id desc) as prk,
cume_dist() over(order by amount desc) as cd from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where status=0;

select a.order_id, b.name, c.goods_name, row_number() over(partition by b.name order by amount desc) as rn, rank() over(partition by b.name order by amount desc) as rk,
dense_rank() over(partition by b.name order by amount desc) as drk, percent_rank() over(partition by b.name order by amount desc) as prk,
cume_dist() over(partition by b.name order by amount desc) as cd from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where status=0;

select a.order_id, b.name, c.goods_name, row_number() over(partition by b.name order by amount desc) + rank() over(partition by b.name order by amount desc) as a,
dense_rank() over(partition by b.name order by amount desc) * percent_rank() over(partition by b.name order by amount desc) / 10 as b,
cume_dist() over(partition by b.name order by amount desc) * 100 as c from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where status=0;

--自定义函数

select order_id, uid, goods_id, window_aggregate_unique(uid) over() as va, window_aggregate_unique(uid) over(partition by uid) as vb,
window_aggregate_unique(uid) over(partition by uid order by amount desc) as vc, window_aggregate_join(uid) over(partition by uid order by amount desc) as vd from `data/orders.json` where status=0;

select a.order_id, b.name, c.goods_name, window_aggregate_unique(b.name) over() as va, window_aggregate_unique(b.name) over(partition by uid) as vb,
window_aggregate_unique(b.name) over(partition by uid order by amount desc) as vc, window_aggregate_join(b.name) over(partition by uid order by amount desc) as vd from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where status=0;

select order_id, uid, goods_id, row_number() over() as rn, lag(order_id) over() as lag, lead(order_id) over() as lead from `data/orders.json` where status=0;

select order_id, uid, goods_id, row_number() over(order by order_id) as rn, lag(order_id, 2, 0) over(order by order_id) as lag,
 lead(order_id, 2, 0) over(order by order_id desc) as lead from `data/orders.json` where status=0 order by order_id;

select order_id, uid, goods_id, row_number() over(order by order_id) + 2 as rn, (lag(order_id, 2, 0) over(order by order_id)
    + lead(order_id, 2, 0) over(order by order_id desc)) * 3 as lead from `data/orders.json` where status=0 order by order_id;

select order_id, uid, goods_id, first_value(uid) over w as fuid, last_value(uid) over w as luid,
    nth_value(uid, 2) over w as nuid, ntile(4) over w as bi
    from `data/orders.json` window w as (partition by goods_id order by order_id) order by order_id;