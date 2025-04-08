
select a.goods_name, b.order_id, c.name from `data/goodses.json` a
    left join `data/orders.json` b on b.goods_id=a.goods_id and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/users.json` c on c.uid=b.uid and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0;

select a.goods_name, b.order_id, c.name from `data/goodses.json` a
    inner join `data/orders.json` b on b.goods_id=a.goods_id and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/users.json` c on c.uid=b.uid and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0;

select a.goods_name, max(b.order_id) as latest_order_id, c.name, sum(if(b.order_id is not null, 1, 0)), count(distinct c.uid) as user_cnt, sum(b.amount) as total_amount from `data/goodses.json` a
    left join `data/orders.json` b on b.goods_id=a.goods_id and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/users.json` c on c.uid=b.uid and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0
group by a.goods_name;

select a.goods_name, max(b.order_id) as latest_order_id, c.name, sum(if(b.order_id is not null, 1, 0)) from `data/goodses.json` a
    inner join `data/orders.json` b on b.goods_id=a.goods_id and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/users.json` c on c.uid=b.uid and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0
group by c.name;

select a.goods_name, b.order_id, c.name from `data/goodses.json` a, `data/orders.json` b, `data/users.json` c
where a.status=0 and b.goods_id=a.goods_id and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
  and c.uid=b.uid and c.status=0 and c.uid in (select uid from `data/users.json` where status=0);