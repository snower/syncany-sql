
select b.order_id, a.name, c.goods_name from `data/users.json` a
    right join `data/orders.json` b on b.uid=a.uid and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/goodses.json` c on c.goods_id=b.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0 and a.gender in ('男', '女');

select b.order_id, a.name, c.goods_name, count(*) as cnt from `data/users.json` a
    right join `data/orders.json` b on b.uid=a.uid and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/goodses.json` c on c.goods_id=b.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0 and a.gender in ('男', '女') group by a.name;

select b.order_id, a.name, c.goods_name, count(distinct a.uid) as cnt, sum(b.amount) as total_amount from `data/users.json` a
    right join `data/orders.json` b on b.uid=a.uid and b.status=0 and b.uid in (select uid from `data/users.json` where status=0)
    left join `data/goodses.json` c on c.goods_id=b.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0 and a.gender in ('男', '女') group by c.goods_name;