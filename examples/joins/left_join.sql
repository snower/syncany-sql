
select a.order_id, b.name, c.goods_name from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0;

select a.order_id, b.name, c.goods_name from `data/orders.json` a
    left join `data/users.json` b on a.uid=b.uid and b.status=0 and convert_string(b.gender) in ('男', '女')
    left join `data/goodses.json` c on convert_string(a.goods_id)=convert_string(c.goods_id) and convert_int(c.status)=0  and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0 and b.status=0 and c.status=0 and convert_string(b.gender) in ('男', '女') and c.uid in (select uid from `data/users.json` where status=0);

select a.order_id, b.name, c.goods_name, count(*) as cnt from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0
group by b.name;

select a.order_id, b.name, c.goods_name, count(distinct b.uid) as ucnt, sum(a.amount) from `data/orders.json` a
    left join `data/users.json` b on a.uid=b.uid and b.status=0 and convert_string(b.gender) in ('男', '女')
    left join `data/goodses.json` c on convert_string(a.goods_id)=convert_string(c.goods_id) and convert_int(c.status)=0  and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0 and b.status=0 and c.status=0 and convert_string(b.gender) in ('男', '女') and c.uid in (select uid from `data/users.json` where status=0)
group by c.goods_name;

select distinct b.name, c.goods_name from `data/orders.json` a
    join `data/users.json` b on a.uid=b.uid and b.status=0 and b.gender in ('男', '女')
    left join `data/goodses.json` c on a.goods_id=c.goods_id and c.status=0 and c.uid in (select uid from `data/users.json` where status=0)
where a.status=0;