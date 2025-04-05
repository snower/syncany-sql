
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

select a.code, c.name, d.goods_name from `data/devices.json` a
    left join `data/orders.json` b on a.uid=b.uid and a.out_order_no=b.`order_id[int]`
    left join `data/users.json` c on a.uid=c.uid
    left join `data/goodses.json` d on b.goods_id=d.goods_id
where d.goods_name is not null;

select a.order_id, c.name from `data/orders.json` a
    left join `data/devices.json` b on a.order_id=b.`out_order_no[varchar]` and b.order_type=1
    left join `data/services.json` c on b.code=c.device_code
where c.name is not null;

select a.code, c.name, d.goods_name from `data/devices.json` a
    left join `data/orders.json` b on a.uid=b.uid and concat(a.out_order_no, 'HHH')=concat(b.`order_id[int]`, 'HHH')
    left join `data/users.json` c on a.uid=c.uid
    left join `data/goodses.json` d on b.goods_id=d.goods_id
where d.goods_name is not null;

select order_id, name, goods_name, history_type, count(*) as cnt, sum(amount) as total_amount from (
    select a.name, b.order_id, c.history_type, c.amount, d.goods_name from `data/users.json` a
        left join `data/orders.json` b on a.uid = b.uid
        left join `data/order_historys.json` c on b.order_id = c.order_id
        left join `data/goodses.json` d on b.goods_id = d.goods_id
) aa where order_id <= 3 group by order_id, history_type order by order_id;