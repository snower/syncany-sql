select uid, latest_order_id, cnt, total_amount from (
    select uid, max(order_id) as latest_order_id, count(*) as cnt, sum(amount) as total_amount from `data/orders.json` group by uid
) a where total_amount>20;

select order_id, uid, amount, (select count(*) from `data/order_historys.json` c where c.order_id=a.order_id and status=0) as history_count,
       exists(select count(*) from `data/order_historys.json` c where c.order_id=a.order_id and status=0) as history_exists
    from `data/orders.json` as a where (select sum(amount) from `data/orders.json` as b where a.order_id=b.order_id and status=0)<5;

select goods_id, goods_name, (select count(*) from `data/orders.json` c where c.goods_id=a.goods_id and status=0) as order_count,
       exists(select count(*) from `data/orders.json` c where c.goods_id=a.goods_id and status=0) as order_exists
    from `data/goodses.json` as a;

select order_id, uid, amount, (select count(*) from `data/order_historys.json` c where c.order_id+1=a.order_id+1 and status=0) as history_count,
       exists(select count(*) from `data/order_historys.json` c where c.order_id=a.order_id and status=0) as history_exists
    from `data/orders.json` as a where (select sum(amount) from `data/orders.json` as b where a.order_id=(b.order_id+1)-1 and status=0)<5;

select goods_id, goods_name, (select count(*) from `data/orders.json` c where c.goods_id+1=a.goods_id+1 and status=0) as order_count,
       exists(select count(*) from `data/orders.json` c where c.goods_id=(a.goods_id+1)-1 and status=0) as order_exists
    from `data/goodses.json` as a;

select order_id, count(*) as cnt, sum(amount) as total_amount from `data/orders.json` where order_id in (select (1, 2, 3) as order_id) group by order_id;

select uid, (
    select sum(amount) as total_amount from `data/orders.json` as b where a.uid=b.uid group by b.uid
) as total_amount from `data/users.json` a where uid in (select uid from `data/orders.json`)
                                             and uid in (select uid from `data/order_historys.json` group by uid);

select order_id, uid, amount, (select count(*) from `data/order_historys.json` c where c.order_id+1=a.order_id+1 and status=0)>0 as has_history,
       exists(select count(*) from `data/order_historys.json` c where c.order_id=a.order_id and status=0) as history_exists
    from `data/orders.json` as a where (
        select sum(amount) from `data/orders.json` as b where a.order_id=(b.order_id+1)-1 and status=0 and exists(
            select uid from `data/users.json` d where b.uid=d.uid
        )
    )<5;

select order_id, uid, amount, (select count(*) from `data/order_historys.json` where order_id+1=a.order_id+1 and status=0)>0 as has_history,
       exists(select count(*) from `data/order_historys.json` where order_id=a.order_id and status=0) as history_exists
    from `data/orders.json` as a where (
        select sum(amount) from `data/orders.json` as b where a.order_id=(b.order_id+1)-1 and status=0 and exists(
            select uid from `data/users.json` where b.uid=uid
        )
    )<5;