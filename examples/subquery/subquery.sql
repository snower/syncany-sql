select uid, latest_order_id, cnt, total_amount from (
    select uid, max(order_id) as latest_order_id, count(*) as cnt, sum(amount) as total_amount from `data/orders.json` group by uid
) a where total_amount>20;

select order_id, uid, amount, (select count(*) from `data/order_historys.json` c where c.order_id=a.order_id and status=0) as history_count,
       exists(select count(*) from `data/order_historys.json` c where c.order_id=a.order_id and status=0) as history_exists
    from `data/orders.json` as a where (select sum(amount) from `data/orders.json` as b where a.order_id=b.order_id and status=0)<5;

select goods_id, goods_name, (select count(*) from `data/orders.json` c where c.goods_id=a.goods_id and status=0) as order_count,
       exists(select count(*) from `data/orders.json` c where c.goods_id=a.goods_id and status=0) as order_exists
    from `data/goodses.json` as a;