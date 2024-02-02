
set @a = 1;

select @a as a, @a := @a + 1 as b, sum(@a := @a + 1) as c, if(@a < 2, @a, @a := @a + 1) as d, @a as e from `data/orders.json` group by order_id;

select row_index() as a, row_last(order_id) as b, order_id as c, @a := @a + 1 as d from `data/orders.json`;

select row_index() as a, row_last(order_id) as b, order_id as c, @a := @a + 1 as d from `data/orders.json`;

-- 计算用户连续订单信息

set @sindex = 1;

select uid, count(*) as cnt, sum(amount) as amount, min(order_at) as start_at, max(finish_at) as end_at from (
    select *, if(uid = last_uid and TIMESTAMPDIFF('SECOND', last_finish_at, order_at) <= 1, @sindex, @sindex := @sindex + 1) as sindex from (
        select *, row_last(uid) as last_uid, row_last(finish_at) as last_finish_at from (
            select order_id, uid, amount, order_at, finish_at from `data/orders.json` order by uid, order_at, finish_at
        ) aaa
    ) aa
) a group by uid, sindex;