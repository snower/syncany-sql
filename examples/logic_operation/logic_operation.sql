
select 1 < 2 as a, 1 <= '2' as b, '123' > '23' as c, true >= 1 as d, false != 1 as f;

select 1 + 2 * 3 > 5 as a, '1' + 2 * '3' < 5 as b;

select 1 = 1 as a, 1 = '1' as b, 1 = 'a' as c, 1 = '1a' as d;

select not 1 < 2 as a, not 1 <= '2' as b, not '123' > '23' as c, not true >= 1 as d, not false <> 1 as f;

select not 1 + 2 * 3 > 5 as a, not '1' + 2 * '3' < 5 as b, not true as c, not 1 as d;

select 'abc' like '%' as a, 'abc' like '%%' as b, 'abc' like '%a%' as c, 'abc' like '%bc' as d, 'abc' like 'ab%' as e, 'abc' like 'ac%' as f;

select 'abc' not like '%' as a, 'abc' not like '%%' as b, 'abc' not like '%a%' as c, 'abc' not like '%bc' as d, 'abc' not like 'ab%' as e, 'abc' not like 'ac%' as f;

select 'a' in ('a', 'b', 'c') as a, 1 in ('a', 'b', 'c') as b, 1 in (1, 2, 3) as c, 'a' in (1, 2, 3) as d;

select 'a' not in ('a', 'b', 'c') as a, 1 not in ('a', 'b', 'c') as b, 1 not in (1, 2, 3) as c, 'a' not in (1, 2, 3) as d;

select if(amount > 0 and status=0, 1, 0) as a, case when amount <= 0 then 'A' when amount > 0 and amount < 1 then 'B' when amount between 1 and 10 'C' else 'D' end as b from `data/orders.json` where uid=1;

select sum(amount) as a, if(sum(amount) > 0, 1, 0) as b, case when sum(amount) <= 0 then 'A' when sum(amount) > 0 and sum(amount) < 1 then 'B' when sum(amount) between 1 and 100 'C' else 'D' end as c from `data/orders.json`;

select order_id, uid, goods_id, amount from `data/orders.json` where uid != 3 and uid != 4 and uid > 0 and uid < 10 order by order_id desc limit 2, 1;

select * from `data/orders.json` where uid != 3 and uid != 4 and uid > 0 and uid < 10 and status=0 order by order_id desc limit 1 offset 2;