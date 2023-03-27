
/*
纵向表转为横向表
    参数1：横向表头
    参数2：纵向统计值
    参数3：值，相同位置如有多个值数字则加和，否则最后一个值有效，无第三个参数时统计数量

如以下表格：
 ---------------------------------------------
| id | name   |  order_date  | amount | goods |
 ---------------------------------------------
| 1  | limei  |  2022-01-01  | 5.5    | 青菜   |
| 2  | wanzhi |  2022-01-01  | 8.2    | 白菜   |
| 3  | wanzhi |  2022-01-01  | 2.2    | 青菜   |
 ---------------------------------------------

经过transform$v2h('name', 'order_date', 'amount')后变为：

 --------------------------------
| order_date   | limei  | wanzhi |
 --------------------------------
| 2022-01-01   | 5.5    | 8.2    |
 --------------------------------
 */

select transform$v2h('name', 'create_date', 'amount') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

select transform$v2h('name', 'create_date') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

insert into test_data (id, name, order_date, amount, goods) values (1, 'limei', '2022-01-01', 5.5, '青菜'), (2, 'wanzhi', '2022-01-01', 8.2, '白菜'), (3, 'wanzhi', '2022-01-01', 2.2, '青菜');

select transform$v2h('name', 'order_date', 'amount') from (
    select id, name, order_date, amount from test_data
);

-- 无第三参数统计数量
select transform$v2h('name', 'order_date') from (
    select id, name, order_date, amount from test_data
);

-- 非数字值最后一个值有效
select transform$v2h('name', 'order_date', 'goods') from (
    select id, name, order_date, goods from test_data
);

-- 不加和则转为字符串
select transform$v2h('name', 'order_date', 'amount') from (
    select id, name, order_date, convert_string(amount, '%.02f') as amount from test_data
);