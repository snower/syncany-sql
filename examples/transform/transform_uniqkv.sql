
/*
去重，重复行横向扩展
    参数1：重复字段key
    参数2：横向表头key
    参数3：值key, 相同位置如有多个值数字则加和，否则最后一个值有效，无第三个参数时统计数量

如以下表格：
 ---------------------------------------------
| id | name   |  order_date  | amount | goods |
 ---------------------------------------------
| 1  | limei  |  2022-01-01  | 5.5    | 青菜   |
| 2  | wanzhi |  2022-01-01  | 8.2    | 白菜   |
| 3  | wanzhi |  2022-01-01  | 2.2    | 青菜   |
 ---------------------------------------------

经过transform$v2h('order_date', 'name', 'amount')后变为：

 -------------------------------------------------------------
| id | name   |  order_date  | amount | goods | limei | wanzhi |
 ---------------------------------------------
| 1  | limei  |  2022-01-01  | 5.5    | 青菜   | 5.5   | 10.4    |
 --------------------------------------------------------------
 */

select transform$uniqkv('create_date', 'name', 'amount') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

insert into test_data (id, name, order_date, amount, goods) values (1, 'limei', '2022-01-01', 5.5, '青菜'), (2, 'wanzhi', '2022-01-01', 8.2, '白菜'), (3, 'wanzhi', '2022-01-01', 2.2, '青菜');

select transform$uniqkv('order_date', 'name', 'amount') from (
    select id, name, order_date, amount, goods from test_data
);

-- 不传第三参数统计数量
select transform$uniqkv('order_date', 'name') from (
    select id, name, order_date, amount, goods from test_data
);

-- 转换为数字保留最新值
select transform$uniqkv('order_date', 'name', 'amount') from (
    select id, name, order_date, convert_string(amount, '%.02f') as amount, goods from test_data
);