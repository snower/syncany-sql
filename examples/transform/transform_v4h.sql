
/*
转为kEY-VALUE纵向表
        参数1：key
        参数2：value

如以下表格：
 --------------------
| id | name   |  age |
 --------------------
| 1  | limei  |  18  |
| 2  | wanzhi |  22  |
 --------------------

经过transform$v4h('key', 'value', 'name')后变为：

 -------------------------
| key   | value  | name   |
 -------------------------
| id    | 1      | limei  |
| age   | 18     | limei  |
| id    | 2      | wanzhi |
| age   | 22     | wanzhi |
 -------------------------
 */

select transform$v4h('key', 'value') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

select transform$v4h('key', 'value', 'site_id') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

insert into test_data (id, name, age) values (1, 'limei', 18), (2, 'wanzhi', 22);

select transform$v4h('key', 'value') from (
    select id, name, age from test_data
);

select transform$v4h('key', 'value', 'name') from (
    select id, name, age from test_data
);