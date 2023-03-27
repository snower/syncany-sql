
/*
把kEY-VALUE转为横向表

如以下表格：
 ----------------------
| key      | name   | value |
 ----------------------
| order_id | limei  | 1     |
| goods    | limei  | 青菜   |
| age      | limei  | 18    |
| order_id | wanzhi | 2     |
| goods    | wanzhi | 白菜   |
| age      | wanzhi | 22    |
| order_id | wanzhi | 3     |
| goods    | wanzhi | 青菜   |
| age      | wanzhi | 22    |
 ----------------------

经过transform$h4v('name', 'key', 'value')后变为：

 ------------------------
| name | id | goods | age |
 -------------------------
| limei  | 1  | 青菜 | 18  |
| wanzhi | 2  | 白菜 | 22  |
| wanzhi | 2  | 青菜 | 22  |
 ------------------------
 */

insert into v4h_data1 select transform$v4h('key', 'value') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

select transform$h4v('key', 'value') from (
    select `key`, `value` from v4h_data1
);

insert into v4h_data2 select transform$v4h('key', 'value', 'site_id') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

select transform$h4v('key', 'value', 'site_id') from (
    select `key`, `value`, `site_id` from v4h_data2
);

insert into test_data1 (`key`, `name`, `value`) values ('order_id', 'limei', '1'), ('goods', 'limei', '青菜'), ('age', 'limei', '18'),
                                                      ('order_id', 'wanzhi', '2'), ('goods', 'wanzhi', '白菜'), ('age', 'wanzhi', '22'),
                                                      ('order_id', 'wanzhi', '3'), ('goods', 'wanzhi', '青菜'), ('age', 'wanzhi', '22');

select transform$h4v('key', 'value', 'name') from (
    select `key`, `name`, `value` from test_data1
);

insert into test_data2 (`key`, `value`) values ('order_id', '1'), ('name', 'limei'), ('goods', '青菜'), ('age', '18'),
                                                      ('order_id', '2'), ('name', 'wanzhi'), ('goods', '白菜'), ('age', '22'),
                                                      ('order_id', '3'), ('name', 'wanzhi'), ('goods', '青菜'), ('age', '22');

select transform$h4v('key', 'value') from (
    select `key`, `value` from test_data2
);
