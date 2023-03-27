
insert into v2h_data select transform$v2h('name', 'create_date', 'amount') from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id
);

/*
横向表转为纵向表
    参数1：横向表头
    参数2：纵向统计值
    参数3：值，不传递不保留值

如以下表格：
 --------------------------------
| order_date   | limei  | wanzhi |
 --------------------------------
| 2022-01-01   | 5.5    | 8.2    |
| 2022-01-02   | 4.3    | 1.8    |
 --------------------------------

经过transform$v2h('name', 'order_date', 'amount')后变为：

--------------------------------
| name   |  order_date  | amount |
--------------------------------
| limei  |  2022-01-01  | 5.5    |
| wanzhi |  2022-01-01  | 8.2    |
| limei  |  2022-01-02  | 4.3    |
| wanzhi |  2022-01-02  | 1.8    |
 --------------------------------
 */

select transform$h2v('name', 'create_date', 'amount') from (
    select `create_date`, `黄豆网`, `青菜网`, `火箭网`, `卫星网` from v2h_data
);

insert into test_data (order_date, limei, wanzhi) values ('2022-01-01', 5.5, 8.2), ('2022-01-02', 4.3, 1.8);

select transform$h2v('name', 'order_date', 'amount') from (
    select order_date, limei, wanzhi from test_data
);

select transform$h2v('name', 'order_date') from (
    select order_date, limei, wanzhi from test_data
);