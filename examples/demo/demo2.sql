
SELECT yield_array(a.sites) as site_id, b.name as site_name FROM `data/demo.json` a
    JOIN `data/sites.json` b ON a.sites = b.site_id;

select * from `data/orders.json` order by amount desc limit 2;

select a.order_id, a.site_id, a.amount * 100 as amount from `data/orders.json` a order by site_id desc, a.order_id desc limit 2;

select a.order_id, a.site_id, a.amount * 100 as amount from `data/orders.json` a order by a.amount * 100 desc limit 2;

execute `execute.sql`;

execute `json/demo.json`;

select site_id, site_name, site_amount, timeout_at, vip_timeout_at from execute_demo_sql_data;

select site_id, site_name, site_amount, timeout_at, vip_timeout_at from execute_demo_json_data;