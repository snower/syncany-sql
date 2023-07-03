
select time_window('1S') as a, time_window('15S') as b, time_window('1M') as c, time_window('15M') as d, time_window('2H') as e, time_window('1d') as f;

select time_window('1S', datetime('2023-07-03 12:24:27')) as a, time_window('15S', datetime('2023-07-03 12:24:27')) as b, time_window('1M', datetime('2023-07-03 12:24:27')) as c,
       time_window('15M', datetime('2023-07-03 12:24:27')) as d, time_window('2H', datetime('2023-07-03 12:24:27')) as e, time_window('1d', datetime('2023-07-03 12:24:27')) as f;

select time_window('1S', datetime('2023-07-03 12:24:27'), 3) as a, time_window('15S', datetime('2023-07-03 12:24:27'), 3) as b, time_window('1M', datetime('2023-07-03 12:24:27'), 3) as c,
       time_window('15M', datetime('2023-07-03 12:24:27'), 3) as d, time_window('2H', datetime('2023-07-03 12:24:27'), 3) as e, time_window('1d', datetime('2023-07-03 12:24:27'), 3) as f;

select time_window('15M', create_time), count(*) from `data/order.csv` group by time_window('15M', create_time) limit 10;