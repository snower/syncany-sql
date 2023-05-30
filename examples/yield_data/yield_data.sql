use `generate_customize`;

set @aaa=(1, 2, 3);

select yield_array(('a', 'b', 'c')) as a, yield_array(@aaa) as b;

insert into test_data (id, name, order_date, amount, goods) values (1, 'limei', '2022-01-01', 5.5, '青菜'), (2, 'wanzhi', '2022-01-01', 8.2, '白菜'), (3, 'wanzhi', '2022-01-01', 2.2, '青菜');

select goods into @bbb from test_data;

select yield_array(select goods from test_data) as a, yield_array(@bbb) as b;

select yield_array(data) as a from (
    select json$decode("[1, 2, 3]") as data
) t;

select range_count(3), generate_customize$range_count(4);