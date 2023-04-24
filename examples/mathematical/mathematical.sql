
select 2 + 1 as a, 2 - 1 as b, 2 * 1 as c, 2 / 1 as d;

select 2 & 1 as a, 2 | 1 as b, 2 ^ 1 as c, ~2 as d;

select '2' + 1 as a, '2022-10-11' / 1 as b, '111abc' * 2 as c;

select null + 1 as a, true + 1 as b, false + 1 as c;

select datetime('2023-04-02 10:08:06') / 2 as a, date('2023-04-02 10:08:06') / 2 as b, time('2023-04-02 10:08:06') / 2 as c;

select add((1, 2, 3), 1) as a, mul((1, 2, 3), 4) as b;

select 1 + 2 * 3 - 2 as a, (1 + 2) * (3 - 2) as b;