
select concat('a', 'b', 'c') as a, substring('abc', 1, 2) as b, lower('AbC') as c, upper('AbC') as d;

select trim(' a b c   ') as a, repeat('a', 3) as b, reverse('abc') as c, strcmp('abc', 'bc') as d;

select startswith('abc', 'ab') as a, endswith('abc', 'bc') as b, contains('abc', 'bc') as c;

select concat('a', null) as a, concat('a ', datetime('2023-04-02 10:08:06'), ' ', date('2023-04-02 10:08:06'), ' ', time('2023-04-02 10:08:06'), ' ', true, ' ', false, ' ', 1, ' ', 1.23) as b;