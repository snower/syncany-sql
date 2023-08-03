
use `utils`;
use `pendulum.parsing`;
use `sys`;
use `os`;
use `datetime as python_datetime`;

select utils$hello();

select utils$add_number(1, 2), utils$sum_array((1, 2, 3));

select parsing$parse('2023-02-10 10:33:22');

select sys$version(), os$getcwd();

select python_datetime$datetime$now();

select ext_sum_func(1, 2);

select math$pow(2, 3);

select util_helpers_sum(1, 2);

select uh$sum(1, 2) as v1, uh$load_time() as v2, uh$loadTime() as v3, uh$LoadTime() as v4, uh$LOAD_TIME() as v5;

select uh$A$sum(uh$A(), 1, 2);