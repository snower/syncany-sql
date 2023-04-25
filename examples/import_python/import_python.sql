
use `utils`;
use `pendulum.parsing`;
use `sys`;
use `os`;

select utils$hello();

select utils$add_number(1, 2), utils$sum_array((1, 2, 3));

select parsing$parse('2023-02-10 10:33:22');

select sys$version(), os$getcwd();