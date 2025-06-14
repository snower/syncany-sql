insert into `cdata` select 1 as `id`, '萝卜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 2 as `id`, '土豆' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 4 as `id`, '花菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 1 as `id`, '白菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 2 as `id`, '青菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 3 as `id`, '油麦菜' as `name`, '2023-03-12 10:12:34' as `create_time`;

-- 只插入（第一个字段`id`为主键）
insert into `cdata<I>` select `id`, `name`, `create_time` from `ndata` where `id`>=2;
select `id`, `name`, `create_time` from `cdata`;

insert into `tdata1` (`id`, `name`, `value`) values (1, 'a', 1), (2, 'a', 2), (3, 'c', 3);
select `id`, `name`, `value` from `tdata1`;

insert into `tdata2` (`id`, `name`, `value`) values (1, concat(1, 'a', 1), 1), (2, 'a', 1 + 3), (3, concat(1, 'b', 1), 2 * 4 + 1);
select `id`, `name`, `value` from `tdata2`;

insert into `tdata3` (`id`, `name`, `value`) values (1, 'a', 1), (2, 'a', 1 + 3), (3, concat(1, 'b', 1), 2 * 4 + 1);
select `id`, `name`, `value` from `tdata3`;
