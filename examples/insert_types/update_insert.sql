insert into `cdata` select 1 as `id`, '萝卜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 2 as `id`, '土豆' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 4 as `id`, '花菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 5 as `id`, '萝卜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 6 as `id`, '土豆' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `cdata` select 8 as `id`, '花菜' as `name`, '2023-03-12 10:12:34' as `create_time`;

insert into `ndata` select 1 as `id`, '白菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 2 as `id`, '青菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 3 as `id`, '油麦菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 5 as `id`, '白菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 6 as `id`, '青菜' as `name`, '2023-03-12 10:12:34' as `create_time`;
insert into `ndata` select 7 as `id`, '油麦菜' as `name`, '2023-03-12 10:12:34' as `create_time`;

-- 以主键IN查询加载数据后以主键`id`判断，存在则更新否则插入（第一个字段`id`为主键）
insert into `cdata<UI>` select `id`, `name`, `create_time` from `ndata` where `id`>=2;
select `id`, `name`, `create_time` from `cdata`;

insert into `cdata<UI>` select `id`, '玉米' as `name` from `ndata` where `id`>=2;
select `id`, `name`, `create_time` from `cdata`;
