insert into `cdata` select 1 as `id`, '萝卜' as `name`, now() as `create_time`;
insert into `cdata` select 2 as `id`, '土豆' as `name`, now() as `create_time`;
insert into `cdata` select 4 as `id`, '花菜' as `name`, now() as `create_time`;
insert into `ndata` select 1 as `id`, '白菜' as `name`, now() as `create_time`;
insert into `ndata` select 2 as `id`, '青菜' as `name`, now() as `create_time`;
insert into `ndata` select 3 as `id`, '油麦菜' as `name`, now() as `create_time`;

-- 只插入（第一个字段`id`为主键）
insert into `cdata<I>` select `id`, `name`, `create_time` from `ndata` where `id`>=2;
select `id`, `name`, `create_time` from `cdata`;

