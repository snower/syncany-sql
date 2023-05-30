
set @aaa=1;
set @bbb=@aaa + 1;

-- 获取环境变量或命令行参数
set @ccc='${PATH:}';

select @aaa as a, @bbb as b, @ccc as c;
select @aaa + @bbb as a, math$pow(@bbb, 2) as b, length(@ccc) as c;

select 2 as a into @aaa;
select a into @bbb from (
    select yield_array(json$decode('[1, 3]')) as a
) t;
select a, b into @ccc from (
    select 1 as a, 'abc' as b
) t;
select a, b into @ddd from (
    select yield_array(json$decode('[1, 3]')) as a, yield_array(json$decode('["abc", "efg"]')) as b
) t;

select @aaa as a, @bbb as b, @ccc as c, @ddd as d;