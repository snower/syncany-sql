
insert into `orders` select * from `data/data.json`;

-- 字符串
select type(`order.orderId[str]`) as t, `order.orderId[str]` as v from orders;
-- 数字格式化
select type(`order.payInfo.amount[str %.02f]`) as t, `order.payInfo.amount[str %.02f]` as v from orders;

-- 时间
select type(`order.orderAt[datetime]`) as t, `order.orderAt[datetime]` as v from orders;
-- 时间格式化为字符串
select `orderAt`, type(`orderAt[str %Y-%m-%d]`) as t, `orderAt[str %Y-%m-%d]` as v from (
    select `order.orderAt[datetime]` as orderAt from orders
) a;
-- 时间转换为时间戳
select `orderAt`, type(`orderAt[int]`) as t, `orderAt[int]` as v from (
    select `order.orderAt[datetime]` as orderAt from orders
) a;

-- ObjectId，如和mongo进行join查询时需要为转换对应类型
select type(`goods.goodsId[objectid]`) as t, `goods.goodsId[objectid]` as v from (
    select yield_data(`order.goodses`) as goods from orders
) a;

-- UUID
select type(`order.payInfo.payId[uuid]`) as t, `order.payInfo.payId[uuid]` as v from orders;
-- UUID转换为数字
select `payId`, type(`payId[int]`) as t, `payId[int]` as v from (
    select `order.payInfo.payId[uuid]` as payId from orders
) a;

-- 转换为bool值
select type(`order.orderId[bool]`) as t, `order.orderId[bool]` as v from orders;
select `v1[bool]` as v1, `v2[bool]` as v2, `v3[bool]` as v3, `v4[bool]` as v4, `v5[bool]` as v5 from (
    select 0 as v1, 1 as v2, '' as v3, 'a' as v4, null as v5
) a;