
insert into `orders` select * from `data/data.json`;

select `order.orderId` as orderId, `user.username` as username, `order.payInfo.payId` as payId, `order.refundInfo.refundId` as refundId from `orders`;

select get_value(`order`, 'orderId') as orderId, get_value(`user.username`) as username, get_value(`order`, 'payInfo.payId') as payId, get_value(`order`, 'refundInfo.refundId') as refundId from `orders`;

select `order.address.mobile` as mobile1, get_value(`order`, 'address.mobile') as mobile2 from `orders`;

select `order.payInfo.payTypes.:0` as payTypes1, get_value(`order`, 'payInfo.payTypes', 0) as payTypes2, get_value(`order`, 'payInfo.payTypes.:0') as payTypes3 from `orders`;

select `order.payInfo.payTypes.:0:2` as payTypes1, get_value(`order`, 'payInfo.payTypes', (0, 2)) as payTypes2, get_value(`order`, 'payInfo.payTypes.:0:2') as payTypes3 from `orders`;

select `order.payInfo.payTypes.:-1:-3:-1` as payTypes1, get_value(`order`, 'payInfo.payTypes', (-1, -3, -1)) as payTypes2, get_value(`order`, 'payInfo.payTypes.:-1:-3:-1') as payTypes3 from `orders`;

select `order.channel.:0` as channel1, get_value(`order`, 'channel.:0') as channel2 from `orders`;