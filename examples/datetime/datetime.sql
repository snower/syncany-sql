
select NOW();

select NOW(0), NOW('-1d'), NOW('+3d'), NOW('-3d', 0), NOW('-3d', 0, 10, 11);

select DATE_ADD(now(), 1), ADDDATE(now(), INTERVAL 20 DAY), DATE_SUB(now(), INTERVAL 10 DAY), SUBDATE(now(), INTERVAL 7 MONTH);

select ADDTIME(now(), '10:00'), SUBTIME(now(), '1 10:00');

select DATE_FORMAT(datetime('2023-04-24 17:07:08'), '%Y-%m-%d %H:%M:%S'), TIME_FORMAT(datetime('2023-04-24 17:07:08'), '%H:%M:%S'), TIME_TO_SEC('10:11:00'), SEC_TO_TIME(234);

select CURDATE(), CURRENT_DATE(), CURRENT_TIME(), CURTIME();

select FROM_UNIXTIME(1677833819), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(now()), CURRENT_TIMESTAMP();

select UTC_DATE(), UTC_TIME(), UTC_TIMESTAMP();

select date(now()), datetime(now()), time(now()), datetime(date(now())), datetime(time(now())), date(time(now())), time(date(now()));

select DATE_FORMAT(convert_datetime('1600-01-01'), '%Y-%m-%d %H:%M:%S'), DATE_FORMAT(convert_datetime('1900-01-01 10:12:23'), '%Y-%m-%d %H:%M:%S'), TIME_FORMAT(convert_datetime('1800-01-01 10:12:23'), '%H:%M:%S');