
-- SQL型RAW SELECT查询，raw和endraw之间的SQL会以原始SQL提交到数据库执行
-- raw括号中第一个参数指定配置的数据库名，第二个参数为该RAW SQL取一个名称
-- 注意RAW SQL直接提交到数据库执行，其中数据库名和表名称都应直接和数据库一致
select * from /* raw(mysql_cdtx.test2) */ (
    select a.customer_id, b.mobile, b.nickname, c.birthday, c.name, c.gender
        from cdtx.t_customer a join cdtx.t_user b on a.uid=b.uid and b.is_deleted=0
        join cdtx.t_user_profile c on b.uid=c.uid and c.is_deleted=0
        where a.is_deleted=0 and a.workshop_id in (77, 78)
) /* endraw */ b;

-- 非SQL型数据库编写RAW QUERY可省略“*/ (”和“) /*”
-- 如mongo的RAW QUERY即为aggregate查询语句
select msg_text from /* raw(mongo_instant_messaging.t_im_group_messages)
    []
endraw */ a where msg_type='text' order by a.msg_time desc;