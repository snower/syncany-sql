# Syncany-SQL

简单易用的SQL执行引擎。

- 可在本地运行MySQL语法结构的SQL
- 支持查询常用mysql、mongodb、postgresql、sqlserver、oracle、elasticsearch、influxdb、clickhouse、sqlite数据库及execl、csv、json和普通文本文件
- 支持本地临时数据表逻辑做中间结果保存
- 数据库数据加载使用简单条件过滤及IN条件查询
- 因由本地完成Join匹配所以支持不同库表、不同主机及不同类型数据库间Join关联查询
- Group By分组聚合计算及Order By排序也由本地执行，保证数据库安全性
- 数据写Insert Into支持 ”仅插入 I“、”仅更新 U“、”存在更新否则插入 UI“、”存在更新否则插入其余删除 UDI“、”删除后插入 DI“四种模式
- 大数据量支持批次执行，有Group By或Having条件过滤自动执行Reduce合并结果
- 支持流式执行
- 支持使用常用MySQL函数
- 支持分区窗口函数查询
- 支持使用pyeval执行python代码（调用的就是python的eval函数）

-----

- [安装](#安装)
- [特性与限制](feature-restrictions.md)
- [配置详解](configure.md)
- [驱动依赖](driver-dependency.md)
- [示例详解](../examples)
- [内置函数](functions.md)

## 安装

```
pip3 install syncanysql
```

## 查询Nginx日志

```sql
-- 查询访问量最高的三个IP
SELECT seg0 AS ip, COUNT(*) AS cnt FROM `file://data/access.log?sep= ` GROUP BY seg0 ORDER BY cnt DESC LIMIT 3;
```

## 查询JSON文件

```sql
SELECT
    a.site_id,
    b.name AS site_name,
    IF(c.site_amount > 0, c.site_amount, 0) AS site_amount,
    MAX(a.timeout_at) AS timeout_at,
    MAX(a.vip_timeout_at) AS vip_timeout_at,
    now() as `created_at?`
FROM
    (SELECT
        YIELD_ARRAY(sites) AS site_id,
            IF(vip_type = '2', GET_VALUE(rules, 0, 'timeout_time'), '') AS timeout_at,
            IF(vip_type = '1', GET_VALUE(rules, 0, 'timeout_time'), '') AS vip_timeout_at
    FROM
        `data/demo.json`
    WHERE
        start_date >= '2021-01-01') a
        JOIN
    `data/sites.json` b ON a.site_id = b.site_id
        JOIN
    (SELECT
        site_id, SUM(amount) AS site_amount
    FROM
        `data/orders.json`
    WHERE
        status <= 0
    GROUP BY site_id) c ON a.site_id = c.site_id
GROUP BY a.site_id;
```

# License

Syncany-SQL uses the MIT license, see LICENSE file for the details.