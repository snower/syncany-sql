# syncany-sql

简单易用的SQL执行引擎。

- 可在本地运行MySQL语法结构的SQL
- 支持查询常用mysql、mongodb、postgresql、sqlserver、elasticsearch、influxdb、clickhouse数据库及execl、csv、json和普通文本文件
- 支持本地临时数据表逻辑做中间结果保存
- 数据库数据加载使用简单条件过滤及IN条件查询
- 因由本地完成Join匹配所以支持不同库表、不同主机及不同类型数据库间Join关联查询
- Group By分组聚合计算及Order By排序也由本地执行，保证数据库安全性
- 数据写Insert Into支持 ”仅插入 I“、”存在更新否则插入 UI“、”存在更新否则插入其余删除 UDI“、”删除后插入 DI“四种模式
- 大数据量支持批次执行，有Group By或Having条件过滤自动执行Reduce合并结果
- 支持流式执行

# 安装

```
pip3 install syncanysql
```

# 示例

## 统计Nginx的access.log中访问最多IP数

```sql
SELECT
    ip, cnt
FROM
    (SELECT
        seg0 AS ip, COUNT(*) AS cnt
    FROM
        `file:///var/log/nginx/access.log?sep= `
    GROUP BY seg0) a
ORDER BY cnt DESC
LIMIT 3;
```

## 订单统计demo

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
        YIELD_DATA(sites) AS site_id,
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

# 支持的特性

- Join关联查询
- Having支持复杂条件过滤
- Group By分组聚合计算
- Order By排序
- 子查询
- Where或Having条件支持简单子查询
- Insert Into除支持正常写入数据库表外可直接写入到execl、json和csv中

# 使用限制

- Where条件仅可使用==、>、>=、<、<=、!=、in简单条件且仅可用and
- Join仅支持Left Join模式且关联条件仅被关联表能添加常量条件及主表可添加计算条件值
- Join查询及子查询各表必须有别名

# 依赖驱动

默认不安装数据库驱动，需要查询读写对应类型数据库或文件时需要自行安装。

- pyyaml>=5.1.2
- pymongo>=3.6.1
- PyMySQL>=0.8.1
- openpyxl>=2.5.0
- psycopg2>=2.8.6
- elasticsearch>=6.3.1
- influxdb>=5.3.1
- clickhouse_driver>=0.1.5
- redis>=3.5.3
- requests>=2.22.0
- pymssql>=2.2.7

# License

syncany-sql uses the MIT license, see LICENSE file for the details.