# Syncany-SQL SQL 语言参考

> **适用范围：** 本参考介绍 Syncany-SQL 的常用查询与数据操作写法。它采用部分 MySQL 风格语法，但**不是完整 MySQL**；未在本页说明的语法、函数和行为均不应假定可用。函数、NULL 与类型转换规则请同时阅读[内置函数参考](built-in-functions.md)。

## 使用前须知

- 一条查询可能从不同数据源读取数据，并在本地完成连接、分组、去重或排序。请先用 `WHERE` 尽早减少数据量。
- 跨数据源的 `JOIN` 会在本地匹配数据；大表连接、分组和全局排序可能占用较多内存。
- 请为表和计算列起别名，尤其是在连接、子查询和聚合中。
- 使用参数化方式或受控输入构造条件值；不要把未经验证的用户输入直接拼接进 SQL 文本。
- 本页不承诺 `EXECUTE`、`SHOW`、DDL（如 `CREATE`、`ALTER`、`DROP`）或完整 MySQL 方言可用。

## SELECT：查询、筛选、排序与限制

```sql
SELECT order_id, uid, amount AS order_amount
FROM orders AS o
WHERE o.status = 0
  AND o.amount > 5
ORDER BY o.order_id DESC
LIMIT 10;
```

常用组成：

```sql
SELECT DISTINCT uid
FROM orders
WHERE status IN (0, 1)
  AND deleted_at IS NULL
ORDER BY uid
LIMIT 100;
```

- 使用 `WHERE` 进行比较、逻辑组合、`IN (...)` 和 `IS NULL` / `IS NOT NULL` 判断。
- `ORDER BY` 建议优先使用主表字段；复杂表达式或聚合结果排序可能在本地完成。
- `LIMIT` 用于限制返回行数；在试运行大查询时应优先添加它。
- 标识符可使用反引号，例如 `` `order` ``；字符串字面量使用单引号。

## JOIN：关联数据

支持常用的 `JOIN` / `INNER JOIN`、`LEFT JOIN` 和 `RIGHT JOIN`：

```sql
SELECT o.order_id, u.name, g.goods_name
FROM orders AS o
JOIN users AS u
  ON o.uid = u.uid AND u.status = 0
LEFT JOIN goods AS g
  ON o.goods_id = g.goods_id AND g.status = 0
WHERE o.status = 0;
```

兼容性与性能提示：

- 连接键两侧应具有相同或可明确转换的类型；例如字符串 ID 与数字 ID 不应依赖隐式匹配。
- 跨源关联不是远端数据库的联表执行。过滤每个参与表，避免无条件连接大数据集。
- 不要假定 `FULL OUTER JOIN` 或其他未列出的连接变体可用。

## 子查询

### 派生表

`FROM` 中的子查询必须使用别名：

```sql
SELECT t.uid, t.total_amount
FROM (
  SELECT uid, SUM(amount) AS total_amount
  FROM orders
  WHERE status = 0
  GROUP BY uid
) AS t
WHERE t.total_amount > 20;
```

### 标量子查询、EXISTS 与 IN

```sql
SELECT o.order_id,
       (SELECT COUNT(*)
        FROM order_history AS h
        WHERE h.order_id = o.order_id) AS history_count
FROM orders AS o
WHERE EXISTS (
  SELECT 1
  FROM users AS u
  WHERE u.uid = o.uid AND u.status = 0
);

SELECT order_id
FROM orders
WHERE uid IN (SELECT uid FROM users WHERE status = 0);
```

相关子查询会按外层行求值。先单独验证内层查询的结果范围：作为标量使用时，应确保它为每个外层行产生一个可预期的单值。

## DISTINCT、GROUP BY 与 HAVING

```sql
SELECT uid,
       COUNT(*) AS order_count,
       COUNT(DISTINCT goods_id) AS goods_count,
       SUM(amount) AS total_amount,
       AVG(amount) AS average_amount
FROM orders
WHERE status = 0
GROUP BY uid
HAVING total_amount > 20
ORDER BY total_amount DESC;
```

常用聚合包括 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX`，以及 `GROUP_CONCAT`、`GROUP_ARRAY`、`GROUP_UNIQ_ARRAY` 等。具体函数及 NULL 规则见[内置函数参考](built-in-functions.md)。

分组、去重和聚合可能在本地完成。对大数据集，先筛选、只选择需要的列，并避免不必要的高基数分组键。

## 窗口计算

窗口函数使用 `OVER (...)`。排序相关函数应在窗口中提供 `ORDER BY`：

```sql
SELECT order_id,
       uid,
       ROW_NUMBER() OVER (
         PARTITION BY uid
         ORDER BY amount DESC
       ) AS row_no,
       LAG(amount, 1, 0) OVER (
         PARTITION BY uid
         ORDER BY order_id
       ) AS previous_amount
FROM orders;
```

可用窗口函数与边界行为见[内置函数参考](built-in-functions.md)。窗口计算会保留分区数据，使用前应评估分区规模。

## INSERT、UPDATE 与 DELETE

### INSERT

```sql
INSERT INTO order_summary (uid, total_amount)
SELECT uid, SUM(amount)
FROM orders
WHERE status = 0
GROUP BY uid;

INSERT INTO order_summary (uid, total_amount)
VALUES (1001, 99.50);
```

### UPDATE

```sql
UPDATE orders
SET status = 1
WHERE order_id = 10001
  AND status = 0;
```

### DELETE

```sql
DELETE FROM orders
WHERE order_id = 10001
  AND status = 0;
```

写操作安全建议：

- 执行前先将同一 `WHERE` 条件改写为 `SELECT`，核对影响范围。
- 对 `UPDATE` 和 `DELETE` 始终写明确的 `WHERE`，除非确实需要处理全部数据。
- 批量写入、跨源写入和冲突处理的实际效果取决于所连接数据源的能力；不要将其视为传统数据库事务。

## SET、USE 与 EXPLAIN

可使用 `SET`、`USE` 和 `EXPLAIN` 管理会话或检查查询；其可接受的选项与输出内容取决于当前运行环境。建议先对目标环境中的简单语句验证配置和解释结果，再用于自动化流程。

```sql
EXPLAIN
SELECT order_id, amount
FROM orders
WHERE status = 0
LIMIT 10;
```

## 常见兼容性检查清单

1. 只使用本参考及[内置函数参考](built-in-functions.md)中列出的语法和函数。
2. 对 NULL 明确写出处理策略；比较或拼接前可使用 `IFNULL` 或 `COALESCE`。
3. 对跨源连接键显式统一类型。
4. 先以 `LIMIT` 验证查询，再运行全量查询或写操作。
5. 不因语法外观类似 MySQL，就推断函数参数、类型转换、正则、JSON 或错误处理完全兼容。
