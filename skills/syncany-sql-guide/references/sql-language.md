# Syncany-SQL：SQL 语言参考

> **范围声明：** Syncany-SQL 使用 MySQL 风格的语法结构，但它不是完整的 MySQL 实现。应把它当作由本项目编译器和执行器定义的 SQL 子集：本章只列出仓库文档、示例、测试或实现中已经核实的能力；未列出的 MySQL 语法（即使解析器可能接受）均**不应假定可用**。

## 先理解执行方式

一条 `SELECT` 通常先以数据源可表达的简单条件和排序加载数据，再由本地执行器完成其余工作。尤其是：

- `JOIN` 会使用 `IN` 查询加载关联数据，随后在内存中匹配；因此可以跨库、跨主机、跨数据库类型关联。
- `GROUP BY`、`DISTINCT` 和不能完全落到主表字段的 `ORDER BY` 在内存中完成。
- 仅由主表字段组成的排序可由数据源完成；其他排序在内存中完成。
- 配置批量执行时，带聚合或 `HAVING` 的查询会通过 reduce 合并结果；执行器也有流式执行选项。不要把批处理理解为普通数据库的事务语义。

这意味着 SQL 应优先将可下推的加载条件写成简单条件；跨源 `JOIN`、内存聚合和全局排序会占用本地内存。数据源加载只保证“简单查询条件和排序”，不要依赖复杂表达式一定由远端数据库执行。

来源：[功能限制](../../../docs/feature-restrictions.md)、[README](../../../README.md)、[查询 tasker 的批量/reduce 逻辑](../../../syncanysql/taskers/query.py)、[编译器](../../../syncanysql/compiler.py)。

## 已核实支持矩阵

| 类别 | 已核实的写法/能力 | 证据 |
| --- | --- | --- |
| 基础查询 | `SELECT`、常量查询、`FROM`、别名、反引号标识符、`WHERE`、`ORDER BY`、`LIMIT` | [README 查询示例](../../../README.md)、[编译器的 `compile_select`](../../../syncanysql/compiler.py) |
| 行与表达式 | 列引用、字面量、算术/比较/逻辑条件、`IN (...)`、`IS NULL`、`CASE`/常用 MySQL 函数 | [逻辑示例](../../../examples/logic_operation/logic_operation.sql)、[聚合示例](../../../examples/aggregate/aggregate.sql)、[函数文档](../../../docs/functions.md) |
| 去重与聚合 | `DISTINCT`、`GROUP BY`、`HAVING`；`COUNT`（含 `DISTINCT`）、`SUM`、`AVG`、`MIN`、`MAX`，及已注册的聚合计算器 | [聚合示例](../../../examples/aggregate/aggregate.sql)、[聚合编译](../../../syncanysql/compiler.py)、[聚合测试](../../../tests/test_example_aggregate.py) |
| 连接 | `JOIN`/`INNER JOIN`、`LEFT JOIN`、`RIGHT JOIN`；`ON` 内可有复合条件和子查询结果 | [连接示例](../../../examples/joins)、[连接测试](../../../tests/test_example_joins.py)、[功能限制](../../../docs/feature-restrictions.md) |
| 子查询 | `FROM (SELECT ...) AS alias`、相关标量子查询、`EXISTS`、`IN (SELECT ...)`；可用于 `WHERE`、`JOIN ON`、`HAVING` 的条件值 | [子查询示例](../../../examples/subquery/subquery.sql)、[子查询测试](../../../tests/test_example_subquery.py)、[功能限制](../../../docs/feature-restrictions.md) |
| 窗口 | `函数(...) OVER (PARTITION BY ... ORDER BY ...)`；示例验证 `LEAD` | [窗口聚合示例](../../../examples/aggregate/window_aggregate.sql)、[窗口测试](../../../tests/test_example_aggregate.py)、[窗口编译](../../../syncanysql/compiler.py) |
| 写入 | `INSERT INTO ... SELECT`、`INSERT INTO (...) VALUES (...)`；合并模式 `I`、`U`、`UI`、`UDI`、`DI` | [写入示例](../../../examples/insert_types)、[写入测试](../../../tests/test_example_insert_types.py)、[README](../../../README.md) |
| 更新/删除 | `UPDATE ... SET ... WHERE ...`，包括已测试的多表/`JOIN` 更新；`DELETE` 有专用编译分支 | [更新示例](../../../examples/insert_types/update.sql)、[写入测试](../../../tests/test_example_insert_types.py)、[编译入口](../../../syncanysql/compiler.py) |
| 命令 | 已实现并有编译分支：`SET`、`USE`、`EXPLAIN` | [编译命令分发](../../../syncanysql/compiler.py)、[功能限制](../../../docs/feature-restrictions.md) |

**DDL：本章不列支持项。** 项目示例和测试中未找到 `CREATE`、`ALTER`、`DROP` 或 `TRUNCATE` 的已核实用例，编译入口也没有将它们列为可执行表达式；请不要将它们当作 Syncany-SQL 的语言能力。

## 1. 从单表查询开始

使用数据源表名（可为文件路径或配置的数据源）和明确别名。为输出表达式取别名可避免结果列名难以使用。

```sql
SELECT seg0 AS ip, COUNT(*) AS cnt
FROM `file://data/access.log?sep= `
GROUP BY seg0
ORDER BY cnt DESC
LIMIT 3;
```

可在 `WHERE` 使用比较、`AND`/`OR`、`IN`、空值判断和已注册函数。以下是已在示例中出现的模式：

```sql
SELECT order_id, amount
FROM `data/orders.json`
WHERE status = 0
  AND amount > 5
ORDER BY order_id DESC
LIMIT 10;
```

函数集合和函数语义请查 [内置函数文档](../../../docs/functions.md)，不要因函数名与 MySQL 相同而推断其参数、类型转换或边界行为也完全相同。

## 2. 连接：本地匹配，不是远端联表 SQL

为每个表使用别名，并在 `ON` 中写出关联键与右表过滤条件：

```sql
SELECT a.order_id, b.name, c.goods_name
FROM `data/orders.json` AS a
JOIN `data/users.json` AS b
  ON a.uid = b.uid AND b.status = 0
LEFT JOIN `data/goodses.json` AS c
  ON a.goods_id = c.goods_id AND c.status = 0
WHERE a.status = 0;
```

`INNER JOIN`、`LEFT JOIN`、`RIGHT JOIN` 均有示例和测试。跨数据源关联时，关联键的类型必须相容；项目示例专门提示 MongoDB `_id` 等值在另一数据源保存后可能需显式类型转换。

**限制与排错：** 由于关联数据经 `IN` 加载后再在内存匹配，复杂连接会扩大本地工作集。优先过滤主表和各关联表，并将跨源键统一类型。不要假定任意 SQL join 变体（例如未在矩阵列出的 `FULL OUTER JOIN`）可用。

来源：[连接示例](../../../examples/joins)、[类型注解说明](../../../examples/README.md)、[功能限制](../../../docs/feature-restrictions.md)。

## 3. 子查询：派生表、标量值与存在性

`FROM` 中的派生表必须起别名：

```sql
SELECT uid, total_amount
FROM (
  SELECT uid, SUM(amount) AS total_amount
  FROM `data/orders.json`
  GROUP BY uid
) AS totals
WHERE total_amount > 20;
```

已测试的相关子查询和 `EXISTS`：

```sql
SELECT a.order_id,
       (SELECT COUNT(*)
        FROM `data/order_historys.json` AS h
        WHERE h.order_id = a.order_id AND h.status = 0) AS history_count,
       EXISTS(SELECT COUNT(*)
              FROM `data/order_historys.json` AS h
              WHERE h.order_id = a.order_id AND h.status = 0) AS has_history
FROM `data/orders.json` AS a;
```

子查询返回值可作为 `WHERE`、`JOIN ON` 和 `HAVING` 的条件值。相关子查询会按外层行求值；调试时先单独运行内层查询，确认它对每个外层键返回预期的单值或存在性结果。

## 4. 去重、分组与聚合

`DISTINCT` 和分组聚合均在内存完成。基础写法：

```sql
SELECT uid,
       COUNT(*) AS order_count,
       COUNT(DISTINCT goods_id) AS goods_count,
       SUM(amount) AS total_amount,
       AVG(amount) AS average_amount
FROM `data/orders.json`
WHERE status = 0
GROUP BY uid
HAVING total_amount > 20
ORDER BY total_amount DESC;
```

已核实的内建聚合包括 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX`、`GROUP_CONCAT`、`GROUP_ARRAY`、`GROUP_UNIQ_ARRAY`、`GROUP_BIT_AND`、`GROUP_BIT_OR`、`GROUP_BIT_XOR`。另有由运行时注册的自定义聚合计算器；其可用性取决于配置，不能视为默认 SQL 标准能力。

`HAVING` 的执行位置有特殊规则：只引用聚合计算字段时，在聚合之前运行；否则在聚合之后运行。若结果意外，先把过滤条件分别放在 `WHERE` 与 `HAVING` 验证，并避免依赖其他数据库对 `HAVING` 的优化方式。

## 5. 窗口表达式

窗口查询使用 `OVER`，已验证 `PARTITION BY` 与窗口内 `ORDER BY`，以及 `LEAD`：

```sql
SELECT order_id, create_time,
       LEAD(history_type) OVER (
         PARTITION BY order_id
         ORDER BY create_time
       ) AS next_history_type
FROM `data/order_historys.json`;
```

窗口函数由本地聚合/窗口执行路径处理。编译器对窗口中的 `DISTINCT` 有额外限制：只有 `COUNT(DISTINCT ...)` 可使用；其他窗口聚合的 `DISTINCT` 会编译失败。未在示例或测试中出现的 frame 子句、窗口函数或命名窗口不在本章承诺范围内。

## 6. 写入、更新与删除

### `INSERT INTO`

已测试两种来源：`SELECT` 结果与 `VALUES` 行。`SELECT` 写入的最小形式为：

```sql
INSERT INTO `target_table`
SELECT id, name
FROM `source_table`
WHERE status = 0;
```

`VALUES` 也有已测试用法：

```sql
INSERT INTO `target_table` (`id`, `name`, `value`)
VALUES (1, 'a', 1), (2, 'b', 4);
```

目标表名可带合并模式：`<I>` 仅插入、`<U>` 更新、`<UI>` 存在则更新否则插入、`<UDI>` 更新/插入并删除本次结果中不存在的记录、`<DI>` 先删除再插入。合并行为依赖主键：示例默认第一列为主键，也演示了将投影列标为 ``<pk>`` 的方式。写入 Excel、JSON、CSV 的能力由功能限制文档声明；目标连接与格式配置见其他指南。

### `UPDATE` 与 `DELETE`

已测试的单表更新：

```sql
UPDATE `cdata`
SET name = '花生'
WHERE id IN (1, 3) AND create_time = '2023-03-12 10:12:34';
```

示例还验证了逗号表和 `JOIN` 形式的更新。`DELETE` 由专门的 `DeleteTasker` 编译/执行路径处理，但本仓库没有可作为用户教程最小语法依据的 `DELETE` 示例或测试断言。因此，除非在你的目标版本中先用本地测试验证，否则不要把任意 `DELETE` 写法推广到生产脚本。

## 硬性边界清单

1. 这不是完整 MySQL；仅使用本章矩阵和链接证据覆盖的语法。
2. 远端数据加载仅支持简单条件与排序；复杂条件、连接、分组、去重和许多排序会在本地执行。
3. 跨源 `JOIN` 要显式处理关联键类型，并控制输入规模。
4. 分组、去重、连接与窗口都可能消耗本地内存；大数据量使用批处理或流式执行前，应先在代表性数据上验证结果与资源占用。
5. 子查询、`HAVING` 和窗口表达式的行为以项目测试和编译器为准，不以 MySQL 文档为准。
6. 不承诺 DDL，也不承诺未列出的连接类型、集合运算、窗口 frame 或 MySQL 专有语法。

## 继续核对的源文件

- [支持特性与数据加载限制](../../../docs/feature-restrictions.md)
- [项目定位、执行特性和最小查询](../../../README.md)
- [连接 SQL 与断言](../../../examples/joins) / [测试](../../../tests/test_example_joins.py)
- [子查询 SQL 与断言](../../../examples/subquery/subquery.sql) / [测试](../../../tests/test_example_subquery.py)
- [聚合、窗口 SQL 与断言](../../../examples/aggregate) / [测试](../../../tests/test_example_aggregate.py)
- [写入/合并/更新 SQL 与断言](../../../examples/insert_types) / [测试](../../../tests/test_example_insert_types.py)
- [语句分发、查询/聚合/窗口编译](../../../syncanysql/compiler.py)
- [批处理、依赖子查询与 reduce 执行](../../../syncanysql/taskers/query.py)
