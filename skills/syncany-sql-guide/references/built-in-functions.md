# 内置函数参考（按需加载）

**何时阅读：** 编写 `SELECT` 表达式、`GROUP BY` 聚合、`OVER (...)` 窗口计算，或需要展开数组时加载本章。只想了解 SQL 结构时，先看本技能的 SQL 章节；需要连接、扩展或路由时，转到对应章节。本页只列出仓库中有文档、注册或实现依据的函数；它不是 MySQL 函数全集。

## 先确认可用性

函数名由执行器本地解析，不会自动透传为 MySQL 服务端函数。权威清单按以下顺序核对：

1. 阅读项目总表：[`docs/functions.md`](../../../docs/functions.md)；其中列出内置、MySQL 常用、聚合、窗口和 `YIELD` 函数。
2. 对 MySQL 风格标量函数，以 [`syncanysql/calculaters/mysql_funcs/`](../../../syncanysql/calculaters/mysql_funcs/) 中 `mysql_` 前缀实现为准；各模块在启动时汇入 `funcs` 注册表。
3. 对聚合、窗口与生成函数，以 [`syncanysql/calculaters/__init__.py`](../../../syncanysql/calculaters/__init__.py) 的 `SQL_CALCULATERS` 为准；这里能确认实际注册的名称。
4. 需要可运行范例时，查看 [`examples/`](../../../examples/) 及其对应 `tests/test_example_*.py`。自定义 `aggregate_*`、`window_aggregate_*` 或生成函数可能来自示例中的 `USE` 扩展，不能当作默认内置函数。

下列名称大小写按 SQL 示例书写；函数实现本身主要是 Python 适配，不应据此假定与 MySQL 的边界行为完全一致。

## 标量函数

### 数值与位运算

已实现的数值模块包括 `add`、`sub`、`mul`、`div`、`mod`，位运算 `bitwiseand`、`bitwiseor`、`bitwisenot`、`bitwisexor`、`bitwiseleftshift`、`bitwiserightshift`，以及 `abs`、`sqrt`、`exp`、`ln`、`log`、`ceil`/`ceiling`、`floor`、`rand`、`round`、`sign`、`pow`/`power` 和三角函数等。通常优先使用 SQL 运算符：

```sql
SELECT 2 + 1, 2 & 1, ~2;
SELECT ROUND(amount / 100, 2) FROM `data/orders.json`;
```

源码对 `add`/`sub`/`mul`/`div`/`mod` 先处理数值；任一操作数为 `NULL` 时返回 `NULL`，其他值会尝试数值转换。位运算也会尝试转整数。无效转换、除零等异常不要假定会抛到 SQL 层：MySQL 计算器对 `ValueError`、`KeyError` 以及其他执行异常会返回 `NULL`。

### 字符串与编码

已实现：`concat`、`concat_ws`、`substring`/`substr`、`substring_index`、`lower`/`upper`/`ucase`、`trim`、`left`、`right`、`replace`、`repeat`、`reverse`、`length`、`char_length`/`character_length`、`strcmp`、`startswith`、`endswith`、`contains`、`hex`、`unhex`、`to_base64`、`from_base64`，以及 IP、字符与校验函数。

```sql
SELECT CONCAT('a', 'b', 'c'), SUBSTRING('abc', 1, 2), LOWER('AbC');
SELECT TRIM(' a b '), REPEAT('a', 3), REVERSE('abc');
```

`length` 计算 UTF-8 字节数，`char_length` 计算字符数。`lower`、`upper`、`length` 等单参数实现会对 `NULL` 返回 `NULL`；`concat` 的用户语义是任一参数为 `NULL` 则结果为 `NULL`。实现上它会逐项尝试转换为字符串，`NULL` 的转换失败由计算器捕获后返回 `NULL`。先用 `IFNULL`/`COALESCE` 明确缺失值策略；其他字符串行为也不应假定与 MySQL 完全一致。

### 日期时间

已实现日期时间适配包含 `now`、`curdate`/`current_date`、`curtime`/`current_time`、`current_timestamp`、`date_add`/`adddate`、`date_sub`/`subdate`、`addtime`、`subtime`、`date_format`、`time_format`、`from_unixtime`、`unix_timestamp` 和 UTC 系列。

```sql
SELECT NOW(), DATE_ADD(NOW(), INTERVAL 20 DAY), DATE_SUB(NOW(), INTERVAL 7 MONTH);
SELECT DATE_FORMAT(datetime('2023-04-24 17:07:08'), '%Y-%m-%d %H:%M:%S');
```

日期加减接受 `INTERVAL` 形式；实现对月/季使用日历调整，遇到目标月份不存在的日期会向前尝试有效日期。时间解析失败会产生 `NULL`，因此外部字符串建议先用项目的 `convert_datetime`/`datetime` 转换并在样本数据上验证时区与格式。

### 逻辑、比较与类型

比较/逻辑实现覆盖 `eq`、`neq`、`gt`、`gte`、`lt`、`lte`、`in`、`not_in`、`is`、`is_not`、`and`、`or`、`not`、`if`、`ifnull`、`coalesce`、`nullif` 等，并由 SQL 运算符和表达式语法使用：

```sql
SELECT 1 <= '2', 'abc' LIKE '%bc', IF(amount > 0 AND status = 0, 1, 0);
SELECT CASE WHEN amount <= 0 THEN 'A' ELSE 'B' END FROM `data/orders.json`;
```

相等和大小比较在不同类型间会尝试数值或字符串转换；任一比较项为 `NULL` 时通常返回 `NULL`，而不是 `0`。项目还在函数总表中列出 `type`、`is_null`、`is_int`、`is_number`、`is_string`、`is_array`、`is_map`、`is_datetime` 等检查函数，以及 `convert_int`、`convert_float`、`convert_decimal`、`convert_string`、`convert_array`、`convert_map`、`convert_datetime` 等转换函数。跨数据源关联时应显式转换为一致类型。

### JSON 与正则

JSON 模块已注册 `json_contains`、`json_contains_path`、`json_extract`、`json_depth`、`json_keys`、`json_length`、`json_valid`、`json_set`、`json_remove` 等：

```sql
SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[2][*]');
SELECT JSON_SET('{"a": 1}', '$.a', 2), JSON_VALID('{"a": 1}');
```

路径支持 `$`、点路径、数组下标与 `[*]`；不存在的路径可得到 `NULL`。JSON 输入为 `NULL` 时，路径读取会失败并由计算器归为 `NULL`，因此先用 `JSON_VALID` 检查不可信字符串。

正则模块已注册 `regexp`/`regexp_like`、`regexp_instr`、`regexp_replace`、`regexp_substr` 和 `like`：

```sql
SELECT REGEXP_LIKE('fo\nfo', '^fo$', 'm');
SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 2);
```

实现使用 Python `re`：默认忽略大小写，`c` 关闭忽略大小写，`m`、`n`、`u` 分别映射多行、DOTALL、Unicode 标志。模式错误时，匹配类函数可能返回 `0`，替换/子串函数可能返回 `NULL`；不要按 MySQL 正则方言推断全部兼容性。

## 集合、窗口与行生成

### 聚合

默认注册：`count`（含 `count(distinct expr)`）、`sum`、`max`、`min`、`avg`、`group_concat`、`group_array`、`group_uniq_array`、`group_bit_and`、`group_bit_or`、`group_bit_xor`、`json_arrayagg`、`json_objectagg`、`any_value`。

```sql
SELECT uid, COUNT(DISTINCT goods_id), SUM(amount), GROUP_ARRAY(goods_id)
FROM `data/orders.json`
WHERE status = 0
GROUP BY uid;
```

`count(expr)` 与 `count(distinct expr)` 忽略 `NULL`，无值时分别收敛为 `0`；`sum` 忽略 `NULL`，无有效输入时为 `0`，且会尝试数值转换。其余集合聚合的输出类型（字符串、数组或 JSON）应以结果消费方式为准，不要假定数据库端同名函数的格式。

### 窗口函数

默认注册：`row_number`、`rank`、`dense_rank`、`percent_rank`、`cume_dist`、`first_value`、`last_value`、`nth_value`、`ntile`、`lag`、`lead`。其中排序相关计算要求在 `OVER (...)` 中给出 `ORDER BY`；否则实现会报出“require order by”。

```sql
SELECT order_id,
       ROW_NUMBER() OVER (PARTITION BY uid ORDER BY amount DESC) AS rn,
       RANK() OVER (PARTITION BY uid ORDER BY amount DESC) AS rk,
       LAG(order_id, 2, 0) OVER (ORDER BY order_id) AS previous_id
FROM `data/orders.json`;
```

`ntile` 的桶数必须大于 0；`nth_value` 越界时返回其第三个参数（若提供），否则 `NULL`。窗口中的 `count`、`sum` 等聚合使用 `OVER (...)` 的示例也在 `examples/window_aggregate/window.sql`。

### 生成函数

当前默认注册的生成函数只有 `yield_array(values)`。它会逐项展开 Python `list`；非 `list` 值作为单个元素产出：

```sql
SELECT YIELD_ARRAY(('a', 'b', 'c')) AS value;
SELECT YIELD_ARRAY(data) FROM (SELECT json$decode('[1, 2, 3]') AS data) t;
```

不要将示例中的 `range_count` 或其他生成函数视为内置：它们需要示例通过 `USE generate_customize` 加载扩展。

## 继续阅读

- 完整名称与较多标量示例：[项目函数总表](../../../docs/functions.md)。
- 聚合实例：[`examples/aggregate/aggregate.sql`](../../../examples/aggregate/aggregate.sql)。
- 窗口实例：[`examples/window_aggregate/window.sql`](../../../examples/window_aggregate/window.sql)。
- JSON、日期时间、逻辑/正则实例：[`examples/json/json.sql`](../../../examples/json/json.sql)、[`examples/datetime/datetime.sql`](../../../examples/datetime/datetime.sql)、[`examples/logic_operation/logic_operation.sql`](../../../examples/logic_operation/logic_operation.sql)。
