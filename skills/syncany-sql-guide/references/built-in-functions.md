# Syncany-SQL 内置函数参考

> **适用范围：** 本页列出常用的 Syncany-SQL 标量、聚合、窗口和生成函数。函数在本地执行，函数名相同不表示与 MySQL 的参数、类型转换或边界行为完全一致。SQL 结构和数据操作请阅读[SQL 语言参考](sql-language.md)。

## 使用函数前的规则

- 未列出的函数不要假定可用；自定义函数是否可用取决于当前运行环境。
- 输入类型不确定时先显式转换，并在样本数据上验证结果。
- 许多无效转换、无效日期、JSON/正则错误或除零情形会得到 `NULL`；不要依赖数据库式错误行为。
- `NULL` 参与比较通常得到 `NULL`，而不是 `0`。使用 `IS NULL` 判断空值，不要用 `= NULL`。
- **`CONCAT` 的任一参数为 `NULL` 时，结果为 `NULL`。** 如需将缺失值视为空字符串，先使用 `IFNULL` 或 `COALESCE`。

```sql
SELECT CONCAT(first_name, ' ', last_name) AS full_name
FROM users;

-- 缺失的 last_name 按空字符串处理
SELECT CONCAT(first_name, ' ', IFNULL(last_name, '')) AS full_name
FROM users;

SELECT COALESCE(nickname, name, '匿名用户') AS display_name
FROM users;
```

## 数值、算术与位运算

常用函数：`ABS`、`SQRT`、`EXP`、`LN`、`LOG`、`CEIL` / `CEILING`、`FLOOR`、`RAND`、`ROUND`、`SIGN`、`POW` / `POWER` 及三角函数。

```sql
SELECT ABS(-8), ROUND(12.3456, 2), CEIL(2.1), POWER(2, 3);
SELECT order_id, ROUND(amount / 100, 2) AS amount_hundreds
FROM orders;
SELECT 2 & 1 AS bit_and, 2 | 1 AS bit_or, ~2 AS bit_not;
```

可使用 `+`、`-`、`*`、`/`、`%` 及 `&`、`|`、`~` 等运算符。数值与位运算会尝试转换输入；遇到 `NULL`、无效数值或除零时，按可能返回 `NULL` 处理，并在结果使用前显式兜底：

```sql
SELECT COALESCE(ROUND(amount / NULLIF(quantity, 0), 2), 0) AS unit_price
FROM order_items;
```

## 字符串与编码

常用函数：

- 拼接与替换：`CONCAT`、`CONCAT_WS`、`REPLACE`、`REPEAT`、`REVERSE`
- 截取与修整：`SUBSTRING` / `SUBSTR`、`SUBSTRING_INDEX`、`LEFT`、`RIGHT`、`TRIM`
- 大小写与长度：`LOWER`、`UPPER` / `UCASE`、`LENGTH`、`CHAR_LENGTH` / `CHARACTER_LENGTH`
- 其他：`STRCMP`、`STARTSWITH`、`ENDSWITH`、`CONTAINS`、`HEX`、`UNHEX`、`TO_BASE64`、`FROM_BASE64`

```sql
SELECT SUBSTRING('abcdef', 2, 3), LOWER('AbC'), TRIM('  text  ');
SELECT LEFT('abcdef', 3), REPLACE('a-b-c', '-', '/'), REPEAT('x', 3);
SELECT LENGTH('你好') AS utf8_bytes, CHAR_LENGTH('你好') AS characters;
```

`LENGTH` 计算 UTF-8 字节数，`CHAR_LENGTH` 计算字符数。单参数字符串函数遇到 `NULL` 通常返回 `NULL`；拼接前按业务规则使用 `IFNULL` 或 `COALESCE`。

## 日期与时间

常用函数：`NOW`、`CURDATE` / `CURRENT_DATE`、`CURTIME` / `CURRENT_TIME`、`CURRENT_TIMESTAMP`、`DATE_ADD` / `ADDDATE`、`DATE_SUB` / `SUBDATE`、`ADDTIME`、`SUBTIME`、`DATE_FORMAT`、`TIME_FORMAT`、`FROM_UNIXTIME`、`UNIX_TIMESTAMP`。

```sql
SELECT NOW(), CURDATE(), CURRENT_TIMESTAMP;
SELECT DATE_ADD(NOW(), INTERVAL 7 DAY), DATE_SUB(NOW(), INTERVAL 1 MONTH);
SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%M:%S');
```

日期字符串、时区及格式的兼容性应在目标环境验证。无法解析的日期时间值可能得到 `NULL`；导入外部数据时，应先统一格式并保留原始值以便排查。

## 条件、比较、空值与类型

可使用 `IF`、`IFNULL`、`COALESCE`、`NULLIF`、`CASE`，以及 `IS NULL`、`IN`、`LIKE` 等表达式。

```sql
SELECT IF(amount > 0, 'paid', 'free') AS payment_type
FROM orders;

SELECT CASE
         WHEN amount IS NULL THEN 'unknown'
         WHEN amount <= 0 THEN 'free'
         ELSE 'paid'
       END AS payment_type
FROM orders;

SELECT NULLIF(quantity, 0) AS nonzero_quantity
FROM order_items;
```

类型检查与转换可使用 `TYPE`、`IS_NULL`、`IS_INT`、`IS_NUMBER`、`IS_STRING`、`IS_ARRAY`、`IS_MAP`、`IS_DATETIME`，以及 `CONVERT_INT`、`CONVERT_FLOAT`、`CONVERT_DECIMAL`、`CONVERT_STRING`、`CONVERT_ARRAY`、`CONVERT_MAP`、`CONVERT_DATETIME`。

```sql
SELECT CONVERT_INT('42') AS id,
       CONVERT_DECIMAL('12.50') AS price,
       CONVERT_STRING(user_id) AS user_id_text
FROM users;
```

跨数据源比较或连接时，先将两侧转换为一致类型。不要依赖隐式类型转换来匹配标识符。

## JSON 与正则表达式

JSON 函数包括 `JSON_CONTAINS`、`JSON_CONTAINS_PATH`、`JSON_EXTRACT`、`JSON_DEPTH`、`JSON_KEYS`、`JSON_LENGTH`、`JSON_VALID`、`JSON_SET`、`JSON_REMOVE`。

```sql
SELECT JSON_EXTRACT('{"user":{"name":"Lin"}}', '$.user.name') AS name;
SELECT JSON_SET('{"enabled": false}', '$.enabled', true) AS updated_json;
SELECT JSON_VALID(payload) AS is_valid_json
FROM events;
```

JSON 路径可使用根路径、点路径、数组下标与 `[*]`。不存在的路径或无效 JSON 可能返回 `NULL`；处理不可信输入时先用 `JSON_VALID`。

正则函数包括 `REGEXP` / `REGEXP_LIKE`、`REGEXP_INSTR`、`REGEXP_REPLACE`、`REGEXP_SUBSTR` 和 `LIKE`：

```sql
SELECT REGEXP_LIKE('order-2026', '^order-[0-9]+$') AS is_order_code;
SELECT REGEXP_REPLACE('a1 b2', '[0-9]', '') AS letters_only;
```

正则采用 Python 风格的 `re` 行为，而非完整 MySQL 正则方言。无效模式可能返回 `0` 或 `NULL`；不要将用户提供的复杂模式直接用于大数据集。

## 聚合函数

可用聚合包括 `COUNT`、`SUM`、`MAX`、`MIN`、`AVG`、`GROUP_CONCAT`、`GROUP_ARRAY`、`GROUP_UNIQ_ARRAY`、`GROUP_BIT_AND`、`GROUP_BIT_OR`、`GROUP_BIT_XOR`、`JSON_ARRAYAGG`、`JSON_OBJECTAGG`、`ANY_VALUE`。

```sql
SELECT uid,
       COUNT(*) AS order_count,
       COUNT(DISTINCT goods_id) AS goods_count,
       SUM(amount) AS total_amount,
       GROUP_ARRAY(goods_id) AS goods_ids
FROM orders
WHERE status = 0
GROUP BY uid;
```

- `COUNT(expr)` 与 `COUNT(DISTINCT expr)` 忽略 `NULL`；无有效值时为 `0`。
- `SUM` 忽略 `NULL`，无有效输入时为 `0`，并会尝试数值转换。
- 集合类聚合的输出可能是字符串、数组或 JSON；按实际结果类型消费，不要假定同名数据库函数的序列化格式。

聚合写法、`HAVING` 和性能注意事项见[SQL 语言参考](sql-language.md)。

## 窗口函数

可用窗口函数：`ROW_NUMBER`、`RANK`、`DENSE_RANK`、`PERCENT_RANK`、`CUME_DIST`、`FIRST_VALUE`、`LAST_VALUE`、`NTH_VALUE`、`NTILE`、`LAG`、`LEAD`。

```sql
SELECT order_id,
       ROW_NUMBER() OVER (PARTITION BY uid ORDER BY amount DESC) AS row_no,
       RANK() OVER (PARTITION BY uid ORDER BY amount DESC) AS amount_rank,
       LAG(amount, 1, 0) OVER (PARTITION BY uid ORDER BY order_id) AS previous_amount
FROM orders;
```

- 排名和偏移等排序相关函数应在 `OVER (...)` 内提供 `ORDER BY`。
- `NTILE` 的桶数必须大于 `0`。
- `NTH_VALUE` 越界时返回第三个参数；未提供第三个参数时返回 `NULL`。
- 窗口计算对大分区可能消耗较多内存，应先用 `WHERE` 缩小数据范围。

## 生成函数

`YIELD_ARRAY(values)` 可将数组逐项展开为多行：

```sql
SELECT YIELD_ARRAY(tags) AS tag
FROM articles;
```

它只会展开列表值；非列表值按单个元素处理。处理前先确认字段类型，并避免对不受控的大数组直接展开。
