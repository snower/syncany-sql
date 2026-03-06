# 支持的函数

- [内置函数](#内置函数)
- [支持的MySQL常用函数](#支持的MySQL常用函数)
- [聚合函数](#聚合函数)
- [窗口函数](#窗口函数)
- [YIELD函数](#YIELD函数)
- [transform转换函数](#transform转换函数)
- [时间窗口函数](#时间窗口函数)

## 内置函数

- type(expr)
- is_null(expr)
- is_int(expr)
- is_float(expr)
- is_decimal(expr)
- is_number(expr)
- is_string(expr)
- is_bytes(expr)
- is_bool(expr)
- is_array(expr)
- is_set(expr)
- is_map(expr)
- is_objectid(expr)
- is_uuid(expr)
- is_datetime(expr)
- is_date(expr)
- is_time(expr)
- convert_int(expr)
- convert_float(expr)
- convert_decimal(expr)
- convert_string(expr)
- convert_bytes(expr)
- convert_bool(expr)
- convert_array(expr)
- convert_set(expr)
- convert_map(expr)
- convert_objectid(expr)
- convert_uuid(expr)
- convert_datetime(expr)
- convert_date(expr)
- convert_time(expr)
- range()
- substring()
- split()
- join()
- now()
- current_env_variable()
- objectid
- uuid
- snowflakeid

## 支持的MySQL常用函数

### 位运算函数

#### bitwiseand(x, y)
**功能**：对两个整数执行按位与运算（x & y）。

**参数**：
- `x`：整数
- `y`：整数

**返回值**：整数，x和y的按位与结果

**SQL示例**：
```sql
SELECT bitwiseand(5, 3);   -- 返回 1 (二进制: 101 & 011 = 001)
SELECT bitwiseand(15, 7);  -- 返回 7 (二进制: 1111 & 0111 = 0111)
```

---

#### bitwiseor(x, y)
**功能**：对两个整数执行按位或运算（x | y）。

**参数**：
- `x`：整数
- `y`：整数

**返回值**：整数，x和y的按位或结果

**SQL示例**：
```sql
SELECT bitwiseor(5, 3);   -- 返回 7 (二进制: 101 | 011 = 111)
SELECT bitwiseor(8, 3);   -- 返回 11 (二进制: 1000 | 0011 = 1011)
```

---

#### bitwisenot(x)
**功能**：对整数执行按位取反运算（~x）。

**参数**：
- `x`：整数

**返回值**：整数，x的按位取反结果

**SQL示例**：
```sql
SELECT bitwisenot(5);   -- 返回 -6 (二进制: ~0101 = ...11111010)
SELECT bitwisenot(0);   -- 返回 -1
```

---

#### bitwisexor(x, y)
**功能**：对两个整数执行按位异或运算（x ^ y）。

**参数**：
- `x`：整数
- `y`：整数

**返回值**：整数，x和y的按位异或结果

**SQL示例**：
```sql
SELECT bitwisexor(5, 3);   -- 返回 6 (二进制: 101 ^ 011 = 110)
SELECT bitwisexor(7, 7);   -- 返回 0 (相同数异或结果为0)
```

---

#### bitwiserightshift(x, y)
**功能**：将整数x右移y位（x >> y）。

**参数**：
- `x`：整数
- `y`：整数，移动位数

**返回值**：整数，右移后的结果

**SQL示例**：
```sql
SELECT bitwiserightshift(8, 2);   -- 返回 2 (8 >> 2 = 2)
SELECT bitwiserightshift(16, 3);  -- 返回 2 (16 >> 3 = 2)
```

---

#### bitwiseleftshift(x, y)
**功能**：将整数x左移y位（x << y）。

**参数**：
- `x`：整数
- `y`：整数，移动位数

**返回值**：整数，左移后的结果

**SQL示例**：
```sql
SELECT bitwiseleftshift(2, 3);   -- 返回 16 (2 << 3 = 16)
SELECT bitwiseleftshift(1, 4);   -- 返回 16 (1 << 4 = 16)
```

---

### 数学函数

#### abs(x)
**功能**：返回x的绝对值。

**参数**：
- `x`：数值

**返回值**：数值，x的绝对值

**SQL示例**：
```sql
SELECT abs(-5);     -- 返回 5
SELECT abs(3.14);   -- 返回 3.14
SELECT abs(0);      -- 返回 0
```

---

#### sqrt(x)
**功能**：返回x的平方根。

**参数**：
- `x`：非负数值

**返回值**：浮点数，x的平方根

**SQL示例**：
```sql
SELECT sqrt(16);   -- 返回 4.0
SELECT sqrt(2);    -- 返回 1.4142135623730951
```

---

#### exp(x)
**功能**：返回e（自然对数的底）的x次方。

**参数**：
- `x`：数值，指数

**返回值**：浮点数，e的x次方

**SQL示例**：
```sql
SELECT exp(1);    -- 返回 2.718281828459045 (e的值)
SELECT exp(0);    -- 返回 1.0
SELECT exp(2);    -- 返回 7.38905609893065
```

---

#### pi()
**功能**：返回圆周率π的值。

**参数**：无

**返回值**：浮点数，π ≈ 3.141592653589793

**SQL示例**：
```sql
SELECT pi();   -- 返回 3.141592653589793
```

---

#### ln(x)
**功能**：返回x的自然对数（以e为底）。

**参数**：
- `x`：正数值

**返回值**：浮点数，x的自然对数

**SQL示例**：
```sql
SELECT ln(2.718281828459045);   -- 返回约 1.0
SELECT ln(1);                    -- 返回 0.0
SELECT ln(10);                   -- 返回 2.302585092994046
```

---

#### log(x, base)
**功能**：返回x以base为底的对数。

**参数**：
- `x`：正数值
- `base`：对数的底，默认为10

**返回值**：浮点数，log_base(x)

**SQL示例**：
```sql
SELECT log(100);      -- 返回 2.0 (以10为底)
SELECT log(8, 2);     -- 返回 3.0 (以2为底)
SELECT log(27, 3);    -- 返回 3.0 (以3为底)
```

---

#### ceil(x) / ceiling(x)
**功能**：返回大于或等于x的最小整数（向上取整）。

**参数**：
- `x`：数值

**返回值**：整数，向上取整后的结果

**SQL示例**：
```sql
SELECT ceil(3.2);      -- 返回 4
SELECT ceiling(3.9);   -- 返回 4
SELECT ceil(-3.2);     -- 返回 -3
```

---

#### floor(x)
**功能**：返回小于或等于x的最大整数（向下取整）。

**参数**：
- `x`：数值

**返回值**：整数，向下取整后的结果

**SQL示例**：
```sql
SELECT floor(3.9);    -- 返回 3
SELECT floor(3.2);    -- 返回 3
SELECT floor(-3.2);   -- 返回 -4
```

---

#### rand()
**功能**：返回0到1之间的随机浮点数。

**参数**：无

**返回值**：浮点数，范围[0.0, 1.0)

**SQL示例**：
```sql
SELECT rand();                    -- 返回随机数，如 0.71589423
SELECT floor(rand() * 100);       -- 返回 0-99 之间的随机整数
```

---

#### round(x, y)
**功能**：将x四舍五入到y位小数。

**参数**：
- `x`：数值
- `y`：小数位数，默认为2

**返回值**：浮点数，四舍五入后的结果

**SQL示例**：
```sql
SELECT round(3.14159, 2);   -- 返回 3.14
SELECT round(3.14159, 3);   -- 返回 3.142
SELECT round(3.5);          -- 返回 3.5 (默认保留2位)
```

---

#### sign(x)
**功能**：返回x的符号。

**参数**：
- `x`：数值

**返回值**：整数
- 1：x > 0
- 0：x = 0
- -1：x < 0

**SQL示例**：
```sql
SELECT sign(5);    -- 返回 1
SELECT sign(0);    -- 返回 0
SELECT sign(-3);   -- 返回 -1
```

---

#### pow(x, y) / power(x, y)
**功能**：返回x的y次方。

**参数**：
- `x`：底数
- `y`：指数

**返回值**：浮点数，x^y

**SQL示例**：
```sql
SELECT pow(2, 3);     -- 返回 8.0
SELECT power(3, 2);   -- 返回 9.0
SELECT pow(2, 0.5);   -- 返回 1.4142135623730951 (√2)
```

---

#### sin(x)
**功能**：返回x的正弦值（x为弧度）。

**参数**：
- `x`：数值，弧度值

**返回值**：浮点数，sin(x)

**SQL示例**：
```sql
SELECT sin(0);                -- 返回 0.0
SELECT sin(pi()/2);           -- 返回 1.0
SELECT sin(pi()/6);           -- 返回 0.5
```

---

#### asin(x)
**功能**：返回x的反正弦值（返回弧度）。

**参数**：
- `x`：数值，范围[-1, 1]

**返回值**：浮点数，arcsin(x)，范围[-π/2, π/2]

**SQL示例**：
```sql
SELECT asin(0);     -- 返回 0.0
SELECT asin(1);     -- 返回 1.5707963267948966 (π/2)
SELECT asin(0.5);   -- 返回 0.5235987755982989 (π/6)
```

---

#### cos(x)
**功能**：返回x的余弦值（x为弧度）。

**参数**：
- `x`：数值，弧度值

**返回值**：浮点数，cos(x)

**SQL示例**：
```sql
SELECT cos(0);       -- 返回 1.0
SELECT cos(pi());    -- 返回 -1.0
SELECT cos(pi()/3);  -- 返回 0.5
```

---

#### acos(x)
**功能**：返回x的反余弦值（返回弧度）。

**参数**：
- `x`：数值，范围[-1, 1]

**返回值**：浮点数，arccos(x)，范围[0, π]

**SQL示例**：
```sql
SELECT acos(1);     -- 返回 0.0
SELECT acos(0);     -- 返回 1.5707963267948966 (π/2)
SELECT acos(-1);    -- 返回 3.141592653589793 (π)
```

---

#### tan(x)
**功能**：返回x的正切值（x为弧度）。

**参数**：
- `x`：数值，弧度值

**返回值**：浮点数，tan(x)

**SQL示例**：
```sql
SELECT tan(0);        -- 返回 0.0
SELECT tan(pi()/4);   -- 返回约 1.0
```

---

#### atan(x)
**功能**：返回x的反正切值（返回弧度）。

**参数**：
- `x`：数值

**返回值**：浮点数，arctan(x)，范围[-π/2, π/2]

**SQL示例**：
```sql
SELECT atan(0);    -- 返回 0.0
SELECT atan(1);    -- 返回 0.7853981633974483 (π/4)
```

---

#### greatest(expr, [expr, expr, ...])
**功能**：返回所有参数中的最大值。

**参数**：
- `expr`：可变数量的数值参数

**返回值**：数值，所有参数中的最大值

**SQL示例**：
```sql
SELECT greatest(1, 5, 3, 2);      -- 返回 5
SELECT greatest(-1, -5, -3);      -- 返回 -1
SELECT greatest(1.5, 2, 3.5);     -- 返回 3.5
```

---

#### least(expr, [expr, expr, ...])
**功能**：返回所有参数中的最小值。

**参数**：
- `expr`：可变数量的数值参数

**返回值**：数值，所有参数中的最小值

**SQL示例**：
```sql
SELECT least(1, 5, 3, 2);      -- 返回 1
SELECT least(-1, -5, -3);      -- 返回 -5
SELECT least(1.5, 2, 3.5);     -- 返回 1.5
```

---

### 字符串函数

#### bin(x)
**功能**：返回整数x的二进制字符串表示。

**参数**：
- `x`：整数

**返回值**：字符串，二进制表示（带'0b'前缀）

**SQL示例**：
```sql
SELECT bin(5);    -- 返回 '0b101'
SELECT bin(255);  -- 返回 '0b11111111'
SELECT bin(0);    -- 返回 '0b0'
```

---

#### hex(x)
**功能**：返回整数x的十六进制字符串表示，或字符串的十六进制编码。

**参数**：
- `x`：整数或字符串

**返回值**：字符串，十六进制表示

**SQL示例**：
```sql
SELECT hex(255);       -- 返回 '0xff'
SELECT hex(16);        -- 返回 '0x10'
SELECT hex('abc');     -- 返回 '616263' (字符串编码)
```

---

#### unhex(x)
**功能**：将十六进制字符串解码为原始字节。

**参数**：
- `x`：十六进制字符串

**返回值**：字符串，解码后的原始字符串

**SQL示例**：
```sql
SELECT unhex('616263');   -- 返回 'abc'
SELECT unhex('48454c4c4f');  -- 返回 'HELLO'
```

---

#### oct(x)
**功能**：返回整数x的八进制字符串表示。

**参数**：
- `x`：整数

**返回值**：字符串，八进制表示

**SQL示例**：
```sql
SELECT oct(8);    -- 返回 '10'
SELECT oct(64);   -- 返回 '100'
SELECT oct(255);  -- 返回 '377'
```

---

#### ord(x)
**功能**：返回字符串第一个字符的Unicode码点。

**参数**：
- `x`：字符串

**返回值**：整数，第一个字符的码点

**SQL示例**：
```sql
SELECT ord('A');     -- 返回 65
SELECT ord('中');    -- 返回 20013
SELECT ord('abc');   -- 返回 97 (只返回第一个字符)
```

---

#### ascii(s)
**功能**：返回字符串中所有字符的ASCII码值之和。

**参数**：
- `s`：字符串

**返回值**：整数，所有字符ASCII码之和

**SQL示例**：
```sql
SELECT ascii('A');     -- 返回 65
SELECT ascii('ABC');   -- 返回 198 (65+66+67)
SELECT ascii('a');     -- 返回 97
```

---

#### char(expr, [expr, expr, ...])
**功能**：将多个整数转换为对应的ASCII字符并连接成字符串。

**参数**：
- `expr`：整数，ASCII码值

**返回值**：字符串，由对应字符组成

**SQL示例**：
```sql
SELECT char(65);           -- 返回 'A'
SELECT char(65, 66, 67);   -- 返回 'ABC'
SELECT char(72, 73);       -- 返回 'HI'
```

---

#### bit_length(s)
**功能**：返回字符串的位长度（字节数 × 8）。

**参数**：
- `s`：字符串

**返回值**：整数，位长度

**SQL示例**：
```sql
SELECT bit_length('abc');    -- 返回 24 (3字节 × 8位)
SELECT bit_length('中国');   -- 返回 48 (6字节 × 8位，UTF-8编码)
```

---

#### length(s)
**功能**：返回字符串的字节长度（UTF-8编码）。

**参数**：
- `s`：字符串

**返回值**：整数，字节数

**SQL示例**：
```sql
SELECT length('abc');    -- 返回 3
SELECT length('中国');   -- 返回 6 (UTF-8编码每个中文字符3字节)
SELECT length('hello');  -- 返回 5
```

---

#### char_length(s) / character_length(s)
**功能**：返回字符串的字符数。

**参数**：
- `s`：字符串

**返回值**：整数，字符数

**SQL示例**：
```sql
SELECT char_length('abc');        -- 返回 3
SELECT char_length('中国');       -- 返回 2
SELECT character_length('hello'); -- 返回 5
```

---

#### concat(expr, [expr, expr, ...])
**功能**：连接多个字符串。

**参数**：
- `expr`：可变数量的字符串参数

**返回值**：字符串，连接后的结果

**SQL示例**：
```sql
SELECT concat('Hello', ' ', 'World');   -- 返回 'Hello World'
SELECT concat('a', 'b', 'c');           -- 返回 'abc'
SELECT concat('价格:', 100);             -- 返回 '价格:100'
```

---

#### concat_ws(sep, ...)
**功能**：使用指定分隔符连接多个字符串。

**参数**：
- `sep`：分隔符字符串
- `...`：可变数量的字符串参数

**返回值**：字符串，用分隔符连接后的结果

**SQL示例**：
```sql
SELECT concat_ws('-', '2024', '01', '15');   -- 返回 '2024-01-15'
SELECT concat_ws(',', 'a', 'b', 'c');        -- 返回 'a,b,c'
SELECT concat_ws('|', 'id', 'name', 'age');  -- 返回 'id|name|age'
```

---

#### insert(s1, x, l, s2)
**功能**：在字符串s1的位置x开始，删除l个字符，然后插入s2。

**参数**：
- `s1`：原字符串
- `x`：起始位置（从1开始）
- `l`：删除的字符数
- `s2`：要插入的字符串

**返回值**：字符串，修改后的结果

**SQL示例**：
```sql
SELECT insert('abcdef', 2, 3, 'XYZ');   -- 返回 'aXYZef'
SELECT insert('hello', 3, 2, 'LL');     -- 返回 'heLLo'
```

---

#### lower(s)
**功能**：将字符串转换为小写。

**参数**：
- `s`：字符串

**返回值**：字符串，小写形式

**SQL示例**：
```sql
SELECT lower('HELLO');    -- 返回 'hello'
SELECT lower('Hello');    -- 返回 'hello'
SELECT lower('ABC123');   -- 返回 'abc123'
```

---

#### upper(s) / ucase(s)
**功能**：将字符串转换为大写。

**参数**：
- `s`：字符串

**返回值**：字符串，大写形式

**SQL示例**：
```sql
SELECT upper('hello');    -- 返回 'HELLO'
SELECT ucase('Hello');    -- 返回 'HELLO'
SELECT upper('abc123');   -- 返回 'ABC123'
```

---

#### left(s, x)
**功能**：返回字符串左边x个字符。

**参数**：
- `s`：字符串
- `x`：字符数

**返回值**：字符串，左边x个字符

**SQL示例**：
```sql
SELECT left('Hello World', 5);   -- 返回 'Hello'
SELECT left('abc', 2);           -- 返回 'ab'
SELECT left('中国', 1);          -- 返回 '中'
```

---

#### right(s, x)
**功能**：返回字符串右边x个字符。

**参数**：
- `s`：字符串
- `x`：字符数

**返回值**：字符串，右边x个字符

**SQL示例**：
```sql
SELECT right('Hello World', 5);   -- 返回 'World'
SELECT right('abc', 2);           -- 返回 'bc'
SELECT right('中国', 1);          -- 返回 '国'
```

---

#### trim(s)
**功能**：去除字符串两端的空白字符。

**参数**：
- `s`：字符串

**返回值**：字符串，去除两端空白后的结果

**SQL示例**：
```sql
SELECT trim('  hello  ');      -- 返回 'hello'
SELECT trim('\t\nabc\n\t');    -- 返回 'abc'
```

---

#### elt(n, ...)
**功能**：返回第n个参数的值。

**参数**：
- `n`：位置索引（从1开始）
- `...`：可变数量的参数

**返回值**：第n个参数的值，如果n超出范围返回NULL

**SQL示例**：
```sql
SELECT elt(2, 'a', 'b', 'c');   -- 返回 'b'
SELECT elt(1, 'one', 'two');    -- 返回 'one'
SELECT elt(5, 'a', 'b');        -- 返回 NULL
```

---

#### field(s, ...)
**功能**：返回字符串s在后续参数中的位置（从1开始）。

**参数**：
- `s`：要查找的字符串
- `...`：可变数量的字符串参数

**返回值**：整数，位置索引；如果未找到返回0

**SQL示例**：
```sql
SELECT field('b', 'a', 'b', 'c');   -- 返回 2
SELECT field('x', 'a', 'b', 'c');   -- 返回 0
SELECT field('a', 'a', 'b', 'a');   -- 返回 1 (返回第一个匹配)
```

---

#### find_in_set(s, ss)
**功能**：返回字符串s在逗号分隔字符串ss中的位置。

**参数**：
- `s`：要查找的字符串
- `ss`：逗号分隔的字符串列表

**返回值**：整数，位置索引（从1开始）；如果未找到返回0

**SQL示例**：
```sql
SELECT find_in_set('b', 'a,b,c');   -- 返回 2
SELECT find_in_set('x', 'a,b,c');   -- 返回 0
SELECT find_in_set('2', '1,2,3');   -- 返回 2
```

---

#### replace(s, s1, s2)
**功能**：将字符串s中的所有s1替换为s2。

**参数**：
- `s`：原字符串
- `s1`：要被替换的子串
- `s2`：替换后的子串

**返回值**：字符串，替换后的结果

**SQL示例**：
```sql
SELECT replace('hello world', 'world', 'SQL');   -- 返回 'hello SQL'
SELECT replace('aaa', 'a', 'b');                  -- 返回 'bbb'
SELECT replace('2024-01-15', '-', '/');           -- 返回 '2024/01/15'
```

---

#### substring(s, n, l) / substr(s, n, l)
**功能**：从字符串s的第n个位置开始，截取l个字符。

**参数**：
- `s`：字符串
- `n`：起始位置（从1开始）
- `l`：截取长度（可选）

**返回值**：字符串，截取的子串

**SQL示例**：
```sql
SELECT substring('Hello World', 1, 5);   -- 返回 'Hello'
SELECT substring('Hello World', 7);      -- 返回 'World'
SELECT substr('abcdef', 2, 3);           -- 返回 'bcd'
```

---

#### substring_index(s, d, c)
**功能**：返回字符串s中第c个分隔符d之前（或之后）的子串。

**参数**：
- `s`：字符串
- `d`：分隔符
- `c`：计数（正数取左边，负数取右边）

**返回值**：字符串

**SQL示例**：
```sql
SELECT substring_index('www.mysql.com', '.', 2);    -- 返回 'www.mysql'
SELECT substring_index('www.mysql.com', '.', -1);   -- 返回 'com'
SELECT substring_index('a,b,c,d', ',', 3);          -- 返回 'a,b,c'
```

---

#### repeat(s, c)
**功能**：将字符串s重复c次。

**参数**：
- `s`：字符串
- `c`：重复次数

**返回值**：字符串，重复后的结果

**SQL示例**：
```sql
SELECT repeat('ab', 3);    -- 返回 'ababab'
SELECT repeat('*', 5);     -- 返回 '*****'
SELECT repeat('x', 0);     -- 返回 ''
```

---

#### reverse(s)
**功能**：反转字符串。

**参数**：
- `s`：字符串

**返回值**：字符串，反转后的结果

**SQL示例**：
```sql
SELECT reverse('hello');   -- 返回 'olleh'
SELECT reverse('abc');     -- 返回 'cba'
SELECT reverse('中国');    -- 返回 '国中'
```

---

#### strcmp(s1, s2)
**功能**：比较两个字符串。

**参数**：
- `s1`：第一个字符串
- `s2`：第二个字符串

**返回值**：整数
- 0：s1 == s2
- -1：s1 < s2
- 1：s1 > s2

**SQL示例**：
```sql
SELECT strcmp('abc', 'abc');   -- 返回 0
SELECT strcmp('abc', 'abd');   -- 返回 -1
SELECT strcmp('abd', 'abc');   -- 返回 1
```

---

#### startswith(s1, s2)
**功能**：判断字符串s1是否以s2开头。

**参数**：
- `s1`：要检查的字符串
- `s2`：前缀字符串

**返回值**：整数，1表示是，0表示否

**SQL示例**：
```sql
SELECT startswith('Hello World', 'Hello');   -- 返回 1
SELECT startswith('Hello World', 'World');   -- 返回 0
```

---

#### endswith(s1, s2)
**功能**：判断字符串s1是否以s2结尾。

**参数**：
- `s1`：要检查的字符串
- `s2`：后缀字符串

**返回值**：整数，1表示是，0表示否

**SQL示例**：
```sql
SELECT endswith('Hello World', 'World');   -- 返回 1
SELECT endswith('Hello World', 'Hello');   -- 返回 0
```

---

#### contains(s1, s2)
**功能**：判断字符串s1是否包含s2。

**参数**：
- `s1`：要检查的字符串
- `s2`：子字符串

**返回值**：整数，1表示包含，0表示不包含

**SQL示例**：
```sql
SELECT contains('Hello World', 'World');   -- 返回 1
SELECT contains('Hello World', 'xyz');     -- 返回 0
```

---

#### crc32(s)
**功能**：计算字符串的CRC32校验值。

**参数**：
- `s`：字符串

**返回值**：整数，CRC32校验值

**SQL示例**：
```sql
SELECT crc32('hello');   -- 返回 907060870
SELECT crc32('world');   -- 返回 3412743245
```

---

#### from_base64(s)
**功能**：将Base64编码的字符串解码。

**参数**：
- `s`：Base64编码的字符串

**返回值**：字符串，解码后的原始字符串

**SQL示例**：
```sql
SELECT from_base64('SGVsbG8gV29ybGQ=');   -- 返回 'Hello World'
SELECT from_base64('YWJj');               -- 返回 'abc'
```

---

#### to_base64(s)
**功能**：将字符串编码为Base64格式。

**参数**：
- `s`：字符串

**返回值**：字符串，Base64编码结果

**SQL示例**：
```sql
SELECT to_base64('Hello World');   -- 返回 'SGVsbG8gV29ybGQ='
SELECT to_base64('abc');           -- 返回 'YWJj'
```

---

#### inet4_aton(s)
**功能**：将IPv4地址字符串转换为数值。

**参数**：
- `s`：IPv4地址字符串

**返回值**：字符串，十六进制表示的数值

**SQL示例**：
```sql
SELECT inet4_aton('192.168.1.1');   -- 返回 'c0a80101'
SELECT inet4_aton('127.0.0.1');     -- 返回 '7f000001'
```

---

#### inet4_ntoa(b)
**功能**：将数值转换为IPv4地址字符串。

**参数**：
- `b`：字节或数值

**返回值**：字符串，IPv4地址

**SQL示例**：
```sql
SELECT inet4_ntoa(0x7f000001);   -- 返回 '127.0.0.1'
SELECT inet4_ntoa(X'c0a80101');  -- 返回 '192.168.1.1'
```

---

#### is_ipv4(s)
**功能**：判断字符串是否为有效的IPv4地址。

**参数**：
- `s`：字符串

**返回值**：整数，1表示是有效IPv4地址，0表示不是

**SQL示例**：
```sql
SELECT is_ipv4('192.168.1.1');   -- 返回 1
SELECT is_ipv4('256.1.1.1');     -- 返回 0 (无效IP)
SELECT is_ipv4('::1');           -- 返回 0 (IPv6)
```

---

#### inet6_aton(s)
**功能**：将IPv6地址字符串转换为数值。

**参数**：
- `s`：IPv6地址字符串

**返回值**：字符串，十六进制表示的数值

**SQL示例**：
```sql
SELECT inet6_aton('::1');                    -- 返回 '00000000000000000000000000000001'
SELECT inet6_aton('2001:db8::1');            -- 返回 '20010db8000000000000000000000001'
```

---

#### inet6_ntoa(b)
**功能**：将数值转换为IPv6地址字符串。

**参数**：
- `b`：字节

**返回值**：字符串，IPv6地址

**SQL示例**：
```sql
SELECT inet6_ntoa(X'00000000000000000000000000000001');   -- 返回 '::1'
```

---

#### is_ipv6(s)
**功能**：判断字符串是否为有效的IPv6地址。

**参数**：
- `s`：字符串

**返回值**：整数，1表示是有效IPv6地址，0表示不是

**SQL示例**：
```sql
SELECT is_ipv6('::1');           -- 返回 1
SELECT is_ipv6('2001:db8::1');   -- 返回 1
SELECT is_ipv6('192.168.1.1');   -- 返回 0 (IPv4)
```

---

### 日期时间函数

#### currenttimestamp()
**功能**：返回当前日期时间。

**参数**：无

**返回值**：datetime对象，当前日期时间（带时区）

**SQL示例**：
```sql
SELECT currenttimestamp();   -- 返回当前时间，如 2024-01-15 10:30:00
```

---

#### curdate() / currentdate()
**功能**：返回当前日期。

**参数**：无

**返回值**：date对象，当前日期

**SQL示例**：
```sql
SELECT curdate();        -- 返回当前日期，如 2024-01-15
SELECT currentdate();    -- 同上
```

---

#### curtime() / currenttime()
**功能**：返回当前时间。

**参数**：无

**返回值**：time对象，当前时间

**SQL示例**：
```sql
SELECT curtime();        -- 返回当前时间，如 10:30:00
SELECT currenttime();    -- 同上
```

---

#### sysdate()
**功能**：返回当前日期时间（同currenttimestamp）。

**参数**：无

**返回值**：datetime对象，当前日期时间

**SQL示例**：
```sql
SELECT sysdate();   -- 返回当前时间，如 2024-01-15 10:30:00
```

---

#### date(dt)
**功能**：从日期时间中提取日期部分。

**参数**：
- `dt`：日期时间值

**返回值**：date对象

**SQL示例**：
```sql
SELECT date('2024-01-15 10:30:00');   -- 返回 2024-01-15
SELECT date('2024-01-15');            -- 返回 2024-01-15
```

---

#### datetime(dt)
**功能**：将值转换为datetime类型。

**参数**：
- `dt`：日期时间值或日期字符串

**返回值**：datetime对象

**SQL示例**：
```sql
SELECT datetime('2024-01-15');            -- 返回 2024-01-15 00:00:00
SELECT datetime('2024-01-15 10:30:00');   -- 返回 2024-01-15 10:30:00
```

---

#### time(dt)
**功能**：从日期时间中提取时间部分。

**参数**：
- `dt`：日期时间值

**返回值**：time对象

**SQL示例**：
```sql
SELECT time('2024-01-15 10:30:00');   -- 返回 10:30:00
SELECT time('15:45:30');              -- 返回 15:45:30
```

---

#### unix_timestamp(dt)
**功能**：将日期时间转换为Unix时间戳。

**参数**：
- `dt`：日期时间值（可选，默认为当前时间）

**返回值**：整数，Unix时间戳（秒）

**SQL示例**：
```sql
SELECT unix_timestamp();                      -- 返回当前时间戳
SELECT unix_timestamp('2024-01-15 00:00:00'); -- 返回指定时间的时间戳
```

---

#### from_unixtime(t)
**功能**：将Unix时间戳转换为日期时间。

**参数**：
- `t`：Unix时间戳（秒）

**返回值**：datetime对象

**SQL示例**：
```sql
SELECT from_unixtime(1705276800);   -- 返回 2024-01-15 00:00:00
```

---

#### month(dt)
**功能**：返回日期中的月份（1-12）。

**参数**：
- `dt`：日期时间值

**返回值**：整数，月份（1-12）

**SQL示例**：
```sql
SELECT month('2024-01-15');    -- 返回 1
SELECT month('2024-06-20');    -- 返回 6
```

---

#### monthname(dt)
**功能**：返回日期中的月份名称。

**参数**：
- `dt`：日期时间值

**返回值**：字符串，月份名称（英文缩写）

**SQL示例**：
```sql
SELECT monthname('2024-01-15');   -- 返回 'Jan'
SELECT monthname('2024-06-20');   -- 返回 'Jun'
```

---

#### dayname(dt)
**功能**：返回日期对应的星期名称。

**参数**：
- `dt`：日期时间值

**返回值**：字符串，星期名称

**SQL示例**：
```sql
SELECT dayname('2024-01-15');   -- 返回 'Monday'
SELECT dayname('2024-01-21');   -- 返回 'Sunday'
```

---

#### dayofweek(dt)
**功能**：返回日期对应的星期几（1=周一，7=周日）。

**参数**：
- `dt`：日期时间值

**返回值**：整数，1-7（周一到周日）

**SQL示例**：
```sql
SELECT dayofweek('2024-01-15');   -- 返回 1 (周一)
SELECT dayofweek('2024-01-21');   -- 返回 7 (周日)
```

---

#### week(dt, mod)
**功能**：返回日期在一年中的周数。

**参数**：
- `dt`：日期时间值
- `mod`：模式（可选，默认为0）

**返回值**：整数，周数（0-53）

**SQL示例**：
```sql
SELECT week('2024-01-15');    -- 返回 2
SELECT week('2024-01-15', 1); -- 返回 3 (ISO周)
```

---

#### yearweek(dt, mod)
**功能**：返回年份和周数的组合。

**参数**：
- `dt`：日期时间值
- `mod`：模式（可选）

**返回值**：整数，年份*100 + 周数

**SQL示例**：
```sql
SELECT yearweek('2024-01-15');    -- 返回 202402
SELECT yearweek('2024-01-15', 1); -- 返回 202403
```

---

#### dayofyear(dt)
**功能**：返回日期在一年中的天数（1-366）。

**参数**：
- `dt`：日期时间值

**返回值**：整数，一年中的第几天（1-366）

**SQL示例**：
```sql
SELECT dayofyear('2024-01-01');   -- 返回 1
SELECT dayofyear('2024-02-01');   -- 返回 32
SELECT dayofyear('2024-12-31');   -- 返回 366 (闰年)
```

---

#### dayofmonth(dt)
**功能**：返回日期在当月的天数（1-31）。

**参数**：
- `dt`：日期时间值

**返回值**：整数，当月的第几天（1-31）

**SQL示例**：
```sql
SELECT dayofmonth('2024-01-15');   -- 返回 15
SELECT dayofmonth('2024-01-31');   -- 返回 31
```

---

#### year(dt)
**功能**：返回日期的年份。

**参数**：
- `dt`：日期时间值

**返回值**：整数，年份

**SQL示例**：
```sql
SELECT year('2024-01-15');   -- 返回 2024
SELECT year('1999-12-31');   -- 返回 1999
```

---

#### time_to_sec(dt)
**功能**：将时间转换为秒数。

**参数**：
- `dt`：时间值

**返回值**：整数，从00:00:00开始的秒数

**SQL示例**：
```sql
SELECT time_to_sec('01:00:00');   -- 返回 3600
SELECT time_to_sec('10:30:00');   -- 返回 37800
```

---

#### sec_to_time(t)
**功能**：将秒数转换为时间字符串。

**参数**：
- `t`：秒数

**返回值**：字符串，HH:MM:SS格式

**SQL示例**：
```sql
SELECT sec_to_time(3600);    -- 返回 '01:00:00'
SELECT sec_to_time(37800);   -- 返回 '10:30:00'
```

---

#### dateadd(dt, i) / adddate(dt, i)
**功能**：向日期添加时间间隔。

**参数**：
- `dt`：日期时间值
- `i`：间隔值（数字或间隔表达式）

**返回值**：datetime对象

**SQL示例**：
```sql
SELECT dateadd('2024-01-15', 7);   -- 返回 2024-01-22 (加7天)
SELECT adddate('2024-01-15', INTERVAL 1 MONTH);   -- 返回 2024-02-15
SELECT dateadd('2024-01-15', INTERVAL 1 YEAR);    -- 返回 2025-01-15
```

---

#### datesub(dt, i) / subdate(dt, i)
**功能**：从日期减去时间间隔。

**参数**：
- `dt`：日期时间值
- `i`：间隔值

**返回值**：datetime对象

**SQL示例**：
```sql
SELECT datesub('2024-01-15', 7);   -- 返回 2024-01-08 (减7天)
SELECT subdate('2024-01-15', INTERVAL 1 MONTH);   -- 返回 2023-12-15
```

---

#### addtime(dt, i)
**功能**：向日期时间添加时间。

**参数**：
- `dt`：日期时间值
- `i`：时间间隔

**返回值**：datetime对象

**SQL示例**：
```sql
SELECT addtime('2024-01-15 10:00:00', '02:30:00');   -- 返回 2024-01-15 12:30:00
```

---

#### subtime(dt, i)
**功能**：从日期时间减去时间。

**参数**：
- `dt`：日期时间值
- `i`：时间间隔

**返回值**：datetime对象

**SQL示例**：
```sql
SELECT subtime('2024-01-15 10:00:00', '02:30:00');   -- 返回 2024-01-15 07:30:00
```

---

#### datediff(dt1, dt2)
**功能**：计算两个日期之间的天数差。

**参数**：
- `dt1`：第一个日期
- `dt2`：第二个日期

**返回值**：整数，天数差（dt2 - dt1）

**SQL示例**：
```sql
SELECT datediff('2024-01-15', '2024-01-10');   -- 返回 5
SELECT datediff('2024-01-10', '2024-01-15');   -- 返回 -5
```

---

#### date_format(dt, f)
**功能**：按指定格式格式化日期时间。

**参数**：
- `dt`：日期时间值
- `f`：格式字符串

**返回值**：字符串

**常用格式符**：
- `%Y`：四位年份
- `%m`：月份（01-12）
- `%d`：日期（01-31）
- `%H`：小时（00-23）
- `%i`：分钟（00-59）
- `%s`：秒（00-59）

**SQL示例**：
```sql
SELECT date_format('2024-01-15 10:30:00', '%Y-%m-%d');        -- 返回 '2024-01-15'
SELECT date_format('2024-01-15 10:30:00', '%Y年%m月%d日');    -- 返回 '2024年01月15日'
```

---

#### time_format(dt, f)
**功能**：按指定格式格式化时间。

**参数**：
- `dt`：时间值
- `f`：格式字符串

**返回值**：字符串

**SQL示例**：
```sql
SELECT time_format('10:30:45', '%H:%i:%s');   -- 返回 '10:30:45'
SELECT time_format('10:30:45', '%H时%i分');   -- 返回 '10时30分'
```

---

#### weekday(dt)
**功能**：返回日期对应的星期几（0=周一，6=周日）。

**参数**：
- `dt`：日期时间值

**返回值**：整数，0-6（周一到周日）

**SQL示例**：
```sql
SELECT weekday('2024-01-15');   -- 返回 0 (周一)
SELECT weekday('2024-01-21');   -- 返回 6 (周日)
```

---

#### utc_date()
**功能**：返回当前UTC日期。

**参数**：无

**返回值**：date对象，UTC日期

**SQL示例**：
```sql
SELECT utc_date();   -- 返回当前UTC日期
```

---

#### utc_time()
**功能**：返回当前UTC时间。

**参数**：无

**返回值**：time对象，UTC时间

**SQL示例**：
```sql
SELECT utc_time();   -- 返回当前UTC时间
```

---

#### utc_timestamp()
**功能**：返回当前UTC日期时间。

**参数**：无

**返回值**：datetime对象，UTC日期时间

**SQL示例**：
```sql
SELECT utc_timestamp();   -- 返回当前UTC时间
```

---

### JSON函数

#### json_contains(target, candidate, path)
**功能**：检查JSON文档中指定路径是否包含候选值。

**参数**：
- `target`：目标JSON文档
- `candidate`：候选值
- `path`：JSON路径

**返回值**：整数，1表示包含，0表示不包含

**SQL示例**：
```sql
SELECT json_contains('{"a": 1, "b": 2}', '1', '$.a');    -- 返回 1
SELECT json_contains('{"a": [1, 2, 3]}', '2', '$.a');    -- 返回 0 (需要精确匹配)
```

---

#### json_contains_path(json_doc, one_or_all, ...)
**功能**：检查JSON文档中是否存在指定的路径。

**参数**：
- `json_doc`：JSON文档
- `one_or_all`：'one'表示任一路径存在即可，'all'表示所有路径都必须存在
- `...`：路径列表

**返回值**：整数，1表示存在，0表示不存在

**SQL示例**：
```sql
SELECT json_contains_path('{"a": 1, "b": 2}', 'one', '$.a', '$.c');   -- 返回 1
SELECT json_contains_path('{"a": 1, "b": 2}', 'all', '$.a', '$.c');   -- 返回 0
```

---

#### json_extract(json_doc, ...)
**功能**：从JSON文档中提取指定路径的值。

**参数**：
- `json_doc`：JSON文档
- `...`：路径列表

**返回值**：提取的值或值列表

**SQL示例**：
```sql
SELECT json_extract('{"a": 1, "b": 2}', '$.a');             -- 返回 1
SELECT json_extract('{"a": 1, "b": 2}', '$.a', '$.b');      -- 返回 [1, 2]
SELECT json_extract('{"items": [{"id": 1}, {"id": 2}]}', '$.items[0].id');  -- 返回 1
```

---

#### json_depth(json_doc)
**功能**：返回JSON文档的最大深度。

**参数**：
- `json_doc`：JSON文档

**返回值**：整数，深度值

**SQL示例**：
```sql
SELECT json_depth('{}');                  -- 返回 1
SELECT json_depth('{"a": {"b": 1}}');     -- 返回 3
SELECT json_depth('[1, [2, [3]]]');       -- 返回 4
```

---

#### json_keys(json_doc, path)
**功能**：返回JSON对象中指定路径的所有键。

**参数**：
- `json_doc`：JSON文档
- `path`：JSON路径（可选）

**返回值**：列表，键名数组

**SQL示例**：
```sql
SELECT json_keys('{"a": 1, "b": 2}');               -- 返回 ['a', 'b']
SELECT json_keys('{"a": {"x": 1, "y": 2}}', '$.a'); -- 返回 ['x', 'y']
```

---

#### json_length(json_doc, path)
**功能**：返回JSON文档或数组中指定路径的长度。

**参数**：
- `json_doc`：JSON文档
- `path`：JSON路径（可选）

**返回值**：整数，长度

**SQL示例**：
```sql
SELECT json_length('{"a": 1, "b": 2}');     -- 返回 2
SELECT json_length('[1, 2, 3, 4]');          -- 返回 4
SELECT json_length('{"a": [1,2,3]}', '$.a'); -- 返回 3
```

---

#### json_valid(val)
**功能**：判断值是否为有效的JSON。

**参数**：
- `val`：要检查的值

**返回值**：整数，1表示有效JSON，0表示无效

**SQL示例**：
```sql
SELECT json_valid('{"a": 1}');    -- 返回 1
SELECT json_valid('[1, 2, 3]');   -- 返回 1
SELECT json_valid('invalid');     -- 返回 0
```

---

## 聚合函数

- count([distinct] expr)
- sum(expr)
- max(expr)
- min(expr)
- avg(expr)
- group_concat(expr)
- group_array(expr)
- group_uniq_array(expr)
- group_bit_and(expr)
- group_bit_or(expr)
- group_bit_xor(expr)

## 窗口函数

- row_number(expr)
- rank(expr)
- dense_rank(expr)
- percent_rank(expr)
- cume_dist(expr)

## YIELD函数

- yield_array(values)

## transform转换函数

- transform$v4h(key, vkey)
- transform$h4v(key, vkey)
- transform$v2h(key, vkey, [value])
- transform$h2v(key, vkey, [value])
- transform$uniqkv(key, vkey, [value])

## 时间窗口函数

- time_window(time_period, [dt, [offset]])