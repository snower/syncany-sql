# 开始使用 Syncany-SQL

Syncany-SQL 是面向本地数据处理和多数据源查询的 SQL 执行引擎。它适合临时分析文件数据、连接不同来源的数据、完成筛选、聚合、排序和连接查询，也可从 Python 程序执行 SQL。

它使用 MySQL 风格的 SQL 结构，但只支持其中一部分语法和函数，不承诺完整 MySQL 兼容性。编写查询前，请查阅[SQL 语言参考](sql-language.md)；函数列表见[内置函数参考](built-in-functions.md)。

## 安装

在已安装 Python 和 pip 的环境中运行：

```bash
pip install syncanysql
```

安装后，命令行程序名为 `syncany-sql`。如果要连接外部数据库，可能还需要安装相应数据库驱动，并在[配置、数据源与运行时状态](configuration-and-sources.md)中配置数据源。

可用下面的命令确认安装：

```bash
syncany-sql --help
```

## 从 SQL 文件运行

CLI 可执行 `.sql`、`.sqlx` 和 `.prql` 文件。先新建一个 `first.sql` 文件，写入以下内容：

```sql
-- 示例：计算一个表达式
SELECT 1 + 2 AS total;
```

再在该文件所在目录运行：

```bash
syncany-sql first.sql
```

这是首次使用 CLI 的推荐方式。SQL 中使用相对文件路径时，路径按当前工作目录解析。为避免找不到数据文件，建议从 SQL 文件所在目录启动命令，或改用清晰的绝对路径。

要处理 CSV、JSON、Excel、文本文件或数据库数据源，请先阅读[配置、数据源与运行时状态](configuration-and-sources.md)，然后按[SQL 语言参考](sql-language.md)中的数据源和查询语法编写文件。

## 标准输入的限制

不要把无参数的管道调用当作可用入口：

```bash
echo "SELECT 1;" | syncany-sql
```

当前版本中，这种无参数且从管道读取标准输入的调用会触发 `IndexError`，不能正常执行 SQL。请改用 SQL 文件，或使用下面的 Python API。不要依赖该限制在未来版本中的行为保持不变。

## 交互式使用

在终端中不带文件参数启动，可进入交互界面：

```bash
syncany-sql
```

交互界面适合试验短查询。需要可重复执行、需要保存查询记录，或查询包含多条语句时，优先使用 SQL 文件。

## 在 Python 中执行 SQL

`ScriptEngine` 是 Python API 的主要入口。下面的示例完全在内存中运行，把查询结果写入内存表后读回：

```python
from syncanysql import ScriptEngine

sql = """
INSERT INTO `result`
SELECT
    1 + 2 AS total,
    'hello' AS message;
"""

with ScriptEngine() as engine:
    engine.execute(sql)
    rows = engine.pop_memory_datas("result")

print(rows)
```

`ScriptEngine.execute()` 接收 SQL 字符串并执行。`pop_memory_datas("result")` 读取示例中 `result` 内存表的结果。实际程序可把 SQL 放在字符串、模板或已读取的 SQL 文件中，但应验证输入内容和结果格式。

如需调用 Python 函数、导入模块或使用 Python 表达式扩展，请阅读[自定义 Python 函数](python-extensions.md)。这类能力会执行 Python 代码，只应对可信脚本开放。

## 下一步

按当前目标继续阅读：

1. [配置、数据源与运行时状态](configuration-and-sources.md)：设置配置文件、变量、数据源和外部连接。
2. [SQL 语言参考](sql-language.md)：确认可用语句、查询结构和限制。
3. [内置函数参考](built-in-functions.md)：查找已提供函数及其行为。
4. [自定义 Python 函数](python-extensions.md)：在 SQL 中使用自定义 Python 能力。

首次处理真实数据时，建议先用一个小文件或限制结果行数验证查询，再扩展到完整数据集。
