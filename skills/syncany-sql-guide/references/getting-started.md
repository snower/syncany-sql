# 开始使用 Syncany-SQL

**何时加载：**需要判断项目用途、安装、第一次运行 SQL 文件、从标准输入执行 SQL、进入交互界面，或在 Python 中调用 `ScriptEngine` 时阅读本章。配置和数据源见[配置、数据源与运行时状态](configuration-and-sources.md)，SQL 结构见[SQL 语言参考](sql-language.md)。

## 1. 项目用途与适用场景

Syncany-SQL 是一个简单易用的 SQL 执行引擎。本地执行 MySQL 风格语法结构的 SQL，可读取常见数据库和 Excel、CSV、JSON、普通文本文件；本地还能完成跨源 `JOIN`、分组聚合和排序。仓库提供了查询 Nginx 日志、查询 JSON 文件、临时内存结果、聚合和跨源连接等示例。

SQL 支持由当前实现定义，是 **MySQL 风格子集**，不是完整 MySQL 兼容层。写查询前请看[SQL 语言参考](sql-language.md)，只使用其中有证据的结构和函数。

## 2. 安装

项目 README 给出的安装命令是：

```bash
pip3 install syncanysql
```

安装包注册的命令名是 `syncany-sql`。部分外部数据库驱动是可选依赖，先在[配置、数据源与运行时状态](configuration-and-sources.md)确认目标 driver 及其依赖，别假定基础安装已包含全部连接驱动。

## 3. 三个命令行入口

### 执行文件

命令行接受 `.sql`、`.sqlx` 或 `.prql` 文件。仓库 demo 的已验证运行方式如下，先进入示例目录，使 SQL 中的相对 `data/` 路径可被找到：

```bash
cd examples/demo
syncany-sql demo.sql
```

`examples/demo/demo.sql` 查询同目录的 JSON 数据，并使用 `JOIN`、`GROUP BY`、`IF`、`MAX` 和 `YIELD_ARRAY`。对应的 `tests/test_example_demo.py` 会加载此文件并断言结果，因此它适合作为首次文件执行的可追溯样例。

### 从标准输入执行

当标准输入不是终端，且首个参数不是 `.sql`、`.sqlx` 或 `.prql` 文件时，CLI 会进入读取标准输入 SQL 的分支。不过当前无参数管道调用存在已验证的缺陷：该分支在执行 SQL 前访问缺失的 `sys.argv[1]`，因此会抛出 `IndexError`。

```bash
echo "SELECT 1;" | syncany-sql
```

上述命令当前不能正常执行，不能作为可用的标准输入入口推荐。在缺陷修复前，请改用 SQL 文件入口，或使用下面的 Python `ScriptEngine` 入口；本章不提供未经验证的命令行替代方案。

### 交互式使用

不提供文件参数并从终端启动时，CLI 会创建并运行 `CliPrompt`：

```bash
syncany-sql
```

本章只确认该交互入口存在。提示符命令、补全和输出格式不在这里推断，需要时以 `syncanysql/prompt.py` 的实现为准。

## 4. Python 入口

README 提供 `ScriptEngine` 作为 Python API。下面保留其已给出的调用模式，并读取内存表 `top_ips`：

```python
from syncanysql import ScriptEngine

with ScriptEngine() as engine:
    engine.execute('''
        INSERT INTO `top_ips` SELECT
            ip, cnt
        FROM
            (SELECT
                seg0 AS ip, COUNT(*) AS cnt
            FROM
                `file:///var/log/nginx/access.log?sep= `
            GROUP BY seg0) a
        ORDER BY cnt DESC
        LIMIT 3;
    ''')
    print(engine.pop_memory_datas("top_ips"))
```

`ScriptEngine.execute(sql)` 会在尚未初始化时先完成设置，然后解析并执行 SQL。`ScriptEngine` 支持上下文管理器。Python 绑定模块或函数、自定义计算和 `PYEVAL` 相关内容见[自定义 Python 函数](python-extensions.md)。

## 5. 最小首次运行流程

1. 安装 `syncanysql`。
2. 进入 `examples/demo`。
3. 运行 `syncany-sql demo.sql`。
4. 如需理解该示例中的 SQL，再加载[SQL 语言参考](sql-language.md)。
5. 如需连接自己的数据库或配置日志、变量、扩展，再加载[配置、数据源与运行时状态](configuration-and-sources.md)。

## 限制与证据

- CLI 对交互式文件参数只接受 `.sql`、`.sqlx`、`.prql`，其他扩展名会报错。
- 相对文件路径依赖当前工作目录，demo SQL 使用 `data/demo.json`、`data/sites.json`、`data/orders.json`。
- 不要把 README 的“MySQL 语法结构”表述扩展为完整 MySQL 兼容，具体范围始终以[SQL 语言参考](sql-language.md)为准。

本章事实来源：[README](../../../README.md)、[打包入口](../../../setup.py)、[CLI 实现](../../../syncanysql/main.py)、[`ScriptEngine` 实现](../../../syncanysql/__init__.py)、[demo SQL](../../../examples/demo/demo.sql)、[demo 测试](../../../tests/test_example_demo.py)。

## 五章边界

按问题只加载一章，跨边界时再继续：

1. [开始使用](getting-started.md)：安装与各入口。
2. [配置、数据源与运行时状态](configuration-and-sources.md)：配置文件、连接和运行时设置。
3. [SQL 语言参考](sql-language.md)：已核实的 SQL 子集与执行限制。
4. [内置函数参考](built-in-functions.md)：函数名称与行为证据。
5. [自定义 Python 函数](python-extensions.md)：模块导入、扩展和 Python 执行风险。
