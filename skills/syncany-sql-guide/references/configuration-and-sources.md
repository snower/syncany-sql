# 配置、数据源与运行时状态

> **何时加载：**需要找 `config.yaml`、连接已配置数据库、导入可由 Python 导入的模块、设置 `@` 变量，或判断扩展是否会生效时加载本章。SQL 语法范围见 [SQL 语言](sql-language.md)；编写或审查 Python 扩展见 [Python 扩展](python-extensions.md)。

## 1. 配置文件从哪里来

`syncany-sql` 接受 JSON 或 YAML 配置。默认候选文件为当前工作目录与 `SYNCANY_HOME`（未设置时为 `~/.syncany`）下的 `config.json`、`config.yaml`；配置目录也会加入 Python 的模块搜索路径。候选文件间的冲突优先级不可靠，因为当前实现先将候选文件放入集合；把同一设置只放在一个位置最稳妥。

这是**文档约定而非可依赖的冲突解决 API**：当前实现把四个候选文件先放入集合再加入 `extends`，集合遍历本身不表达稳定顺序。因此不要让同一个键同时出现在这些默认文件中来赌覆盖次序。`extends` 中的文件会先递归加载，随后当前文件合并；Python `ScriptEngine(custom_config=...)` 提供的字典在文件加载后再合并。

最小项目配置可以从仓库根目录的 [`config.yaml.example`](../../../config.yaml.example) 复制为工作目录的 `config.yaml`。不要提交真实口令。

```yaml
logfile: '-'
loglevel: INFO
encoding: utf-8

databases:
  - name: mysql_example
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    passwd: '***'
    db: example
```

示例文件明确给出的全局键为 `logfile`、`logformat`、`loglevel`、`encoding`、`datetime_format`、`date_format`、`time_format`、`databases` 与（注释掉的）`extensions`。`logfile` 未设、为空或为 `-` 时输出到标准输出；`loglevel` 的示例值为 `CRITICAL`、`ERROR`、`WARNING`、`INFO`、`DEBUG`。实现还会读取 `timezone`、`pypackage_paths`、`executes` 和 `extends`；它们不是示例中的通用连接参数，应按本章的边界使用。

## 2. 合并规则与 `config.yaml` 的边界

合并按键类型进行，而不是简单地整份覆盖：

| 配置类别 | 合并方式 |
| --- | --- |
| `imports`、`defines`、`variables`、`sources`、`options`（以及 `arguments`、`logger`） | 字典按键更新；后加载的同名项替换前项。 |
| `databases`、`caches` | 列表按每项的 `name` 合并；同名数据库仅更新给出的字段。 |
| `states`、`executes` | 追加为列表。 |
| 其他键 | 后加载值整体替换。 |

启动后，运行时还保证两个内部数据库名：`-` 使用 `textline`，`--` 使用 `memory`；它们不是必须写进你的 YAML 的连接配置。数据库项的 `name` 和 `driver` 是 syncany-sql 所需的标识；其余连接字段由对应 driver 决定。仅使用示例中有依据的 driver/字段组合（MySQL、MongoDB、PostgreSQL、SQL Server、ClickHouse、InfluxDB、Elasticsearch、SQLite；完整清单以 [`config.yaml.example`](../../../config.yaml.example) 和 [`docs/configure.md`](../../../docs/configure.md) 为准）。驱动依赖并非默认全部安装，参见 [`docs/使用教程/2、安装和配置.md`](../../../docs/使用教程/2、安装和配置.md)。

`executes` 是初始化 SQL 文件列表：文件先按给定路径查找，再查 `SYNCANY_HOME`；找不到会使启动失败。仓库的 [`examples/import_python/config.yaml`](../../../examples/import_python/config.yaml) 给出了 `extensions`、`imports`、`executes` 的组合实例。

## 3. 数据库、source 与 import 不是一回事

- **`databases`**：具名 driver 连接定义。查询实际取得数据库时按 `name` 查找并用该项的 `driver` 创建连接；因此 SQL 中引用的数据库名必须与配置的 `name` 对应。
- **`imports`**：别名到 Python 模块/对象名称的映射；示例配置把 `math` 映射到 `math`。这是供 SQL 调用导入对象的名称表，不是数据库连接。
- **`sources`**：别名到源路径的映射。`USE` 会先按 Python 名称导入目标，导入成功后才按目标是否为本机已有路径，分别写入 `sources` 或 `imports`。因此 `sources` 的登记条件不让 `USE` 导入任意本地路径。本仓库没有把任意 `sources` YAML 结构列为公开配置示例，故不要把底层 Syncany 的 source/loader 配置逐项当作 syncany-sql 已文档化功能。

这一区分也适用于 Python 代码：底层 Syncany 确实提供 `register_database`、`register_loader`、`register_outputer`、`register_valuer`、`register_filter`、`register_calculater` 等注册 API，但 syncany-sql 的 `config.yaml` 只会读取 `extensions` 列表并 `import` 其中模块；它不会根据 YAML 自动调用这些注册函数。若扩展模块自行注册了能力，才可能在导入后可用。注册方式与风险留给 [Python 扩展](python-extensions.md)。

## 4. 变量与 `SET`

SQL 环境变量以 `@` 开头，默认属于当前执行器/上下文。仓库示例可作为最小写法：

```sql
set @aaa = 1;
set @bbb = @aaa + 1;
select @aaa, @bbb;
```

`SET` 会识别带 `@` 的普通变量；`set global @aaa=1;` 会写到全局环境变量，测试覆盖了该形式。变量也可由 `SELECT ... INTO @name` 赋值；参见 [`examples/parameter_variable/parameter_variable.sql`](../../../examples/parameter_variable/parameter_variable.sql)。任务器直接接受带引号的字符串、`true`/`false`、`null`、整数和小数；示例中的 `@aaa + 1` 由 SQL 编译阶段处理。不要把 `SET` 的右侧误当作任意 YAML 或 Python 文字量。

`SET` 还接受配置命名空间 `@databases`、`@imports`、`@sources`、`@defines`、`@variables`、`@options`、`@caches`，`@virtual_views`（映射到数据库的 `virtual_views`）和 `@config`；前缀 `global` 让对应修改进入全局配置。它是会话配置修改接口，不是 `config.yaml` 的落盘工具。尤其对整个 `@databases` 项赋值时，底层 setter 会先清空该项，再解析 JSON 并更新它。JSON 解析异常会被吞掉，不会回滚已清空的配置；不要用它替代经过审查的 YAML 数据库连接定义。

## 5. `USE` 的实际行为

`USE` 会先导入目标 Python 名称，成功后才登记别名：

```sql
use `datetime as python_datetime`;
use `utils`;
```

若不写 `as`，别名取目标最后一个 `.` 分段。目标必须先能按 Python 名称在当前 `sys.path` 中导入，不能借由本机路径存在而导入任意本地文件。导入成功后，目标为本机已有路径时登记为 `sources`，否则登记为 `imports`。例如 [`examples/import_python/import_python.sql`](../../../examples/import_python/import_python.sql) 使用 `use \`datetime as python_datetime\`;` 后以 `python_datetime$datetime$now()` 调用。不要把它当作选择已配置数据库的 SQL 方言命令。

## 6. 扩展：启用，不等于配置能力

在配置中仅能声明要导入的模块名：

```yaml
extensions:
  - syncany_ext
```

引擎完成配置加载和日志设置后才逐项导入 `extensions`。导入异常会记录 warning 并继续；因此“配置了扩展”不等于扩展已成功注册或其 driver 已安装。扩展的依赖、导入路径、注册以及不可信代码边界见 [Python 扩展](python-extensions.md)；本章不把 Syncany 的底层注册 API 扩写成 syncany-sql 的 YAML 键。

## 本地证据

- [`config.yaml.example`](../../../config.yaml.example)：公开示例键、driver 样例与 `extensions` 注释。
- [`docs/configure.md`](../../../docs/configure.md)：配置示例、`imports` 与 `executes` 的用户说明。
- [`docs/使用教程/2、安装和配置.md`](../../../docs/使用教程/2、安装和配置.md)：默认发现位置、文档化优先级与 driver 依赖说明。
- [`syncanysql/config.py`](../../../syncanysql/config.py)：发现、递归 `extends`、合并、初始化 SQL、扩展导入和会话配置实现。
- [`syncanysql/taskers/set.py`](../../../syncanysql/taskers/set.py)、[`syncanysql/taskers/use.py`](../../../syncanysql/taskers/use.py)、[`syncanysql/compiler.py`](../../../syncanysql/compiler.py)：`SET`/`USE` 的解析与修改范围。
- [`tests/test_script_engine.py`](../../../tests/test_script_engine.py)、[`examples/parameter_variable/parameter_variable.sql`](../../../examples/parameter_variable/parameter_variable.sql)、[`examples/import_python/`](../../../examples/import_python/)：变量、导入与初始化扩展示例。
- 相邻 Syncany 实现：[`../syncany/syncany/database/__init__.py`](../../../../syncany/syncany/database/__init__.py)、[`../syncany/syncany/loaders/__init__.py`](../../../../syncany/syncany/loaders/__init__.py)、[`../syncany/syncany/calculaters/__init__.py`](../../../../syncany/syncany/calculaters/__init__.py)，用于区分底层注册 API 与本项目的 YAML 加载行为。
