# 配置、数据源与运行时状态

本章说明如何为 `syncany-sql` 配置数据库、Python 导入和扩展，以及如何在当前会话中用 `SET`、`USE` 调整状态。SQL 查询与表达式写法见[SQL 语言](sql-language.md)，Python 扩展的编写方式见[Python 扩展](python-extensions.md)。

## 配置文件

`syncany-sql` 接受 YAML 或 JSON 配置。它会查找以下默认位置中的 `config.yaml` 和 `config.json`：

- 当前工作目录
- `SYNCANY_HOME` 指定的目录
- 未设置 `SYNCANY_HOME` 时，`~/.syncany`

不要在多个默认配置文件中定义同一个设置并依赖覆盖顺序。默认候选文件的冲突顺序不是稳定的配置接口，版本、环境或运行细节都可能让结果不同。一个设置只放在一个默认配置文件中。

如果需要拆分配置，可用 `extends`。被扩展的文件会先加载，再合并当前文件。例如：

```yaml
# config.yaml
extends:
  - common.yaml

loglevel: INFO
```

常用全局项如下：

```yaml
logfile: '-'
loglevel: INFO
encoding: utf-8
timezone: Asia/Shanghai
datetime_format: '%Y-%m-%d %H:%M:%S'
date_format: '%Y-%m-%d'
time_format: '%H:%M:%S'
```

`logfile` 未设置、为空或为 `-` 时，日志写到标准输出。常用 `loglevel` 为 `CRITICAL`、`ERROR`、`WARNING`、`INFO` 和 `DEBUG`。

配置合并时，`imports`、`defines`、`variables`、`sources`、`options` 等映射按键合并。`databases` 和 `caches` 按各项的 `name` 合并，同名项只更新给出的字段。`states` 和 `executes` 会追加。其他顶层项由后加载的值替换。

## 数据库连接

在 `databases` 中为每个连接设置唯一的 `name` 与 `driver`。SQL 中使用的数据库名应与 `name` 一致，其余字段取决于所选 driver。

```yaml
databases:
  - name: orders_mysql
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: report_user
    passwd: '${MYSQL_PASSWORD}'
    db: orders

  - name: local_cache
    driver: sqlite
    database: ./cache.db
```

将口令放在部署环境提供的配置中，避免把真实凭据提交到代码库。所用 driver 及其 Python 依赖必须已安装，否则连接不能建立。

运行环境自带两个特殊数据库名：`-` 对应文本行数据源，`--` 对应内存数据源。通常不必在配置中再次声明它们。

`executes` 可列出启动时运行的 SQL 文件：

```yaml
executes:
  - bootstrap.sql
```

文件必须可找到，否则启动会失败。把初始化文件与配置放在清晰、固定的位置，避免依赖不明确的工作目录。

## 导入、sources 与变量初值

`databases`、`imports` 与 `sources` 用途不同：

- `databases` 定义具名数据库连接。
- `imports` 将别名映射到可由 Python 导入的模块或对象名称，供 SQL 调用。
- `sources` 保存已登记的数据源路径或别名，不等同于数据库连接。

例如，配置 Python 标准库模块和变量初值：

```yaml
imports:
  math: math
  python_datetime: datetime

variables:
  report_limit: 100
  region: cn
```

配置中的变量可作为运行开始时的默认值。运行期赋值属于当前会话，下一次新建会话时应重新从配置或启动代码提供所需值。

## 用 `SET` 修改当前会话

SQL 环境变量以 `@` 开头。可以引用已赋值变量，也可以用查询结果赋值：

```sql
SET @limit = 100;
SET @next_limit = @limit + 50;
SET @enabled = true;
SET @note = 'monthly report';
SET @empty_value = null;

SELECT @limit, @next_limit, @enabled, @note;
SELECT count(*) INTO @row_count FROM orders_mysql.orders;
```

`SET` 可处理带引号字符串、`true`、`false`、`null`、整数和小数。把右侧写成 SQL 可识别的值或表达式，不要把它当成任意 YAML 或 Python 字面量。

可通过以下配置命名空间调整会话中的配置：

```sql
SET @variables = '{"report_limit": 200, "region": "us"}';
SET @imports = '{"math": "math"}';
SET @options = '{"batch_size": 500}';
```

可用的命名空间包括 `@databases`、`@imports`、`@sources`、`@defines`、`@variables`、`@options`、`@caches`、`@virtual_views` 和 `@config`。`SET global ...` 修改全局运行时环境，例如：

```sql
SET global @variables = '{"report_limit": 500}';
```

这些命令修改运行时状态，不会把变更写回配置文件。

> **警告：**不要把 `SET @databases = '...'` 当作安全的数据库配置编辑方式。为整个 `@databases` 赋值时，现有内容会先被清空，再解析 JSON。若 JSON 无效，解析错误不会恢复已清空的内容，当前会话的数据库配置可能因此丢失。需要改动连接定义时，优先修改并检查配置文件，或只在可重建的会话中测试。

## 用 `USE` 导入模块

`USE` 用于导入 Python 模块或可导入对象，不是切换已配置数据库的命令：

```sql
USE `datetime as python_datetime`;
USE `math`;

SELECT python_datetime$datetime$now();
SELECT math$sqrt(9);
```

未写 `as` 时，别名取目标名称最后一个 `.` 分段。目标必须能按 Python 模块名从当前 Python 搜索路径导入。`USE` 不能因为某个本地文件路径存在，就导入任意本地文件。

导入成功后，运行时会根据目标是否是本机已有路径，登记为 `sources` 或 `imports`。因此，先确保模块可被 Python 正常导入，再使用 `USE`；本地代码应安装为包、放入明确的模块搜索路径，或在启动配置中设置合适的 `pypackage_paths`。

## 扩展

在 `extensions` 中列出启动时需要导入的模块：

```yaml
extensions:
  - my_company.syncany_extensions
```

引擎会在加载配置后导入这些模块。扩展导入失败时会记录警告并继续运行，所以“已经写入 `extensions`”不表示扩展一定可用。遇到功能缺失时，检查模块是否可导入、依赖是否已安装，以及扩展自身是否完成了所需注册。

扩展代码会在你的 Python 进程中执行。只启用可信来源的扩展，并将其版本与依赖固定在可复现的部署环境中。

## 排查清单

1. 找不到配置时，确认工作目录、`SYNCANY_HOME` 与文件名。
2. 配置结果不符合预期时，移除默认位置中重复的同名设置，不要依赖冲突覆盖顺序。
3. 数据库连接失败时，核对 `name`、`driver`、连接字段、网络可达性和 driver 依赖。
4. `USE` 失败时，先在相同 Python 环境中确认目标模块可导入，不要传入任意文件路径。
5. 扩展功能未出现时，查看警告日志，并核对扩展模块及其依赖。
6. 会话中误设了整个 `@databases` 后，重新建立已知配置的会话，不要期待无效 JSON 自动恢复旧值。
