# 自定义 Python 函数

## 何时加载本章

在 SQL 中需要调用自己的 Python 函数、标准库或已安装模块时加载本章。按需求选择最小入口：

| 需求 | 入口 |
| --- | --- |
| 仅在一段 SQL 中调用模块函数 | `USE` 导入模块 |
| 每次启动都注册一个无前缀的 SQL 函数 | 配置 `extensions`，模块导入时注册计算器 |
| 在 Python 调用方预先绑定模块或函数 | `ScriptEngine.use()` 或 `engine.context().use()` |
| 临时计算一个简短、完全受信任的表达式 | `PYEVAL` |
| 对查询行集合写一个 Python 表达式 | `PYEVALT` |

不要把本章当作 Syncany 底层扩展 API 的完整参考。这里说明的是 Syncany-SQL 当前源码明确暴露或加载的路径。

## 先用 `USE` 导入模块

`USE` 的实现会先执行 Python 的 `__import__`，再将模块名写入当前会话的 `imports`。别名存在时使用别名，没有别名时取模块路径最后一段。因此模块必须已经位于 Python 的可导入路径中。SQL 示例使用反引号包住模块文本。

```sql
USE `utils`;

SELECT utils$add_number(1, 2) AS total;
```

对应的最小模块：

```python
# utils.py
def add_number(left, right):
    return left + right
```

这会调用 `utils.add_number(1, 2)`。嵌套属性以多个 `$` 表示，例如仓库示例中的 `python_datetime$datetime$now()`。别名也可减少 SQL 中的前缀：

```sql
USE `datetime as python_datetime`;
SELECT python_datetime$datetime$now();
```

`USE` 的作用域是当前会话。可用 `SHOW IMPORTS` 查看会话记录的别名和模块路径。

### 在配置或 Python 中预设导入

若不想在每个 SQL 文件中写 `USE`，配置中的 `imports` 是别名到模块路径的映射：

```yaml
imports:
  math: math
```

也可以从 Python 绑定。`ScriptEngine.use(name, func_or_module)` 与 `engine.context().use(name, func_or_module)` 都把值写入会话的 `imports`，不会替你验证路径或导入模块。

```python
from syncanysql import ScriptEngine
import math

with ScriptEngine() as engine:
    engine.use("math", math)
    engine.execute("SELECT math$pow(2, 3) AS value;")
```

## 注册无前缀 SQL 函数

当函数应以 `my_sum(1, 2)` 形式出现，而不是 `module$my_sum(1, 2)`，定义一个 `Calculater` 子类，在模块导入时注册它：

```python
# my_extension.py
from syncanysql import Calculater, register_calculater


@register_calculater("my_sum")
class MySumCalculater(Calculater):
    def calculate(self, left, right):
        return left + right
```

把模块加入启动配置：

```yaml
extensions:
  - my_extension
```

之后可执行：

```sql
SELECT my_sum(1, 2) AS total;
```

`GlobalConfig.load_extensions()` 只接受列表，并逐项调用 `__import__`。`ScriptEngine.setup()` 和命令行入口都会在建立任务管理器前调用它。导入失败会记为 warning，进程继续运行，因此函数缺失时应先检查启动日志和模块的 Python 导入路径。

### 本项目暴露的 API，与下层 Syncany 的边界

下面的区分避免把依赖库的能力误写成 Syncany-SQL 自己的接口。

| 名称 | 本项目中的状态 | 使用建议 |
| --- | --- | --- |
| `syncanysql.Calculater`、`syncanysql.register_calculater` | `syncanysql/__init__.py` 从 `syncany.calculaters` 导入并重新导出 | 可以按上面的扩展示例直接导入 |
| `syncanysql.calculaters.register_calculater` | 由 `syncanysql/calculaters/__init__.py` 重新导出 | 可用，但根包路径更短 |
| `StateAggregateCalculater`、`GenerateCalculater`、窗口计算器类 | 在 `syncanysql.calculaters` 导出，并有仓库示例 | 仅在确实需要聚合、生成或窗口生命周期时使用 |
| `Loader`、`Outputer`、`Valuer`、`Filter`、`DataBase` 及对应 `register_*` | 根包从 `syncany.*` 导入后重新导出 | 它们是 Syncany 的低层扩展能力，本文不定义其生命周期或签名 |
| `TransformCalculater` | 根包从 `syncany.calculaters` 重新导出 | 同样属于下层计算器体系，先以现有 transform 示例核实需求 |

注册装饰器与基类来自 Syncany 依赖，但 Syncany-SQL 确实在两个公开导入路径重新导出了它们。本文不推断其它 Syncany 装饰器、配置字段或方法签名。

## `PYEVAL` 和 `PYEVALT`

这两个函数只在没有设置环境变量 `SYNCANY_PYEVAL_DISABLED` 时注册。它们使用 Python 内建 `eval`，异常会写 warning 并返回 `NULL`，不会将 Python 异常直接作为 SQL 错误抛出。

`PYEVAL` 的第一个参数是表达式字符串，后续参数作为局部变量 `args` 传入：

```sql
SELECT PYEVAL('args[0] + args[1]', 1, 2) AS total;
```

在当前实现中，表达式可访问预置模块 `sys`、`os`、`datetime`、`time`、`math`、`random`、`string`、`uuid`、`base64`、`hashlib`、`pickle`、`json`、`re`。若环境安装了 `requests`，它也会加入。还可调用 `current_tasker()`、`current_executor()`、`current_manager()`、`current_session()` 和 `current_env_variables()`。

`PYEVALT` 用于查询数据集合。编译后的调用会把查询数据作为第一个参数传给实现，并将其绑定为 `this`，表达式作为第二个参数，余下参数仍放入 `args`。仓库已覆盖的 SQL 用法是：

```sql
SELECT PYEVALT('[{"v": item["v"] * 2} for item in this]')
FROM (
    SELECT YIELD_ARRAY(PYEVAL('list(range(2))')) AS v
);
```

不要依赖表达式内未记录的名字、语句块或沙箱行为。这里的实现调用的是 `eval`，不是受限解释器。

## 测试与排错

1. 先让 Python 函数保持纯粹，直接在 Python 单元测试中覆盖参数、空值和异常边界。
2. 为 SQL 接口再加一条最小查询，例如 `SELECT my_sum(1, 2)`，确认注册名、加载时机和 SQL 参数顺序。
3. 对 `USE` 故障，确认当前工作目录或安装环境能让 Python 导入模块，再执行 `SHOW IMPORTS` 检查别名。
4. 对 `extensions` 故障，查看 `import extension ... error ...` warning。该加载器会吞掉导入异常，所以“进程启动成功”不代表扩展已注册。
5. 对 `PYEVAL` 或 `PYEVALT` 的 `NULL`，查看 `pyeval calculater execute ... error ...` warning，并确认未设置 `SYNCANY_PYEVAL_DISABLED`。

仓库的回归用例会执行 `examples/import_python/import_python.sql` 和 `examples/pyeval/pyeval.sql`，并断言模块函数、注册函数、`PYEVAL` 参数及 `PYEVALT` 结果。这些示例适合作为本地扩展的最小对照。

## 信任边界

把 `USE`、`extensions` 和 `PYEVAL` 都视为执行本机 Python 代码的入口：

* `USE` 导入模块，模块顶层代码会在导入时执行。
* `extensions` 在引擎初始化阶段导入模块，模块顶层代码同样会执行。
* `PYEVAL` 与 `PYEVALT` 直接执行表达式，且当前全局环境包含 `os`、`sys`，并可能包含网络库 `requests`。

因此，不要让不受信任的用户提交 SQL、表达式、扩展模块名或可写入导入路径的文件。多租户或需要隔离的场景应设置环境变量 [`SYNCANY_PYEVAL_DISABLED`](#pyeval-和-pyevalt) 禁用 `PYEVAL` 和 `PYEVALT`，只部署审查过的扩展，并在受限进程、容器或操作系统账户中运行 Syncany-SQL。

## 证据与继续阅读

* [`examples/import_python/import_python.sql`](../../../examples/import_python/import_python.sql)，`USE`、别名和 `$` 调用语法。
* [`examples/import_python/utils.py`](../../../examples/import_python/utils.py)，最小模块函数。
* [`examples/import_python/config.yaml`](../../../examples/import_python/config.yaml) 与 [`syncany_ext.py`](../../../examples/import_python/syncany_ext.py)，扩展加载和注册示例。
* [`syncanysql/taskers/use.py`](../../../syncanysql/taskers/use.py)，`USE` 的真实导入及会话映射行为。
* [`syncanysql/config.py`](../../../syncanysql/config.py)，`extensions` 的列表加载逻辑。
* [`syncanysql/__init__.py`](../../../syncanysql/__init__.py) 与 [`syncanysql/calculaters/__init__.py`](../../../syncanysql/calculaters/__init__.py)，公开重导出与 `PYEVAL` 的条件注册。
* [`syncanysql/calculaters/pyeval_calculater.py`](../../../syncanysql/calculaters/pyeval_calculater.py)，表达式上下文、返回 `NULL` 的异常处理和 `PYEVALT` 参数绑定。
* [`tests/test_example_import_python.py`](../../../tests/test_example_import_python.py) 与 [`tests/test_example_pyeval.py`](../../../tests/test_example_pyeval.py)，仓库实际断言的行为。
