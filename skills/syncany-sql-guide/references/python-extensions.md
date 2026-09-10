# 使用 Python 扩展 Syncany-SQL

本章说明怎样在 Syncany-SQL 中调用 Python 模块、通过配置加载扩展、从 Python 绑定对象，以及怎样安全地使用 `PYEVAL` 和 `PYEVALT`。这些能力都会执行 Python 代码，只应对受信任的 SQL、模块和运行环境开放。

## 选择入口

| 目标 | 入口 |
| --- | --- |
| 在一段 SQL 中调用已安装模块的函数 | `USE` |
| 启动时注册无前缀 SQL 函数 | 配置 `extensions` |
| 在嵌入式 Python 调用方绑定模块或函数 | `ScriptEngine.use()` 或 `engine.context().use()` |
| 计算简短且完全受信任的 Python 表达式 | `PYEVAL` |
| 用 Python 表达式处理查询行集合 | `PYEVALT` |

## 用 `USE` 导入 Python 模块

`USE` 按 Python 的普通导入规则导入模块，并将模块保存到当前 SQL 会话。模块必须已经可以被该 Python 进程导入，例如标准库模块、通过包管理器安装的包，或作为应用程序正式安装的一部分。

不要把任意文件系统路径当成模块名，也不要假设 SQL 文件所在目录会自动加入 Python 导入路径。应先安装或部署模块，再用其包名导入。

```sql
USE `math`;

SELECT math$sqrt(81) AS result;
```

SQL 中以 `$` 访问模块属性。上例等价于 Python 的 `math.sqrt(81)`。多层属性也使用多个 `$`：

```sql
USE `datetime as py_datetime`;

SELECT py_datetime$datetime$now() AS current_time;
```

`as` 为会话中的模块别名。别名有助于缩短函数名，也能避免模块名冲突。

```sql
USE `json as js`;
SELECT js$dumps('hello') AS encoded;
SHOW IMPORTS;
```

`SHOW IMPORTS` 可检查当前会话已登记的模块和别名。`USE` 只影响当前会话，新的会话需要再次导入，或改用后面的配置和 Python 绑定方式。

### `USE` 排错

若 `USE` 失败，请按以下顺序检查：

1. 使用启动 Syncany-SQL 的同一个 Python 解释器运行 `python -c "import 模块名"`，确认模块确实可导入。
2. 确认使用的是包名或模块名，不是 `.py` 文件名、相对路径或任意本地路径。
3. 导入成功后执行 `SHOW IMPORTS`，确认 SQL 中使用的别名与登记的名称一致。
4. 函数找不到时，检查 `$` 的层级是否对应 Python 属性层级，例如 `package$submodule$function`。

## 通过配置加载扩展

如果某个函数应当在每次启动时都可用，可把一个可导入的 Python 扩展模块列入配置的 `extensions`。引擎启动时会导入列表中的模块，因此模块顶层的注册代码会执行。

```yaml
extensions:
  - acme_syncany_functions
```

配置项必须是列表。每一项都必须是该运行环境能正常 `import` 的模块名。部署扩展时，将其作为已安装的 Python 包的一部分交付，而不是依赖某个任意工作目录或临时文件路径。

下面是一个最小扩展模块。安装后其导入名为 `acme_syncany_functions`：

```python
from syncanysql import Calculater, register_calculater


@register_calculater("discounted_price")
class DiscountedPriceCalculater(Calculater):
    def calculate(self, price, rate):
        return price * (1 - rate)
```

加载配置后，注册名可直接作为 SQL 函数调用：

```sql
SELECT discounted_price(100, 0.2) AS final_price;
```

扩展加载失败时，进程可能仍会继续启动，函数却不会注册。遇到“函数不存在”时，应先检查启动日志中的扩展导入警告，再用相同解释器验证 `import acme_syncany_functions`。同时确认模块顶层导入没有因缺少依赖或配置错误而抛出异常。

### 设计自定义函数

`Calculater` 的 `calculate()` 参数按 SQL 调用位置传入。函数应尽量保持无副作用，并明确处理 `NULL` 可能映射出的 Python `None`、类型转换和业务异常。把网络访问、文件写入和隐式全局状态留在函数外，便于测试、排错和权限控制。

注册名属于 SQL 名称空间。选择稳定且具有业务前缀的名称，例如 `acme_discounted_price`，避免覆盖内置函数或与其他扩展冲突。

## 在 Python 中预先绑定模块或函数

嵌入式调用 Syncany-SQL 时，可直接向会话绑定 Python 对象，无需让 SQL 自己执行导入。绑定后的名称同样通过 `$` 访问。

```python
import math

from syncanysql import ScriptEngine


with ScriptEngine() as engine:
    engine.use("math", math)
    engine.execute("SELECT math$pow(2, 8) AS value;")
```

也可以绑定单个可调用对象：

```python
from syncanysql import ScriptEngine


def normalize_name(value):
    return value.strip().title()


with ScriptEngine() as engine:
    engine.context().use("normalize_name", normalize_name)
    engine.execute("SELECT normalize_name('  ada lovelace  ') AS name;")
```

绑定 API 不会替你验证对象的行为、权限或可序列化性。调用方应在绑定前完成依赖初始化，并只绑定明确允许 SQL 调用的对象。会话结束后，绑定不会自动成为其他会话的全局能力。

## `PYEVAL` 和 `PYEVALT`

`PYEVAL` 与 `PYEVALT` 使用 Python 的 `eval` 执行表达式，不是沙箱。它们只能处理完全受信任的表达式，绝不能用于用户提交的 SQL、报表公式、租户配置或任何其他不受信任输入。

设置环境变量 `SYNCANY_PYEVAL_DISABLED` 会禁用 `PYEVAL` 和 `PYEVALT` 的注册。这是控制这两个函数可用性的开关：

```text
SYNCANY_PYEVAL_DISABLED=1
```

在启动 Syncany-SQL 的进程环境中设置该变量。设置后，任何依赖这两个函数的 SQL 都应被视为不可用，并在部署前改写或移除。

### `PYEVAL`

第一个参数是 Python 表达式字符串，后续参数以局部变量 `args` 提供：

```sql
SELECT PYEVAL('args[0] + args[1]', 20, 22) AS answer;
```

表达式可使用 Python 的求值能力和运行时提供的对象，其中可能包括 `os`、`sys`、日期时间、序列化和正则相关模块，安装了可选依赖时还可能出现其他模块。因此即使表达式看似简单，也可能读写文件、启动进程、访问网络或取得运行时上下文。

执行表达式出错时，函数会记录警告并返回 `NULL`。若结果为 `NULL`，查看运行日志中的 PYEVAL 错误，再检查表达式、参数类型和 `SYNCANY_PYEVAL_DISABLED` 是否已设置。不要依赖未说明的变量、语句块或任何隔离行为。

### `PYEVALT`

`PYEVALT` 面向查询结果集合。表达式中的 `this` 表示当前查询行集合，额外参数仍通过 `args` 访问。下例将每一行的 `amount` 翻倍：

```sql
SELECT PYEVALT('[{"amount": row["amount"] * 2} for row in this]') AS doubled_rows
FROM (
    SELECT 10 AS amount
    UNION ALL
    SELECT 25 AS amount
);
```

与 `PYEVAL` 一样，`PYEVALT` 运行任意 Python 表达式并在异常时返回 `NULL`。它不是行级隔离机制，也不能使不受信任代码变得安全。

## 安全基线

将以下入口都当作本机代码执行入口：

* `USE` 会导入模块，模块顶层代码会执行。
* `extensions` 会在引擎初始化时导入模块，模块顶层代码会执行。
* `PYEVAL` 和 `PYEVALT` 会直接求值 Python 表达式。

不要允许不受信任用户提交 SQL、PYEVAL 表达式、扩展模块名，或写入 Python 导入路径中的文件。对多租户、共享服务或需要权限隔离的部署，应设置 `SYNCANY_PYEVAL_DISABLED`，只安装经过审查的扩展，并使用权限最小化的操作系统账户、受限容器或独立进程运行 Syncany-SQL。

禁用 PYEVAL 并不会使任意 SQL 自动安全。若攻击者仍能控制 `USE` 的模块名、扩展配置或可导入文件，仍可能执行 Python 代码。安全边界应同时限制 SQL 提交者、配置修改权限、软件包来源、文件写入权限和进程权限。
