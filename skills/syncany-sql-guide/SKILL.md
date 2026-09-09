---
name: syncany-sql-guide
description: Use when users need Chinese guidance for installing, configuring, querying, using functions, or extending Syncany-SQL with Python.
---

# Syncany-SQL 使用指南

## 概览

本指南面向 Syncany-SQL 使用者，按问题加载最小必要参考内容。先确认用户的目标，再只阅读对应章节，不要一次加载全部教程。

## 何时使用

适用于安装和首次运行、配置与数据源、SQL 写法、内置函数，以及 Python 扩展问题。

不适用于为底层 Syncany 推断 Syncany-SQL 未接入的能力，也不应用于声称完整 MySQL 兼容。

## 按意图加载参考

| 用户意图 | 先阅读的唯一参考文件 | 何时再查看其他章节 |
| --- | --- | --- |
| 安装、CLI、SQL 文件执行或 Python API 首次使用 | [references/getting-started.md](references/getting-started.md) | 用户明确需要配置、SQL 细节或扩展时 |
| 配置文件、数据源、变量、`USE`、`SET` 或扩展启用 | [references/configuration-and-sources.md](references/configuration-and-sources.md) | 需要编写查询、查函数或实现扩展时 |
| 判断 JOIN、聚合、窗口、DML 或其他 SQL 构造是否可用 | [references/sql-language.md](references/sql-language.md) | 查询具体函数或 Python 调用时 |
| 查询可用内置函数、签名、空值行为或方言差异 | [references/built-in-functions.md](references/built-in-functions.md) | 函数不足而需要自定义 Python 逻辑时 |
| 导入 Python 模块、注册函数、`PYEVAL`、`PYEVALT` 或安全风险 | [references/python-extensions.md](references/python-extensions.md) | 需要先配置扩展加载或使用 SQL 语法时 |

## 来源与准确性规则

- 以本仓库的实现、测试、示例和现有文档为准，必要时再核对相邻 Syncany 仓库的已接入实现。
- 不要把底层 Syncany 的注册 API 或功能说成 Syncany-SQL 已配置或已支持的能力。
- SQL 支持应表述为项目实现的 MySQL 风格子集。不要声称完整 MySQL 支持、所有 MySQL 函数可用，或全部 SQL 语法兼容。
- 函数、SQL 构造、数据源和扩展行为没有本地证据时，说明需要在目标版本和环境中验证，不要补充猜测。
- 涉及 Python 导入、`PYEVAL` 或 `PYEVALT` 时，明确提醒它们会执行 Python 代码，不能用于不可信输入。

## 响应流程

1. 用用户问题匹配上表中的单一意图。
2. 先检查唯一对应的参考文件，只在问题跨越边界时加载下一份必要文件。
3. 基于其中可追溯的事实回答，附上适用限制和必要的验证建议。
