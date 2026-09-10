---
name: syncany-sql-guide
description: Use when users need Chinese guidance for installing, configuring, querying, using functions, or extending Syncany-SQL with Python.
---

# Syncany-SQL 使用指南

## 概览

本技能面向 Syncany-SQL 使用者。参考资料随技能包安装，只在需要时加载对应文件。

Syncany-SQL 支持的是 MySQL 风格的 SQL 子集，不应视为完整 MySQL 兼容实现。

## 按问题选择参考

每类问题先加载一份对应的本地参考。仅当问题确实跨越主题时，再加载另一份。

| 用户问题 | 首先加载 |
| --- | --- |
| 安装、命令行、SQL 文件执行或 Python API 入门 | [开始使用](references/getting-started.md) |
| 配置、数据源、变量、`USE`、`SET` 或启用扩展 | [配置与数据源](references/configuration-and-sources.md) |
| JOIN、聚合、窗口函数、DML 或其他 SQL 写法 | [SQL 语言](references/sql-language.md) |
| 内置函数、函数签名、空值行为或方言差异 | [内置函数](references/built-in-functions.md) |
| Python 模块、注册函数、`PYEVAL`、`PYEVALT` 或相关风险 | [Python 扩展](references/python-extensions.md) |

## 安全边界

Python 导入、`PYEVAL` 和 `PYEVALT` 会执行 Python 代码。不要将它们用于不可信输入。
