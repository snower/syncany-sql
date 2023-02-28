# 支持的特性

- Join关联查询
- Having支持复杂条件过滤
- Group By分组聚合计算
- Order By排序
- 子查询
- Where或Having条件支持简单子查询
- Insert Into除支持正常写入数据库表外可直接写入到execl、json和csv中

# 使用限制

- Where条件仅可使用==、>、>=、<、<=、!=、in简单条件且仅可用and
- Join仅支持Left Join模式且关联条件仅被关联表能添加常量条件及主表可添加计算条件值
- Join查询及子查询各表必须有别名