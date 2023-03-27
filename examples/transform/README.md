
# transform 对查询结果执行变换

### SQL语法规则

- from子查询且无别名
- 不包含任何where、group、having、limit语句
- select只查询一个值且是函数，同时为设置别名

满足以上条件的SQL语句会被编译为transform

### transform函数定义

- 第一个参数子查询结果数组
- 其他为常量参数
- 返回值为结果数组

使用是不写第一个子查询数组参数，执行时会自动传递该参数