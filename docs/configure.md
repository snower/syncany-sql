# 配置示例

```yaml
# syncany-sql配置文件定义全局日志、时区、编码格式及数据库参数定义和全局导入包信息
# 支持json和yaml格式配置文件
# 默认加载当前目录的"./config.[json|yaml]”和当前用户目录下"~/.syncany/config.[json|yaml]"文件
# 当前目录配置文件优先级高于用户目录，合并配置项后为最终加载配置

# 日志文件地址，未配置、配置空字符串或'-'都为标准输出
logfile: '-'
#  日志格式，请参照Python标准库logging配置
logformat: ''
# 日志输出级别 CRITICAL ERROR WARNING INFO DEBUG，默认INFO
loglevel: 'INFO'

# 文件编码，默认utf-8
encoding: 'utf-8'
# 默认日期时间格式化格式，请参照Python标准库datetime设置日期时间输出格式
datetime_format: '%Y-%m-%d %H:%M:%S'
# 默认日期格式化格式，请参照Python标准库datetime设置日期输出格式
date_format: '%Y-%m-%d'
# 默认时间格式化格式，请参照Python标准库datetime设置时间输出格式
time_format: '%H:%M:%S'

# 数据库配置信息
databases:
  - name: mysql_example # MySQL 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/PyMySQL/PyMySQL 配置
    driver: mysql
    host: '127.0.0.1'
    port: 3306
    user: 'root'
    passwd: '123456'
    db: 'example'
    charset: 'utf8mb4'

  - name: mongo_example # MongoDB 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/mongodb/mongo-python-driver 配置
    driver: mongo
    host: "127.0.0.1"
    port: 27017
    username: 'admin'
    password: '123456'
    authSource: 'admin'
    db: 'example'

  - name: postgresql_example # PostgreSQL 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/psycopg/psycopg2 配置
    driver: postgresql
    host: "127.0.0.1"
    port: 5432
    username: 'user'
    password: '123456'
    dbname: 'example'

  - name: sqlserver_example # Microsoft SQL Server 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/pymssql/pymssql 配置
    driver: sqlserver
    host: '127.0.0.1'
    port: 1433
    user: 'sa'
    password: '123456'
    database: 'example'
    charset: 'utf8'

  - name: clickhouse_example # ClickHouse 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/mymarilyn/clickhouse-driver 配置
    driver: clickhouse
    host: "127.0.0.1"
    port: 9000
    username: 'default'
    password: '123456'
    database: 'example'

  - name: influxdb_example # InfluxDB 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/influxdata/influxdb-python 配置
    driver: influxdb
    host: "127.0.0.1"
    port: 8086
    username: 'root'
    password: '123456'
    database: 'example'

  - name: elasticsearch_example # Elasticsearch 示例参数，除name和driver是专有参数，其余连接参数可参照 https://github.com/elastic/elasticsearch-py 配置
    driver: elasticsearch
    hosts: "http://localhost:9200"
    
  - name: sqlite_example # SQLite 示例参数，除name和driver是专有参数，其余连接参数可参照 https://docs.python.org/3/library/sqlite3.html 配置
    driver: sqlite
    database: ':memory:'

# 配置全局导入包，也可在SQL中用"use"指令导入
#imports:
#  np: numpy # 导入numpy到别名numpy，可在SQL中"np$array()"使用

# 执行初始化SQL脚本
#executes:
#  - init.sql # 执行当前目录下init.sql初始化脚本，如果该脚本在用户目录下则可用“${HOME}/init.sql”，如在syncany配置目录下则可为“${SYNCANY_HOME}/init.sql”
```