#### mssql To mysql 数据同步功能

1. lib/Config.py 配置 mssql 和 mysql 连接信息，mssql 可配置多个
2. 多个mssql 数据源自动创建到 mysql 数据库中，数据库表以 {ip}_{tableName} 方式命名
3. 本项目结合 dbRepl 和 MSSQL-TO-MYSQL 应运而生，支持自动建表、更新数据、删除多余数据

Requirements:
- Python 2.6 or higher
- MySQLdb (easy_install mysql-python)
- pyodbc (http://code.google.com/p/pyodbc/)
- FreeTDS (http://www.freetds.org/)*

For my some local env config:

> ~/.odbcinst.ini

    [FreeTDS]
    Driver=/usr/local/lib/libtdsodbc.so
    Setup=/usr/local/lib/libtdsodbc.so
    FileUsage=1
