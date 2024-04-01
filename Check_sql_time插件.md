# Check_sql_time插件

实现了记录SQL开始-结束的时间，如果Sql执行时间大于10s，记录查询的当前时间，SQL耗时和执行的SQL语句。

## 使用

在contrib/check_sql_time文件下使用以下命令

```
make -sj
make install
```

启动数据库后使用以下命令

```
create extension check_sql_time
load '$libdir/check_sql_time'
```

