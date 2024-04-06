## 项目名称
`slowquery`插件是一款为PostgreSQL设计的监控工具，专注于自动识别并记录执行时间超过预设阈值的SQL查询。通过GUC参数配置，它能够灵活地适应不同的性能监控需求。插件利用PostgreSQL的钩子机制和内存管理，准确追踪查询执行时间，并在超出设定阈值时输出详细的日志信息，辅助数据库管理员进行性能分析和优化。

## 运行条件
将本项目移动至 `${SOURCECODE_PATH}/contrib` 目录，并在该目录下运行 `make -sj && make install` 来安装该插件。

## 运行说明
### 安装

1. 编译插件（如果使用`pgxs`，则运行`make`命令）。
2. 连接到PostgreSQL数据库并运行`CREATE EXTENSION slowquery;`来安装插件。

### 配置

1. 设置慢查询的阈值，例如在`postgresql.conf`中添加`slowquery.min_value = 10000000;`。
2. 重新加载配置或重启数据库以应用更改。

### 使用

插件将自动记录执行时间超过设定阈值的查询，并在PostgreSQL日志中记录详细信息。

### 查看日志

检查PostgreSQL服务器的日志文件以查看慢查询记录。

### 卸载

通过运行`DROP EXTENSION slowquery;`来卸载插件。



## 测试说明
### 测试步骤

1. 连接到PostgreSQL数据库。

2. 执行一个预计执行时间较短的查询，例如：

   ```
   SELECT COUNT(*) FROM users;
   ```

   这条查询应该迅速执行完成，不会触发慢查询日志记录。

3. 执行一个预计执行时间较长的查询，例如一个全表扫描或复杂的JOIN操作：

   ```
   SELECT * FROM large_table WHERE some_column IN (SELECT some_id FROM another_table);
   ```

   如果查询执行时间超过10秒，它应该被`slowquery`插件捕获并记录。

4. 对于第三步中的慢查询，可以使用`pg_sleep`函数来模拟一个长时间的执行，以确保查询确实会超过设定的阈值：

   ```
   SELECT * FROM large_table WHERE some_column IN (SELECT some_id FROM another_table ORDER BY (SELECT pg_sleep(5)));
   ```

   这条查询通过`pg_sleep(5)`模拟了一个5秒的延迟，确保查询执行时间超过10秒。

5. 完成所有查询后，检查PostgreSQL的日志文件，查找慢查询记录。

### 预期结果

- 执行时间较短的查询（步骤2）不会记录在慢查询日志中。
- 执行时间较长的查询（步骤3和4）将会被记录在慢查询日志中，记录应包含查询的文本和执行时间。

### 验证方法

在PostgreSQL日志文件中搜索慢查询记录，例如：

```
LOG: 2023-04-06 10:00:00.000 UTC [慢查询]  SELECT * FROM large_table WHERE some_column IN (SELECT some_id FROM another_table ORDER BY (SELECT pg_sleep(5)));  10s
```

## 技术架构
`slowquery`插件通过GUC配置、执行时钩子、内存上下文和日志记录来监控并记录超过指定阈值的慢查询。


## 协作者
> 无