# 使用说明：慢查询检测插件（slow_sql）

## 概述
慢查询检测插件`slow_sql`是为Opentenbase数据库设计的扩展，旨在监测并记录执行时间超过预定阈值的SQL查询。此插件通过记录过慢的查询来帮助数据库管理员（DBA）和开发人员识别潜在的性能瓶颈，进而进行针对性的优化。

## 安装
前提：确保已经安装Opentenbase，并且具有相应的构建和安装扩展的权限。

1. **编译插件**
   
   在contrib目录下运行以下命令来编译插件：
   ```sh
   make -sj
   make install
   ```

2. **在Opentenbase中启用插件**
   
   登录到目标数据库，执行以下命令启用`slow_sql`插件：
   ```sql
   CREATE EXTENSION slow_sql;
   ```

## 配置
慢查询检测插件通过SQL命令动态设置慢查询的阈值。

- **slow_sql.threshold**

  设置慢查询的时间阈值（单位：毫秒）。查询执行时间超过此阈值的将被记录。

  - **动态设置：**
    可以通过SQL命令动态设置慢查询阈值：
    ```sql
    SELECT set_config('slow_sql_min_duration', '10000', true);
    ```
    或
    ```sql
    ALTER SYSTEM SET slow_sql_min_duration TO '10000';
    ```
## 使用
安装并配置插件后，满足慢查询阈值条件的查询将被自动记录到Opentenbase的日志文件中。你可以根据日志文件中的信息来识别和分析慢查询。

日志示例：
```
LOG:  slow query: xxx, execution time: xx ms.'
```

## 卸载插件
如果需要从数据库中卸载`slow_sql`插件，可以执行以下命令：
```sql
DROP EXTENSION slow_sql;
```
之后，还需要在数据库的配置文件中删除或注释掉相关的配置项，并重启数据库。

## 注意事项
- 插件的使用可能会对数据库性能产生轻微影响，尤其是在高负载条件下。
- 检查Opentenbase的日志配置，确保慢查询日志可以被正确记录和存储。
- 考虑定期审查和分析慢查询日志，以便及时识别和解决性能问题。

## 测试
可以查看slow_sql_tests.sql文件中的测试用例，以了解插件的基本功能和使用方法。