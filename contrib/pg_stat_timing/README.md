#### 编译插件

使用 gcc 编译器的编译工具链编译插件代码

```
gcc -Wall -Werror -shared -o pg_stat_timing.so pg_stat_timing.c -I/path/to/opentenbase/include -L/path/to/opentenbase/lib -lopenbase
```



#### 配置插件

编辑 OpenTenBase 的配置文件，确保 `shared_preload_libraries` 配置项包含插件的名称。

```
shared_preload_libraries = 'pg_stat_timing'
```



#### 重启 OpenTenBase

重新启动 OpenTenBase 以使插件加载。



#### 加载插件

需要在 OpenTenBase 数据库中加载该插件。使用以下 SQL 命令在数据库中加载插件：

```
CREATE EXTENSION pg_stat_timing;
```



#### 设置预期时间

使用提供的自定义函数 `set_expected_time()` 来设置预期的查询执行时间。在 SQL 控制台中执行以下命令：

```
SELECT set_expected_time(100000); -- 设置预期时间为100毫秒
```



#### 执行查询

当用户执行查询时，插件会跟踪查询的执行时间，并根据预期时间进行检查。如果查询执行时间超出了预期时间，将会生成相应的日志信息。例如：

```
LOG:  Query (ID: 123) exceeded expected time: 100000 microseconds (actual: 150000 microseconds)
```



#### 定位慢SQL

 查询的执行时间信息将记录在日志中。可以查看日志以获取有关查询执行时间的详细信息。

