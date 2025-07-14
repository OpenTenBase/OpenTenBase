### redis_fdw使用文档

### 使用说明

 根据官方文档已经安装好$$opentenbase$$服务，然后来安装这个$redis$_$fdw$插件。

#### 下载地址

https://github.com/nahanni/rw_redis_fdw/tree/master

#####  依赖项

**hiredis**

###### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install libhiredis-dev
```

###### CentOS/RHEL

```bash
sudo yum install hiredis-devel
```

###### **hints**

**需要在所有节点都安装hiredis。**

##### 配置路径

```
export PATH=/data/opentenbase/install/opentenbase_bin_v2.0/bin:$PATH
```

##### 进入rw_redis_fdw-master

```
make USE_PGXS=1          # 使用 PGXS 构建
make USE_PGXS=1 install  # 安装到 PostgreSQL 扩展目录
```

##### 通过pgxc_ctl部署

```
pgxc_ctl                              
deploy all                              
init all
```

##### 进入创建redis_fdw EXTENSION

``` 
CREATE EXTENSION redis_fdw;
```

### 使用命令

##### 创建一个服务端

| option 选项       | description 描述                                     | default 默认          |
| ----------------- | ---------------------------------------------------- | --------------------- |
| **host 主机**     | Redis 服务器的 Unix 套接字绝对路径、主机名或 IP 地址 | localhost             |
| **port 端口**     | 服务器网络端口                                       | 6379                  |
| **password 密码** | Redis-authentication Redis 认证                      | *no default 无默认值* |

```
  CREATE SERVER redis_server 
     FOREIGN DATA WRAPPER redis_fdw 
     OPTIONS (host '127.0.0.1', port '6379');
 
  CREATE USER MAPPING FOR PUBLIC
     SERVER redis_server
     OPTIONS (password 'secret');
   \det #可以查看外部表
```

##### Table options

| option 选项         | description 描述                                             |
| ------------------- | ------------------------------------------------------------ |
| **tabletype**       | 必选项。指定 Redis 数据类型：字符串、哈希、多哈希、集合、有序集合、列表、TTL、长度、发布 |
| **key 键**          | 将表绑定到特定键。注意：如果使用此选项，则不要在表中指定“key”列。对于发布表，使用 channel 而不是 key。 |
| **keyprefix**       | 为表中的所有键添加此值作为前缀。一个用途是启用 Redis 中与其他键的命名空间分离。 |
| **readonly 只读**   | read-only table, no writes permitted 只读表，不允许写入      |
| **database 数据库** | 用于 Redis 数据库的（一个整数）                              |

```
  CREATE FOREIGN TABLE ftbl (
     ...
  ) ...
  OPTIONS ( <options> )
```

#####  字符串类型 (string)

```sql
CREATE FOREIGN TABLE rft_str(
    key    TEXT,
    value  TEXT,
    expiry INT
) SERVER redis_server
OPTIONS (tabletype 'string');
```

#####  哈希类型 (hash)

```sql
CREATE FOREIGN TABLE rft_hash(
    key    TEXT,
    field  TEXT,
    value  TEXT,
    expiry INT
) SERVER redis_server
OPTIONS (tabletype 'hash');
```

#####  列表类型 (list)

```sql
CREATE FOREIGN TABLE rft_list(
    key    TEXT,
    value  TEXT,
    "index" INT,
    expiry INT
) SERVER redis_server
OPTIONS (tabletype 'list');
```

#####  集合类型 (set)

```sql
CREATE FOREIGN TABLE rft_set(
    key    TEXT,
    member TEXT,
    expiry INT
) SERVER redis_server
OPTIONS (tabletype 'set');
```

#####  有序集合类型 (zset)

```sql
CREATE FOREIGN TABLE rft_zset(
    key     TEXT,
    member  TEXT,
    score   INT,
    "index" INT,
    expiry  INT
) SERVER redis_server
OPTIONS (tabletype 'zset');
```

#####  发布/订阅 (publish)

```sql
CREATE FOREIGN TABLE rft_pub(
    channel   TEXT,
    message   TEXT,
    len       INT
) SERVER redis_server
OPTIONS (tabletype 'publish');
```

####  查询示例

##### 基本查询

```sql
-- 查询字符串值
SELECT * FROM rft_str WHERE key = 'greeting';

-- 查询哈希字段
SELECT * FROM rft_hash WHERE key = 'user:1000' AND field = 'name';
```

#####  插入数据

```sql
-- 插入字符串
INSERT INTO rft_str(key, value) VALUES ('greeting', 'Hello World');

-- 插入哈希字段
INSERT INTO rft_hash(key, field, value) VALUES ('user:1000', 'name', 'Alice');
```

#####  更新数据

```sql
-- 更新字符串值
UPDATE rft_str SET value = 'Hi there' WHERE key = 'greeting';

-- 更新哈希字段
UPDATE rft_hash SET value = 'Bob' WHERE key = 'user:1000' AND field = 'name';
```

#####  删除数据

```sql
-- 删除键
DELETE FROM rft_str WHERE key = 'greeting';

-- 删除哈希字段
DELETE FROM rft_hash WHERE key = 'user:1000' AND field = 'name';
```

###  高级功能

#####  TTL管理

```sql
CREATE FOREIGN TABLE rft_ttl(
    key    TEXT,
    expiry INT
) SERVER redis_server
OPTIONS (tabletype 'ttl');

-- 设置键的过期时间
UPDATE rft_ttl SET expiry = 3600 WHERE key = 'session:123';

-- 使键持久化
UPDATE rft_ttl SET expiry = 0 WHERE key = 'session:123';
```

#####  发布消息

```sql
-- 发布消息到频道
INSERT INTO rft_pub(channel, message) VALUES ('notifications', 'New message');
```

### 限制

1. 如果WHERE子句中没有将键作为常量或参数提供，无法处理JOIN操作
2. 只能处理简单的WHERE条件（相等、比较、数组包含）
3. 发布/订阅功能仅实现了PUBSUB NUMSUB channel