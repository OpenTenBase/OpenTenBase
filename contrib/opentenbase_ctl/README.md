# OpenTenBase Cluster Management Tool

OpenTenBase 集群管理工具，用于管理 OpenTenBase 集群的安装、删除、启动、停止、扩容和缩容等操作。

## 功能特性

- 支持通过配置文件或命令行参数管理集群
- 支持集群的安装、删除、启动、停止操作
- 支持集群的扩容和缩容操作
- 自动端口分配和管理
- 详细的日志记录
- 支持多种节点类型（CN、DN）

## 编译安装

### 依赖项

| 依赖 | 用途 | Debian / Ubuntu | RHEL / CentOS |
| --- | --- | --- | --- |
| g++ (C++17) | 编译器 | `g++` | `gcc-c++` |
| libssh2 | 远程执行、SCP | `libssh2-1-dev` | `libssh2-devel` |
| OpenSSL | 加密 | `libssl-dev` | `openssl-devel` |
| libpq（头文件 + 库） | 连接数据库 | `libpq-dev` | `postgresql-devel` |
| libpqxx 7.x | C++ 的 PostgreSQL 客户端 | `libpqxx-dev` | `libpqxx-devel` |
| CLI11 1.9+ | 命令行解析（纯头文件） | `libcli11-dev` | 见下方说明 |
| cmake (>= 3.16) | 仅当需要就地编译 libpqxx 时 | `cmake` | `cmake` |

几点说明：

- **`pg_config` 必须在 `PATH` 中**（OpenTenBase 或 PostgreSQL 的安装自带）。
  Makefile 用它定位 libpq 的头文件和库，并把库目录同时写进链接参数和 rpath，
  所以从源码装到自定义前缀时，运行期也不用手动 export `LD_LIBRARY_PATH`。
- **libpqxx 优先用系统已安装的版本**。系统里没有时，Makefile 会用仓库自带的
  `lib/libpqxx-7.7.5.tar.gz` **就地编译到 `build/pqxx/`——不需要 root，也不会写 `/usr/local`**。
  只有走这条路时才需要 `cmake`。libpqxx 7.x 默认产出静态库，会被直接链进二进制，
  因此运行期不依赖 `libpqxx.so`。
- **CLI11 是纯头文件库**，放到 `/usr/local/include/CLI/CLI.hpp` 即可。
  发行版没有对应包时：

```bash
sudo mkdir -p /usr/local/include/CLI
sudo curl -sSL https://github.com/CLIUtils/CLI11/releases/download/v1.9.1/CLI11.hpp \
     -o /usr/local/include/CLI/CLI.hpp
```

### 编译步骤

`opentenbase_ctl` 是 OpenTenBase 的一个 contrib 模块，通常随主项目一起编译：

```bash
./configure --prefix=<安装路径> ...    # 参数见仓库根目录 README
make && make install
cd contrib && make && make install
```

只编这一个模块也可以：

```bash
make -C contrib/opentenbase_ctl
```

产物是 `contrib/opentenbase_ctl/opentenbase_ctl`，`make install` 后位于 `$prefix/bin`。
`make clean` 会同时清掉就地编译的 libpqxx，下次构建自动重建。

### 常见编译 / 运行问题

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `fatal error: pqxx/pqxx: No such file or directory` | 系统没有 libpqxx，且仓库自带的源码包被删了 | 安装 `libpqxx-dev` / `libpqxx-devel`，或恢复 `lib/libpqxx-7.7.5.tar.gz` |
| `fatal error: CLI/CLI.hpp: No such file or directory` | 缺 CLI11 头文件 | 按上面的命令放置 `CLI/CLI.hpp` |
| `Could NOT find PostgreSQL` | `pg_config` 不在 `PATH`，或没装 libpq 开发包 | 把 `$OPENTENBASE_HOME/bin` 加进 `PATH`，或安装 `libpq-dev` |
| `error while loading shared libraries: libpqxx.so` | 链接的是系统的动态 libpqxx，运行期找不到 | 现已通过 rpath / 静态链接规避；若仍出现，检查是否装了多套 libpqxx |
| `no version information available (required by .../libpq.so.5)` | 运行期加载到了另一套 OpenTenBase 的 libpq | 保证 `PATH` 与 `LD_LIBRARY_PATH` 指向同一套安装 |

## 使用方法

### 配置文件

配置文件使用 INI 格式，包含以下主要部分：

```ini
[meta]
etcd-server = 127.0.0.1:2379
meta-id = 1
shard-num = 2

[instance]
instance-name = opentenbase_cluster
package-path = /path/to/opentenbase_package.tar.gz

[node]
node-name = cn1,cn2,dn1,dn2
node-ip = 192.168.1.101,192.168.1.102,192.168.1.103,192.168.1.104
data-path = /data/opentenbase/cn1,/data/opentenbase/cn2,/data/opentenbase/dn1,/data/opentenbase/dn2
install-path = /opt/opentenbase/cn1,/opt/opentenbase/cn2,/opt/opentenbase/dn1,/opt/opentenbase/dn2

[server]
ssh-user = root
ssh-password = your_password
ssh-port = 22

[log]
level = info
```

### 命令行参数

工具支持以下命令：

- `install`: 安装新的 OpenTenBase 集群
- `delete`: 删除现有的 OpenTenBase 集群
- `start`: 启动 OpenTenBase 集群
- `stop`: 停止 OpenTenBase 集群
- `expand`: 扩容 OpenTenBase 集群
- `shrink`: 缩容 OpenTenBase 集群

### 命令范例

1. 使用配置文件安装集群：
```bash
./opentenbase_ctl install -c config.ini
```

2. 使用命令行参数安装集群：
```bash
./opentenbase_ctl install \
    --etcd-server 127.0.0.1:2379 \
    --meta-id 1 \
    --instance-name opentenbase_cluster \
    --package-path /path/to/opentenbase_package.tar.gz \
    --node-name cn1,cn2,dn1,dn2 \
    --node-ip 192.168.1.101,192.168.1.102,192.168.1.103,192.168.1.104 \
    --ssh-user root \
    --ssh-password your_password \
    --ssh-port 22
```

3. 启动集群：
```bash
./opentenbase_ctl start -c config.ini
```

4. 停止集群：
```bash
./opentenbase_ctl stop -c config.ini
```

5. 扩容集群（添加新节点）：
```bash
./opentenbase_ctl expand \
    --etcd-server 127.0.0.1:2379 \
    --meta-id 1 \
    --instance-name opentenbase_cluster \
    --package-path /path/to/opentenbase_package.tar.gz \
    --node-name cn3,dn3 \
    --node-ip 192.168.1.105,192.168.1.106 \
    --data-path /data/opentenbase/cn3,/data/opentenbase/dn3 \
    --install-path /opt/opentenbase/cn3,/opt/opentenbase/dn3 \
    --ssh-user root \
    --ssh-password your_password \
    --ssh-port 22

./opentenbase_ctl expand \
     --etcd-server 172.16.16.50:2379 \
     --meta-id 201 \
     --cluster-oid 35770\
     --instance-name cluster03 \
     --package-path /data/opentenbase_pgxz-3.16.9.0.tar.gz \
     --node-name dn3 \
     --node-ip 172.16.16.83 \
     --ssh-user opentenbase \
     --ssh-password your_password \
     --ssh-port 36000
```

6. 缩容集群（移除节点）：
```bash
./opentenbase_ctl shrink \
    --instance-name cluster03 \
     --package-path /data/opentenbase_pgxz-3.16.9.0.tar.gz \
     --node-name dn3 \
     --node-ip 172.16.16.83 \
     --ssh-user opentenbase \
     --ssh-password your_password \
     --ssh-port 36000
```

### 节点命名规则

- CN 节点：必须以 `cn` 开头，例如 `cn1`, `cn2` 等
- DN 节点：必须以 `dn` 开头，例如 `dn1`, `dn2` 等

工具会根据节点名称自动判断节点类型，无需额外指定。

## 日志

日志级别可以通过配置文件或环境变量设置：

- 配置文件：在 `[log]` 部分设置 `level` 参数
- 环境变量：设置 `OPENTENBASE_LOG_LEVEL` 环境变量

支持的日志级别：
- debug
- info
- warn
- error

## 注意事项

1. 确保所有节点的 SSH 访问权限正确配置
2. 确保安装包路径正确且可访问
3. 确保数据目录和安装目录有足够的权限
4. 建议在操作前备份重要数据

## 许可证

[许可证类型]

## 贡献指南

[贡献指南内容] 
