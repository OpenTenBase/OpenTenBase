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

- C++17 或更高版本
- CMake 3.10 或更高版本
- libssh2 开发库
- CLI11 库（已包含在项目中）

### 编译步骤

1. 克隆代码仓库：
```bash
git clone <repository_url>
cd opentenbase_ctl
```

2. 创建构建目录：
```bash
mkdir build
cd build
```

3. 配置和编译：
```bash
cmake ..
make
```

4. 安装（可选）：
```bash
make install
```

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
     --ssh-password Tdsql@2024 \
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
     --ssh-password Tdsql@2024 \
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
