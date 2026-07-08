<img src="images/OpenTenBase_logo.svg" width="60%" />

___
# OpenTenBase 数据库管理系统

OpenTenBase 是基于 Postgres-XL 项目的先进企业级数据库管理系统。它支持SQL标准的扩展子集，包括事务、外键、用户定义类型和函数。此外，它还添加了并行计算、安全性、管理、审计和其他功能。

OpenTenBase具有许多类似于PostgreSQL的语言接口，其中的一些可以在下面的链接中找到：

	https://www.postgresql.org/download

## 概览
一个 OpenTenBase 集群由多个 CoordinateNodes、DataNodes 和 GTM 节点组成。所有用户数据都存储在 DataNodes 中，CoordinateNode 仅包含元数据，GTM 用于全局事务管理。CoordinateNodes 和 DataNodes 共享相同的模式。

用户总是连接到 CoordinateNodes，CoordinateNodes 将查询分解为在 DataNodes 中执行的片段，并收集结果。

您可以在以下链接获取 OpenTenBase 软件的最新版本：

	https://github.com/OpenTenBase/OpenTenBase

更多信息可以从我们的网站中获取：

	https://www.opentenbase.org/

## 构建
### 系统要求

内存: 最小 8G RAM

操作系统: TencentOS 2, TencentOS 3, OpenCloudOS 8.x, CentOS 7, CentOS 8, Ubuntu 18.04

### 依赖

以下依赖安装命令需要使用 root 用户执行，或在命令前添加 `sudo`。

对于 RedHat/CentOS：

```shell
yum -y install git sudo gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel sshpass libcurl-devel libxml2-devel
```

对于 Ubuntu/Debian：

```shell
apt update
apt install -y git sudo gcc make libreadline-dev zlib1g-dev libssl-dev \
  libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass libxml2-dev \
  libcurl4-openssl-dev libzstd-dev liblz4-dev language-pack-zh-hans
```

Ubuntu 18.04 软件源提供的 zstd 1.3.3 缺少当前源码使用的部分 API。如果编译时出现 zstd API 未定义错误，请安装更新版本的 zstd 后重新执行 `configure`。本次 Ubuntu 18.04 验证使用 zstd 1.5.5。

使用 `opentenbase_ctl` 部署前，还需要确保目标服务器已启动 SSH 服务。Ubuntu/Debian 可使用以下命令安装 SSH 服务和端口检查工具：

```shell
apt install -y openssh-server iproute2
```

### 创建用户 'opentenbase'

以下命令需要使用 root 用户执行，或在命令前添加 `sudo`。

```bash
# 1. 创建目录 /data
mkdir -p /data

# 2. 添加用户
useradd -d /data/opentenbase -s /bin/bash -m opentenbase # 添加用户 opentenbase

# 3. 设置密码
passwd opentenbase # 设置密码

# 4. 将用户添加到 wheel 组
# 对于 RedHat
usermod -aG wheel opentenbase
# 对于 Debian
usermod -aG sudo opentenbase

# 5. RedHat/CentOS 需要通过 visudo 为 wheel 组启用 sudo 权限
visudo
# 然后取消注释 "% wheel" 行，保存并退出
```

Ubuntu/Debian 默认通过 `sudo` 组授权，不需要取消注释 `%wheel`。完成后可以执行 `visudo -c` 检查 sudoers 配置语法。

### 编译

先切换到 `opentenbase` 用户。以下源码下载、编译和安装命令均使用该用户执行：

```bash
su - opentenbase
cd /data/opentenbase/
git clone https://github.com/OpenTenBase/OpenTenBase

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/

cd ${SOURCECODE_PATH}
rm -rf ${INSTALL_PATH}/opentenbase_bin_v5.0
chmod +x configure*
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make clean
make -s -j2
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -s -j2
make install
```

`-j2` 已在最小部署路径中验证，可根据可用内存调整并发数。不要省略 `-j` 后的数字；`make -sj` 会启用不受限制的并发，在内存不足时可能导致编译进程被系统终止。

## 安装
使用 OPENTENBASE\_CTL 工具来搭建一个集群，例如：搭建一个具有1个全局事务管理节点(GTM)、1个协调器节点(COORDINATOR)以及2个数据节点(DATANODE)的集群。
<img src="images/topology.png" width="50%" />
### 准备工作

#### 1. 安装 opentenbase 并将 opentenbase 安装包的路径导入到环境变量中。

以下命令使用 `opentenbase` 用户执行，并且只对当前 shell 生效。切换用户或重新登录后需要重新设置，或将相同的 `export` 命令加入该用户的 shell 启动文件。

```shell
PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export LC_ALL=C

command -v opentenbase_ctl
command -v psql
psql --version
```

#### 2. 禁用 SELinux 和防火墙（可选）

```
vi /etc/selinux/config 
set SELINUX=disabled

# 禁用防火墙
sudo systemctl disable firewalld
sudo systemctl stop firewalld
```

#### 3. 创建用于初始化实例的 *.tar.gz 包。

```shell
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *
tar -tzf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz | head
cd ${INSTALL_PATH}
```

包名中的版本号会用于生成远端安装目录。修改包名时，需要同步修改配置文件中 `[instance]` 的 `package` 路径，并保留点分数字版本（例如 `5.21.8`）。

### 集群启动步骤

#### 生成并填写配置文件

当前版本的 `opentenbase_ctl` 不支持 `prepare config` 子命令，需要手工创建配置文件。配置中的 IP、SSH 端口和密码必须与实际环境一致。

* opentenbase\_config.ini 中各字段说明

| 配置类别        | 配置项            | 说明                                                                      |
|----------------|------------------|---------------------------------------------------------------------------|
| instance       | name             | 实例名称，可用字符：字母、数字、下划线，例如：opentenbase_instance01        |
|                | type             | distributed 表示分布式模式，需要 gtm、coordinator 和 data 节点；centralized 表示集中式模式 |
|                | package          | 软件包。完整路径（推荐）或相对于 opentenbase_ctl 的相对路径                  |
| gtm            | master           | 主节点，只有一个 IP                                                        |
|                | slave            | 从节点。如果需要 n 个从节点，在此配置 n 个 IP，用逗号分隔                    |
| coordinators   | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | cn001-cn006 共 6 个节点，每个服务器分布 2 个节点                            |
|                | conf             | CN 自定义 GUC 配置文件的绝对路径；没有自定义配置时文件可以为空，但必须存在    |
| datanodes      | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | dn001-dn006 共 6 个节点，每个服务器分布 2 个节点                            |
|                | conf             | DN 自定义 GUC 配置文件的绝对路径；没有自定义配置时文件可以为空，但必须存在    |
| server         | ssh-user         | 远程命令执行用户名，需要提前创建，所有服务器应有相同账户以简化配置管理          |
|                | ssh-password     | 远程命令执行密码，需要提前创建，所有服务器应有相同密码以简化配置管理            |
|                | ssh-port         | SSH 端口，所有服务器应保持一致以简化配置管理                                 |
| log            | level            | opentenbase_ctl 工具执行的日志级别（不是 opentenbase 节点的日志级别）        |

#### 1. 为实例创建配置文件 opentenbase\_config.ini

以下命令在 `${INSTALL_PATH}` 中创建部署配置和空的自定义 GUC 文件：

```shell
cd ${INSTALL_PATH}
touch postgres.conf opentenbase_config.ini
vim opentenbase_config.ini
```

* 单机最小分布式配置

以下配置用于在一台服务器上部署 1 个 GTM、1 个 CN 和 1 个 DN，已在 `127.0.0.1` 上完成验证。使用前必须将 `ssh-password` 和 `ssh-port` 改为当前 `opentenbase` 用户的实际 SSH 认证信息。

```ini
[instance]
name=opentenbase_minimal
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

[gtm]
master=127.0.0.1
slave=

[coordinators]
master=127.0.0.1
slave=
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

[datanodes]
master=127.0.0.1
slave=
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

[server]
ssh-user=opentenbase
ssh-password=请替换为实际密码
ssh-port=36000

[log]
level=INFO
```

* 例如，如果我有两台服务器 172.16.16.49 和 172.16.16.131，分布在两台服务器上的典型分布式实例配置如下。您可以复制此配置信息并根据您的部署要求进行修改。不要忘记填写 ssh 密码配置。

```ini
# 实例配置
[instance]
name=opentenbase01
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# GTM 节点
[gtm]
master=172.16.16.49
slave=172.16.16.50,172.16.16.131

# 协调器节点
[coordinators]
master=172.16.16.49
slave= 172.16.16.131
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

# 数据节点
[datanodes]
master=172.16.16.49,172.16.16.131
slave=172.16.16.131,172.16.16.49
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```


* 同样，典型集中式实例的配置如下。不要忘记填写 ssh 密码配置。

```ini
# 实例配置
[instance]
name=opentenbase02
type=centralized
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# 数据节点
[datanodes]
master=172.16.16.49
slave=172.16.16.131
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```

#### 2. 执行实例安装命令。

安装前，先从执行机验证配置中的每台服务器都可以通过相同的 SSH 用户、密码和端口登录。以下命令以单机最小配置为例：

```shell
sshpass -p '请替换为实际密码' ssh -o StrictHostKeyChecking=no \
  -p 36000 opentenbase@127.0.0.1 'id'
```

SSH 验证通过后，在 `${INSTALL_PATH}` 目录执行安装：

```shell
cd ${INSTALL_PATH}
opentenbase_ctl install -c opentenbase_config.ini
```

以下是多节点安装成功时的输出示例：

```text

====== Start to Install Opentenbase test_cluster01  ====== 

step 1: Make *.tar.gz pkg ...
    Make opentenbase-5.21.8-i.x86_64.tar.gz successfully.

step 2: Transfer and extract pkg to servers ...
    Package_path: /data/opentenbase/opentenbase_ctl/opentenbase-5.21.8-i.x86_64.tar.gz
    Transfer and extract pkg to servers successfully.

step 3: Install gtm master node ...
    Install gtm0001(172.16.16.49) ...
    Install gtm0001(172.16.16.49) successfully
    Success to install  gtm master node. 

step 4: Install cn/dn master node ...
    Install cn0001(172.16.16.49) ...
    Install dn0001(172.16.16.49) ...
    Install dn0002(172.16.16.131) ...
    Install cn0001(172.16.16.49) successfully
    Install dn0001(172.16.16.49) successfully
    Install dn0002(172.16.16.131) successfully
    Success to install all cn/dn master nodes. 

step 5: Install slave nodes ...
    Install gtm0002(172.16.16.131) ...
    Install cn0001(172.16.16.131) ...
    Install dn0001(172.16.16.131) ...
    Install dn0002(172.16.16.49) ...
    Install gtm0002(172.16.16.131) successfully
    Install dn0002(172.16.16.49) successfully
    Install dn0001(172.16.16.131) successfully
    Install cn0001(172.16.16.131) successfully
    Success to install all slave nodes. 

step 6: Create node group ...
    Create node group successfully. 

====== Installation completed successfully  ====== 
```
* 当您看到 'Installation completed successfully' 字样时，表示安装已完成。尽情享受您的 opentenbase 之旅吧。
* 您可以检查实例的状态

```shell
opentenbase_ctl status -c opentenbase_config.ini
```

状态输出示例：

```text

------------- Instance status -----------  
Instance name: test_cluster01
Version: 5.21.8

-------------- Node status --------------  
Node gtm0001(172.16.16.49) is Running 
Node dn0001(172.16.16.49) is Running 
Node dn0002(172.16.16.49) is Running 
Node cn0001(172.16.16.49) is Running 
Node dn0002(172.16.16.131) is Running 
Node cn0001(172.16.16.131) is Running 
Node gtm0002(172.16.16.131) is Running 
Node dn0001(172.16.16.131) is Running 
[Result] Total: 8, Running: 8, Stopped: 0, Unknown: 0

------- Master CN Connection Info -------  
[1] cn0001(172.16.16.49)  
Environment variable: export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
PSQL connection: psql -h 172.16.16.49 -p 11000 -U opentenbase postgres 
```

## 使用

### 连接到 CN 主节点执行 SQL

先执行状态命令，从 `Master CN Connection Info` 中获取实际安装目录、CN IP 和端口：

```shell
opentenbase_ctl status -c opentenbase_config.ini
```

以下命令对应前面的单机最小配置。端口被占用时工具会自动选择其他端口，因此应以状态命令的实际输出为准：

```shell
export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib
export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:$PATH
psql -h 127.0.0.1 -p 11003 -U opentenbase -d postgres
```

### 基础 SQL 验证

连接 CN 后，可以执行以下 SQL 验证版本和基本读写。本示例中的 `SHARD` 分布语法已使用当前源码构建结果验证：

```sql
select version();

drop table if exists opentenbase_quickstart;
create table opentenbase_quickstart (
    id integer,
    note text
) distribute by shard(id);

insert into opentenbase_quickstart values (1, 'alpha'), (2, 'beta');
select id, note from opentenbase_quickstart order by id;
```

查询应返回刚插入的两行数据。当前构建使用 `DISTRIBUTE BY HASH(id)` 会报 `Cannot support distribute type: Hash`，不要将旧版 `HASH` 示例用于该验证路径。

## 常见错误与排查

### 1. `opentenbase_ctl: command not found`

可能原因是 OpenTenBase 的 `bin` 目录未加入当前 shell 的 `PATH`，或切换用户后环境变量未重新设置。

```shell
echo "$PATH"
echo "$PG_HOME"
command -v opentenbase_ctl
```

确认 `PG_HOME` 指向实际编译安装目录后重新设置：

```shell
export PG_HOME=/data/opentenbase/install/opentenbase_bin_v5.0
export PATH="$PG_HOME/bin:$PATH"
```

### 2. 动态库加载失败：`cannot open shared object file`

检查当前库路径以及 `postgres` 是否存在未解析的动态库：

```shell
echo "$LD_LIBRARY_PATH"
ldd "$PG_HOME/bin/postgres" | grep 'not found'
```

确认路径后重新设置：

```shell
export LD_LIBRARY_PATH="$PG_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

### 3. SSH 登录或软件包分发失败

确认 sshd 已启动、配置中的 `ssh-port` 与实际监听端口一致，并从执行机验证相同的用户和密码：

```shell
ss -lntp | grep 36000
sshpass -p '请替换为实际密码' ssh -o StrictHostKeyChecking=no \
  -p 36000 opentenbase@127.0.0.1 'id'
```

未配置公钥时，`ssh localhost` 免密登录失败是正常的；配置了 `ssh-password` 后，`opentenbase_ctl` 可以通过 `sshpass` 使用密码认证。多机部署时，应对配置中的每个 IP 执行同样的登录检查。

### 4. 节点端口被占用

当前工具从 `11000` 开始为节点自动寻找端口。安装前可以检查该端口段是否已有监听进程：

```shell
ss -lntp | grep -E ':(11000|11001|11002|11003|11004|11005|11006|11007|11008)[[:space:]]'
```

如果存在冲突，应先确认占用进程是否属于已有 OpenTenBase 实例。不要直接终止未知进程；清理旧实例或释放端口后，再使用新的实例名称执行安装。

### 5. Ubuntu/Debian 编译时缺少依赖

本次 Ubuntu 18.04 验证中实际出现过以下错误：

* `zstd library not found`：检查 `libzstd-dev`、`liblz4-dev` 和 zstd 版本。
* `curl/curl.h: No such file or directory`：安装 `libcurl4-openssl-dev`。
* `curl-config: Command not found`：同样检查 `libcurl4-openssl-dev` 是否安装成功。

安装依赖前先执行 `apt update`。Ubuntu 18.04 自带的 zstd 1.3.3 对当前源码过旧，本次验证使用 zstd 1.5.5。

### 6. 权限、配置文件或路径不一致

创建 `/data` 和 `opentenbase` 用户需要管理员权限；源码编译、打包和集群安装应使用 `opentenbase` 用户。检查关键路径的属主和可读性：

```shell
whoami
ls -ld /data /data/opentenbase ${INSTALL_PATH}
test -r ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz
test -r ${INSTALL_PATH}/postgres.conf
```

配置文件中的 `package` 和 `conf` 建议使用绝对路径。`conf` 指向的文件即使没有自定义 GUC 也必须存在；缺少该字段或文件会导致配置解析失败。


## 引用  

```
https://docs.opentenbase.org/
```

## 谁在使用 OpenTenBase
腾讯


## 许可

OpenTenBase 使用 BSD 3-Clause 许可证，版权和许可信息可以在 [LICENSE.txt](LICENSE.txt) 中找到。

## 贡献者
感谢所有参与项目贡献的人: [CONTRIBUTORS](CONTRIBUTORS.md)

## 最新消息和活动

|新闻|
|------|
|[开放原子校源行走进苏南，加速开源人才培养和创新能力提升](https://mp.weixin.qq.com/s/SU5NYTcKQPyHqfiT4OXp8Q)|
|[OpenTenBase首亮相，腾讯云数据库开源取得重大突破](https://www.opentenbase.org/news/news-post-3/)|
|[开放原子校源行走进西部，加速开源人才培养](https://www.opentenbase.org/event/event-post-3/)|
|[开源数据库OpenTenBase获信通院“OSCAR尖峰开源项目优秀案例”奖](https://www.opentenbase.org/news/news-post-2/)|
|[开放原子开源基金会赴黑龙江科技大学走访交流](https://www.opentenbase.org/event/event-post-2/)|

## 博客和文章
|博客和文章|
|------------------|
|[快速入门](https://www.opentenbase.org/blog/01-quickstart/)|

## 历史
[history_events](history_events.md)
