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

内存：最小 8 GB RAM

操作系统：TencentOS 2、TencentOS 3、OpenCloudOS 8.x、CentOS 7、CentOS 8、Ubuntu 18.04

> **版本提示：** 请优先使用上述经过验证的操作系统版本。较新的发行版可能自带不兼容的编译器或基础库（例如 Ubuntu 22.04 的 OpenSSL 3）；此时建议使用 [OpenTenBase-DevEnv](https://github.com/OpenTenBase/OpenTenBase-DevEnv) 提供的容器环境。

### 依赖

RedHat 系统（以下以 CentOS 7 为例，其他系统请启用能提供相同软件包的对应仓库）：

```bash
# CentOS 7 安装 libzstd/lz4 静态库前需要启用 EPEL
sudo yum install -y epel-release
sudo yum install -y git sudo gcc gcc-c++ make readline-devel zlib-devel \
  openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel \
  sshpass libcurl-devel libxml2-devel libzstd-devel libzstd-static \
  lz4-devel lz4-static

# 当前 configure 脚本从 /usr/local/lib 查找这两个静态库
sudo mkdir -p /usr/local/lib
sudo ln -sf /usr/lib64/libzstd.a /usr/local/lib/libzstd.a
sudo ln -sf /usr/lib64/liblz4.a /usr/local/lib/liblz4.a
```

Debian/Ubuntu 系统：

```bash
sudo apt update
sudo apt install -y git sudo gcc g++ make libreadline-dev zlib1g-dev \
  libssl-dev libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass \
  libcurl4-openssl-dev libxml2-dev libzstd-dev liblz4-dev \
  language-pack-zh-hans

# Debian/Ubuntu 的静态库位于 multiarch 目录
MULTIARCH="$(gcc -print-multiarch)"
sudo mkdir -p /usr/local/lib
sudo ln -sf "/usr/lib/${MULTIARCH}/libzstd.a" /usr/local/lib/libzstd.a
sudo ln -sf "/usr/lib/${MULTIARCH}/liblz4.a" /usr/local/lib/liblz4.a
```

### 创建用户 'opentenbase'

```bash
# 1. 创建目录 /data
sudo mkdir -p /data

# 2. 添加用户
sudo useradd -d /data/opentenbase -s /bin/bash -m opentenbase

# 3. 设置密码
sudo passwd opentenbase

# 4. 添加 sudo 权限（二选一）
# RedHat
sudo usermod -aG wheel opentenbase
# Debian/Ubuntu
sudo usermod -aG sudo opentenbase

# 5. 仅 RedHat 需要确认 wheel 组已在 sudoers 中启用
sudo visudo
# 确认 "%wheel ALL=(ALL) ALL" 已取消注释
```

Debian/Ubuntu 默认已经启用 `sudo` 组，无需修改 `sudoers`。切换用户后可运行 `sudo -v` 验证权限。

### 编译

```bash
su - opentenbase
cd /data/opentenbase
git clone https://github.com/OpenTenBase/OpenTenBase.git

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install
export PG_HOME="${INSTALL_PATH}/opentenbase_bin_v5.0"
export JOBS="${JOBS:-$(nproc)}"

cd "${SOURCECODE_PATH}"
rm -rf "${PG_HOME}"
chmod +x configure*
./configure --prefix="${PG_HOME}" --enable-user-switch --with-libxml \
  --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make clean
make -j"${JOBS}"
make install

chmod +x contrib/pgxc_ctl/make_signature
cd "${SOURCECODE_PATH}/contrib"
make -j"${JOBS}"
make install
```

`make -sj` 中的 `-s` 会隐藏编译命令，而没有数值的 `-j` 不限制并行任务数，不利于首次部署排错。上面的 `JOBS` 默认使用 CPU 核数；内存不足时可先执行 `export JOBS=2`。

## 安装
使用 OPENTENBASE\_CTL 工具来搭建一个集群，例如：搭建一个具有1个全局事务管理节点(GTM)、1个协调器节点(COORDINATOR)以及2个数据节点(DATANODE)的集群。
<img src="images/topology.png" width="50%" />
### 准备工作

#### 1. 安装 opentenbase 并将 opentenbase 安装包的路径导入到环境变量中。

```bash
export PG_HOME="${INSTALL_PATH}/opentenbase_bin_v5.0"
export PATH="${PG_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${PG_HOME}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export LC_ALL=C

command -v opentenbase_ctl
command -v psql
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

```bash
export PACKAGE_PATH="${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz"
tar -C "${PG_HOME}" -zcf "${PACKAGE_PATH}" .
tar -tzf "${PACKAGE_PATH}" | head
```

使用 `tar -C ... .` 可以确保安装目录中的隐藏文件也被打包，并避免因当前工作目录错误而生成不完整的软件包。

### 集群启动步骤

#### 生成并填写配置文件

当前版本的 `opentenbase_ctl` 不提供 `prepare config` 子命令，需要手动创建配置文件。可参考仓库中的 [`contrib/opentenbase_ctl/config/config.ini`](contrib/opentenbase_ctl/config/config.ini)，并按实际服务器信息修改。

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
|                | conf             | CN 的自定义 GUC 配置文件；没有自定义项时也需提供一个空文件                    |
| datanodes      | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | dn001-dn006 共 6 个节点，每个服务器分布 2 个节点                            |
|                | conf             | DN 的自定义 GUC 配置文件；没有自定义项时也需提供一个空文件                    |
| server         | ssh-user         | 远程命令执行用户名，需要提前创建，所有服务器应有相同账户以简化配置管理          |
|                | ssh-password     | 远程命令执行密码，需要提前创建，所有服务器应有相同密码以简化配置管理            |
|                | ssh-port         | SSH 服务实际监听端口，默认通常为 22；所有服务器应保持一致                    |
| log            | level            | opentenbase_ctl 工具执行的日志级别（不是 opentenbase 节点的日志级别）        |

#### 1. 为实例创建配置文件 opentenbase\_config.ini

```bash
cd /data/opentenbase
mkdir -p logs
touch postgres.conf opentenbase_config.ini
vim opentenbase_config.ini
```

`postgres.conf` 用于提供自定义 GUC；没有自定义项时保持空文件即可。

* 例如，如果有两台服务器 `172.16.16.49` 和 `172.16.16.131`，典型分布式实例配置如下。请根据实际环境修改 IP、SSH 密码和端口。

```ini
# 实例配置
[instance]
name=opentenbase01
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# GTM 节点
[gtm]
master=172.16.16.49
slave=172.16.16.131

# 协调器节点
[coordinators]
master=172.16.16.49
slave=172.16.16.131
nodes-per-server=1
conf=/data/opentenbase/postgres.conf

# 数据节点
[datanodes]
master=172.16.16.49,172.16.16.131
slave=172.16.16.131,172.16.16.49
nodes-per-server=1
conf=/data/opentenbase/postgres.conf

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=22

# 日志配置
[log]
level=DEBUG
```

* 首次体验可在单台服务器上部署一个集中式实例。即使目标是本机，`opentenbase_ctl` 仍会通过 SSH 执行部分操作；请先确保 `ssh -p 22 opentenbase@127.0.0.1 'echo ok'` 成功，再使用以下最小配置。

```ini
# 实例配置
[instance]
name=opentenbase_minimal
type=centralized
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# 数据节点
[datanodes]
master=127.0.0.1
nodes-per-server=1
conf=/data/opentenbase/postgres.conf

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=22

# 日志配置
[log]
level=DEBUG
```

#### 2. 执行实例安装命令

```bash
cd /data/opentenbase
export PG_HOME=/data/opentenbase/install/opentenbase_bin_v5.0
export PATH="${PG_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${PG_HOME}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
opentenbase_ctl install -c /data/opentenbase/opentenbase_config.ini
```

典型分布式实例安装成功时会看到类似输出：

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

```bash
opentenbase_ctl status -c /data/opentenbase/opentenbase_config.ini
```

典型状态输出如下：

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

## 常见错误与排查

### `configure` 找不到 zstd 或 lz4

如果出现 `zstd library not found`、`lz4 library not found`，或日志中包含 `cannot find /usr/local/lib/libzstd.a`，请检查开发包和静态库链接：

```bash
ls -l /usr/local/lib/libzstd.a /usr/local/lib/liblz4.a
test -r /usr/local/lib/libzstd.a
test -r /usr/local/lib/liblz4.a
```

若文件不存在，重新执行“依赖”小节中对应操作系统的安装和软链接命令。Ubuntu 还需要 `libcurl4-openssl-dev`，否则编译会报 `curl/curl.h: No such file or directory`。

### `opentenbase_ctl`、`psql` 找不到或动态库加载失败

每次打开新终端后都需要设置安装目录。若希望永久生效，可将以下三行加入 `~/.bashrc`：

```bash
export PG_HOME=/data/opentenbase/install/opentenbase_bin_v5.0
export PATH="${PG_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${PG_HOME}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
```

检查命令和动态库：

```bash
command -v opentenbase_ctl psql
ldd "$(command -v opentenbase_ctl)" | grep "not found"
```

`grep` 没有输出表示未发现缺失的动态库。

### SSH 连接失败或超时

`ssh-port` 必须填写目标服务器 SSH 服务实际监听端口，不能直接照抄示例。部署前从执行机逐台验证：

```bash
SSH_PORT=22
SERVER_IP=127.0.0.1
ssh -p "${SSH_PORT}" opentenbase@"${SERVER_IP}" 'echo ssh-ok'
```

若失败，请依次检查：

1. 目标机的 `sshd` 是否启动，`ss -lnt` 是否显示对应端口。
2. 防火墙和安全组是否允许执行机访问该 SSH 端口。
3. `ssh-user`、`ssh-password` 是否与目标机一致，以及 SSH 是否允许密码认证。
4. `/data/opentenbase` 及其子目录是否属于 `opentenbase` 用户。

### 端口被占用

`opentenbase_ctl` 从 `11000` 开始为每个节点寻找三个连续可用端口（节点端口、pooler 端口和 forward 端口）。部署前可检查占用情况：

```bash
ss -lnt | grep -E ':(1100[0-9]|110[1-9][0-9])\b'
```

如果日志显示 `Failed to assign ports for nodes`，请释放冲突端口，或检查 SSH 是否有权限在目标机执行 `ss -tln`。跨服务器访问时，还需要在防火墙和安全组中开放最终分配的节点端口。

### 配置文件或软件包路径错误

`package` 以及 `[coordinators]`、`[datanodes]` 中的 `conf` 建议全部填写绝对路径，并在安装前检查：

```bash
test -r /data/opentenbase/opentenbase_config.ini
test -r /data/opentenbase/postgres.conf
test -r /data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz
tar -tzf /data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz | head
```

工具日志位于执行命令时所在目录的 `logs/` 下。失败后可运行 `tail -n 100 logs/opentenbase_ctl_*.log` 查看详细原因。

## 使用
* 连接到 CN 主节点执行 SQL

```bash
export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib
export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH}
psql -h "${CoordinateNode_IP}" -p "${CoordinateNode_PORT}" -U opentenbase -d postgres

postgres=# 

```

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
