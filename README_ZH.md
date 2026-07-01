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

操作系统: TencentOS 2, TencentOS 3, OpenCloudOS 8.x, OpenCloudOS 9, CentOS 7, CentOS 8, Ubuntu 18.04

### 依赖

**yum / dnf（RHEL 系）：**

```
yum -y install git sudo gcc gcc-c++ make readline-devel zlib-devel openssl-devel uuid-devel \
  bison flex cmake postgresql-devel libssh2-devel sshpass libcurl-devel libxml2-devel \
  libxslt-devel perl-ExtUtils-Embed python3-devel libicu-devel pam-devel \
  libevent-devel libyaml-devel lz4-devel libzstd-devel
```

**apt（Debian 系）：**

```
apt install -y git sudo gcc g++ make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev \
  bison flex cmake libpq-dev libssh2-1-dev sshpass libcurl4-openssl-dev libxml2-dev \
  libxslt1-dev libperl-dev python3-dev libicu-dev libpam0g-dev \
  libevent-dev libyaml-dev liblz4-dev libzstd-dev language-pack-zh-hans
```

> **提示**：部分发行版（如 OpenCloudOS 9）仓库可能未收录 `cli11-devel`。若编译时报 CLI11 相关错误，可从 [CLI11 源码](https://github.com/CLIUtils/CLI11) 编译安装，或使用 `--without-cli11` 选项跳过。

### 创建用户 'opentenbase'

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

# 5. 为 wheel 组启用 sudo 权限（通过 visudo）
visudo
# 然后取消注释 "% wheel" 行，保存并退出
```

### 编译

```bash
su - opentenbase
cd /data/opentenbase/

# GitHub 直连（网络畅通时推荐）
git clone https://github.com/OpenTenBase/OpenTenBase

# 备选 1：使用 ghfast.top 代理加速
# git clone https://ghfast.top/https://github.com/OpenTenBase/OpenTenBase.git

# 备选 2：使用 Gitee 镜像
# git clone https://gitee.com/opentenbase/OpenTenBase.git

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/

cd ${SOURCECODE_PATH}
rm -rf ${INSTALL_PATH}/opentenbase_bin_v5.0
chmod +x configure*
# --disable-license 与 -DNOLIC 等价，二选一即可
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make clean
make -sj
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -sj
make install
```

## 安装
使用 OPENTENBASE\_CTL 工具来搭建一个集群，例如：搭建一个具有1个全局事务管理节点(GTM)、1个协调器节点(COORDINATOR)以及2个数据节点(DATANODE)的集群。
<img src="images/topology.png" width="50%" />
### 准备工作

#### 1. 安装 opentenbase 并将 opentenbase 安装包的路径导入到环境变量中。

建议将环境变量写入 `~/.bash_profile`（而非 `~/.bashrc`），确保登录时自动加载：

```bash
# 写入 ~/.bash_profile
cat >> ~/.bash_profile <<'EOF'

# OpenTenBase 环境变量
PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"
export LC_ALL=C
EOF

# 立即生效
source ~/.bash_profile

# 验证
which psql
echo $LD_LIBRARY_PATH
```

> **注意**：`LD_LIBRARY_PATH` 必须正确设置，否则 `psql`、`pg_ctl` 等工具会报 `error while loading shared libraries` 错误。如果发现 `PATH` 不生效，检查 `~/.bashrc` 是否覆盖了 `~/.bash_profile` 的设置。

#### 2. 禁用 SELinux 和防火墙（可选）

```
vi /etc/selinux/config
set SELINUX=disabled

# 禁用防火墙
sudo systemctl disable firewalld
sudo systemctl stop firewalld
```

> **注意**：OpenCloudOS 9 / 部分 CentOS 精简版默认未安装 `firewalld`，上述命令会报 `Unit firewalld.service could not be found`。此时请改用以下方式：
>
> ```
> # 方式 1：使用 iptables
> sudo systemctl stop iptables
> sudo systemctl disable iptables
>
> # 方式 2：使用 nftables
> sudo systemctl stop nftables
> sudo systemctl disable nftables
>
> # 方式 3：仅放行 OpenTenBase 所需端口（推荐生产环境）
> sudo firewall-cmd --add-port=30001/tcp --permanent  # GTM
> sudo firewall-cmd --add-port=30004/tcp --permanent  # CN
> sudo firewall-cmd --add-port=30006-30007/tcp --permanent  # DN
> sudo firewall-cmd --reload
> # 若无 firewalld，使用 iptables：
> # sudo iptables -A INPUT -p tcp --dport 30001 -j ACCEPT
> # sudo iptables -A INPUT -p tcp --dport 30004 -j ACCEPT
> ```

#### 3. 创建用于初始化实例的 *.tar.gz 包。

```
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *
cd ${INSTALL_PATH}
```

#### 4. 配置 SSH 免密登录（多节点部署必需）

多节点部署时，`opentenbase_ctl` 通过 SSH 远程操作各节点，需要提前配置免密登录。单节点部署也建议配置，避免本机 SSH 操作需要输入密码。

```bash
# 以 opentenbase 用户执行
su - opentenbase

# 生成密钥（非交互式，无密码）
mkdir -p ~/.ssh && chmod 700 ~/.ssh
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N "" -C "opentenbase@localhost"

# 将公钥加入 authorized_keys
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# 多节点部署时，还需将公钥复制到所有节点
# ssh-copy-id -i ~/.ssh/id_rsa.pub opentenbase@<远程节点IP>

# 验证免密登录
ssh opentenbase@localhost echo "SSH OK"
```

### 集群启动步骤

#### 生成并填写配置文件
opentenbase\_config.opentenbase\_ctl 工具可以生成配置文件的模板。您需要在模板中填写集群节点信息。启动 opentenbase\_ctl 工具后，将在当前用户的主目录中生成 opentenbase\_ctl 目录。输入 "prepare config" 命令后，将在 opentenbase\_ctl 目录中生成可直接修改的配置文件模板。

* opentenbase\_config.ini 中各字段说明
```
| 配置类别        | 配置项            | 说明                                                                      |
|----------------|------------------|---------------------------------------------------------------------------||
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
| datanodes      | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | dn001-dn006 共 6 个节点，每个服务器分布 2 个节点                            |
| server         | ssh-user         | 远程命令执行用户名，需要提前创建，所有服务器应有相同账户以简化配置管理          |
|                | ssh-password     | 远程命令执行密码，需要提前创建，所有服务器应有相同密码以简化配置管理            |
|                | ssh-port         | SSH 端口，所有服务器应保持一致以简化配置管理                                 |
| log            | level            | opentenbase_ctl 工具执行的日志级别（不是 opentenbase 节点的日志级别）        |

```

#### 1. 为实例创建配置文件 opentenbase\_config.ini
```
mkdir -p ./logs
touch opentenbase_config.ini
vim opentenbase_config.ini
```

* 例如，如果我有两台服务器 172.16.16.49 和 172.16.16.131，分布在两台服务器上的典型分布式实例配置如下。您可以复制此配置信息并根据您的部署要求进行修改。不要忘记填写 ssh 密码配置。
```
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

# 数据节点
[datanodes]
master=172.16.16.49,172.16.16.131
slave=172.16.16.131,172.16.16.49
nodes-per-server=1

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
```
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

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```

* **集中式单节点最小配置（适用于单机快速体验 / 开发测试）**

如果只有一台机器，可以使用以下最小配置快速体验 OpenTenBase：

```
# 实例配置
[instance]
name=opentenbase_single
type=centralized
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# 数据节点（单节点，不配置 slave）
[datanodes]
master=127.0.0.1
nodes-per-server=1

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=22

# 日志配置
[log]
level=DEBUG
```

> **注意**：集中式单节点模式不需要配置 `[gtm]` 和 `[coordinators]` 段，GTM 功能内嵌在 CN 中。不要填写 `slave=` 行，否则解析器可能报 `Failed to parse configuration file`。

**集中式模式端口规划：**

| 节点 | 角色 | 默认端口 | 说明 |
|------|------|---------|------|
| dn0001 | DataNode | 20001 | 数据节点 |
| cn0001 | Coordinator | 5432 | 协调节点（客户端连接入口）|

> 集中式模式下 GTM 内嵌在 CN 中，无需独立 GTM 进程和端口。

#### 2. 执行实例安装命令。

```
export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase_bin_v5.0/lib
./opentenbase_bin_v5.0/bin/opentenbase_ctl install  -c opentenbase_config.ini

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

> **注意**：`opentenbase_ctl install` 命令只执行初始化（initdb），**不会自动启动节点**。安装完成后，请使用以下命令启动集群：
>
> ```bash
> ./opentenbase_bin_v5.0/bin/opentenbase_ctl start -c opentenbase_config.ini
> ```
>
> 也可以通过 `status` 命令查看节点状态：
>
> ```bash
> ./opentenbase_bin_v5.0/bin/opentenbase_ctl status -c opentenbase_config.ini
> ```

* 当您看到 'Installation completed successfully' 字样时，表示安装已完成。尽情享受您的 opentenbase 之旅吧。
* 您可以检查实例的状态
```
[opentenbase@VM-16-49-tencentos opentenbase_ctl]$ ./opentenbase_bin_v5.0/bin/opentenbase_ctl status -c opentenbase_config.ini

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
* 连接到 CN 主节点执行 SQL

```
export LD_LIBRARY_PATH=/home/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/home/opentenbase/install/opentenbase/5.21.8/bin:${PATH}
$ psql -h ${CoordinateNode_IP} -p ${CoordinateNode_PORT} -U opentenbase -d postgres

postgres=#

```

## 常见错误与排查

在部署和使用 OpenTenBase 过程中，可能会遇到以下常见问题。本节按问题类型分类，提供现象、原因和解决方案。

### 环境类

| 现象 | 原因 | 解决方案 |
|------|------|---------|
| `systemctl stop firewalld` 报 `Unit firewalld.service could not be found` | OpenCloudOS 9 / 部分精简版系统默认未安装 firewalld，使用 iptables/nftables | 改用 `systemctl stop iptables` 或 `systemctl stop nftables`；或放行所需端口 |
| `dnf install` 找不到 `cli11-devel` | 部分发行版仓库未收录此包 | 从 [CLI11 源码](https://github.com/CLIUtils/CLI11) 编译安装，或使用 `--without-cli11` 跳过 |
| `make` 阶段报缺 `libzstd` / `lz4` 静态库 | `libzstd-devel` / `lz4-devel` 未预装 | `dnf install -y libzstd-devel lz4-devel` |
| `git clone` 超时或速度极慢 | GitHub 网络不稳定 | 使用 `ghfast.top` 代理前缀或 Gitee 镜像（见"编译"小节备选命令） |

### 编译类

| 现象 | 原因 | 解决方案 |
|------|------|---------|
| `contrib` 编译报 `Permission denied` | `make_signature` 文件无可执行权限 | `chmod +x contrib/pgxc_ctl/make_signature`（在 `cd contrib` 之前执行）|
| `make install` 报 `Permission denied` | `--prefix` 路径权限不足 | `chown -R opentenbase:opentenbase ${INSTALL_PATH}` |
| `configure` 报 `libxml2 not found` | `libxml2-devel` 未安装 | `dnf install -y libxml2-devel` |
| `make` 报 `fatal error: libxslt/xslt.h: No such file` | `libxslt-devel` 未安装 | `dnf install -y libxslt-devel` |

### 启动类

| 现象 | 原因 | 解决方案 |
|------|------|---------|
| `opentenbase_ctl install` 报 `Failed to parse configuration file` | `[datanodes]` 段 `slave=` 为空时解析器误判 | 集中式单节点模式不写 `slave=` 行 |
| `opentenbase_ctl install` 报 GTM 连接失败 | 集中式模式无独立 GTM 进程，但配置仍写了 `[gtm]` 段 | 集中式模式只保留 `[datanodes]` + `[server]` + `[log]` 段 |
| `pg_ctl start` 后进程没起来 | 未指定节点类型 | 必须加 `-Z datanode`（或 `-Z coordinator`）|
| `opentenbase_ctl install` 完成但节点未运行 | install 命令只执行 initdb，不自动启动 | 安装完成后执行 `opentenbase_ctl start -c opentenbase_config.ini` |
| `opentenbase_ctl` 报 `pg_config: command not found` | `PATH` 未包含 `$PG_HOME/bin` | 确认环境变量已正确设置并 `source ~/.bash_profile` |

### 连接类

| 现象 | 原因 | 解决方案 |
|------|------|---------|
| `psql` 连不上 CN | `pg_hba.conf` 未放行本机网段 | 添加 `host all all 0.0.0.0/0 md5` 或本机 IP 的 `trust` 规则 |
| 环境变量 `PATH` 没生效 | `~/.bashrc` 覆盖了 `~/.bash_profile` | 将环境变量写入 `~/.bash_profile` 顶部并 `source`；或合并去重 |
| `psql` 报 `server closed the connection unexpectedly` | `listen_addresses` 未放开 | 在 `postgresql.conf` 中设置 `listen_addresses = '*'` |
| `psql` 报 `error while loading shared libraries` | `LD_LIBRARY_PATH` 未包含 `$PG_HOME/lib` | 确认 `~/.bash_profile` 中 `LD_LIBRARY_PATH` 已设置并 `source` |

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
|[开源数据库OpenTenBase获信通院"OSCAR尖峰开源项目优秀案例"奖](https://www.opentenbase.org/news/news-post-2/)|
|[开放原子开源基金会赴黑龙江科技大学走访交流](https://www.opentenbase.org/event/event-post-2/)|

## 博客和文章
|博客和文章|
|------------------|
|[快速入门](https://www.opentenbase.org/blog/01-quickstart/)|

## 历史
[history_events](history_events.md)
