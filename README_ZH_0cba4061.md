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

``` 
yum -y install git sudo gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel sshpass  libcurl-devel libxml2-devel libzstd-devel libzstd-static lz4-devel lz4-static
```

或者

```
apt install -y git sudo gcc make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass libxml2-dev
```

> **注意**：
> 1. 编译时 `configure` 会链接 zstd / lz4 的**静态库**（默认查找 `/usr/local/lib/libzstd.a` 和 `/usr/local/lib/liblz4.a`）。如果系统包安装后编译仍报 `zstd library not found` 或 `lz4 library not found`，请将静态库软链接到该路径，例如：
>    ```bash
>    mkdir -p /usr/local/lib
>    ln -s /usr/lib64/libzstd.a /usr/local/lib/libzstd.a   # Debian/Ubuntu 路径为 /usr/lib/x86_64-linux-gnu/
>    ln -s /usr/lib64/liblz4.a /usr/local/lib/liblz4.a
>    ```
>    或从源码编译安装 zstd / lz4（`make lib && make install`）。
> 2. `opentenbase_ctl` 部署时会以 `zh_CN.utf8` 作为 initdb 的 locale（且不可通过配置文件修改）。请确保操作系统已生成该 locale，否则 initdb 会静默失败（详见「常见错误与排查」）：
>    ```bash
>    locale -a | grep zh_CN.utf8 || localedef -c -f UTF-8 -i zh_CN zh_CN.utf8
>    ```
>    Ubuntu 可直接安装 `language-pack-zh-hans`；该包在 Debian 上不存在，请使用 `localedef` 生成。
> 3. `postgresql-devel`（CentOS）需启用 EPEL 源。

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
git clone https://github.com/OpenTenBase/OpenTenBase

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/

cd ${SOURCECODE_PATH}
rm -rf ${INSTALL_PATH}/opentenbase_bin_v5.0
chmod +x configure*
# 注意：x86_64 平台上必须保留 -mcx16。configure 会自动为 x86_64 追加该选项，
# 但命令行传入 CFLAGS 时会整体覆盖，导致链接期报
# `undefined reference to '__sync_val_compare_and_swap_16'`
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g -mcx16"
make clean
make -sj
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -sj
make install
```

> **提示**：如果曾用错误的 CFLAGS 编译过，重新 configure 后务必执行 `make clean`，否则旧的目标文件（不含 `-mcx16`）会导致链接失败。

## 安装
使用 OPENTENBASE\_CTL 工具来搭建一个集群，例如：搭建一个具有1个全局事务管理节点(GTM)、1个协调器节点(COORDINATOR)以及2个数据节点(DATANODE)的集群。
<img src="images/topology.png" width="50%" />
### 准备工作

#### 1. 安装 opentenbase 并将 opentenbase 安装包的路径导入到环境变量中。

```shell
PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
export PATH="$PATH:$PG_HOME/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"
export LC_ALL=C
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

> **说明**：`opentenbase_ctl install` 执行时会输出 `step 1: Make *.tar.gz pkg ...` 并自行完成打包。该手动打包步骤用于提前准备好配置文件中 `package=` 指向的安装包；如果打包失败，也可以先创建空的 tar 包，ctl 工具会重新生成。注意文件名需与配置文件中的 `package` 路径一致。

```
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *
cd ${INSTALL_PATH}
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
|                | conf             | **必填**。GUC 参数覆盖文件路径，说明同 datanodes 的 conf                     |
| datanodes      | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | dn001-dn006 共 6 个节点，每个服务器分布 2 个节点                            |
|                | conf             | **必填**。CN/DN 的 GUC 参数覆盖文件路径（内容可为空文件）。当前版本即使留空也会尝试解析该文件，未配置会导致 `There are some errors in the coordinators/datanodes configurations` 报错。GTM 节点无此配置项 |
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
slave=172.16.16.131

# 协调器节点
[coordinators]
master=172.16.16.49
slave= 172.16.16.131
nodes-per-server=1
conf=/data/opentenbase/cn_guc.conf

# 数据节点
[datanodes]
master=172.16.16.49,172.16.16.131
slave=172.16.16.131,172.16.16.49
nodes-per-server=1
conf=/data/opentenbase/dn_guc.conf

# 登录和部署账户
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```

其中 `cn_guc.conf` / `dn_guc.conf` 为 CN/DN 的 GUC 参数覆盖文件，可先创建为空文件（`touch cn_guc.conf dn_guc.conf`），也可以按 `key = value` 格式写入需要覆盖的参数，例如 `max_connections = 2000`。

* 单机最小化验证配置（1 GTM + 1 CN + 2 DN，全部部署在本机 127.0.0.1），适合快速验证部署流程：
```
[instance]
name=otb_minimal
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

[gtm]
master=127.0.0.1

[coordinators]
master=127.0.0.1
nodes-per-server=1
conf=/data/opentenbase/cn_guc.conf

[datanodes]
master=127.0.0.1
nodes-per-server=2
conf=/data/opentenbase/dn_guc.conf

[server]
ssh-user=opentenbase
ssh-password=<你的密码>
ssh-port=22

[log]
level=DEBUG
```
单机部署时需保证本机 sshd 已启动，且 opentenbase 用户可以通过密码登录本机（`ssh opentenbase@127.0.0.1` 验证）。


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
conf=/data/opentenbase/dn_guc.conf

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
* 当您看到 'Installation completed successfully' 字样时，表示安装已完成。

#### 3. 启动实例并检查状态

> **注意**：`install` 完成后集群并不会自动保持运行状态（部分版本 install 末尾的启动动作在缺少 locale 等情况下会静默失败，且仍显示成功）。请务必手动执行 `start` 并用 `status` 确认所有节点为 Running：

```
./opentenbase_bin_v5.0/bin/opentenbase_ctl start -c opentenbase_config.ini
```

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

> **说明**：`opentenbase_ctl install` 会将安装包重新解压到 `~/install/opentenbase/<版本号>/` 作为各节点的运行时目录（如上例的 `/data/opentenbase/install/opentenbase/5.21.8`），它与源码编译目录 `install/opentenbase_bin_v5.0` 是两个不同路径。连接集群时使用前者，或直接使用源码编译目录中的 psql 也可以。

## 使用
* 连接到 CN 主节点执行 SQL

```
# 使用 opentenbase_ctl 部署后的运行时目录（路径中的版本号以实际部署为准）
export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
# 或者使用源码编译目录
# export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase_bin_v5.0/lib && export PATH=/data/opentenbase/install/opentenbase_bin_v5.0/bin:$PATH

$ psql -h ${CoordinateNode_IP} -p ${CoordinateNode_PORT} -U opentenbase -d postgres

postgres=# 

```

## 常见错误与排查

以下问题均在全新环境按本文档部署时实际复现过。

### 1. `configure: error: zstd library not found` / `lz4 library not found`

* **原因**：依赖清单漏装 zstd / lz4，或已安装系统包但 `configure` 固定链接 `/usr/local/lib/libzstd.a`、`/usr/local/lib/liblz4.a`（静态库），系统包安装位置不在该路径。
* **排查**：`ls /usr/local/lib/libzstd.a /usr/local/lib/liblz4.a`；查看 `config.log` 中 `cannot find /usr/local/lib/libzstd.a` 即可确认。
* **解决**：安装带静态库的开发包（`libzstd-static` / `lz4-static`），并软链接到 `/usr/local/lib/`，或从源码 `make lib && make install`。

### 2. 编译链接期 `undefined reference to '__sync_val_compare_and_swap_16'`

* **原因**：命令行传入 `CFLAGS="-g"` 覆盖了 configure 为 x86_64 自动追加的 `-mcx16`。
* **解决**：`CFLAGS="-g -mcx16"` 重新 configure，并务必 `make clean` 后再编译（旧目标文件不含该选项时仍会失败）。

### 3. `There are some errors in the coordinators/datanodes configurations`

* **原因**：配置文件 `[coordinators]` / `[datanodes]` 节缺少 `conf` 字段（GUC 覆盖文件路径）。当前版本的 opentenbase_ctl 即使该字段为空也会尝试解析对应文件。
* **解决**：创建空文件 `touch cn_guc.conf dn_guc.conf`，并在配置节中添加 `conf=<绝对路径>`。

### 4. install 显示 "Installation completed successfully"，但 `status` 中所有节点为 Stopped

* **排查步骤**：
  1. 检查操作系统 locale：`locale -a | grep zh_CN.utf8`。initdb 硬编码 `--locale=zh_CN.utf8` 且输出被丢弃，缺少该 locale 时 initdb 静默失败，工具仍显示成功。生成方法：`localedef -c -f UTF-8 -i zh_CN zh_CN.utf8`（Ubuntu 也可安装 `language-pack-zh-hans`）。
  2. 查看节点目录是否完成初始化：`ls ~/run/instance/<实例名>/<节点名>/data/` 下是否存在 `PG_VERSION`、`postgresql.conf` 等文件；`pg_ctl_start.log` 中若出现 `is not a database cluster directory`，说明 initdb 未成功。
  3. install 完成后手动执行 `opentenbase_ctl start -c opentenbase_config.ini`，再用 `status` 确认。
* **工具日志位置**：`~/logs/opentenbase_ctl_*.log`（日志级别由配置文件 `[log] level` 控制），其中 `Executing command:` 行可直接看到远程执行的实际命令。

### 5. SCP 传输失败 / `Failed to transfer package to node`

* **原因**：部署机或目标机 sshd 未运行、端口与 `ssh-port` 配置不一致、密码错误或被 `PermitRootLogin`/`PasswordAuthentication no` 等策略拦截。
* **排查**：手工验证 `sshpass -p <密码> ssh -p <端口> <ssh-user>@<节点IP> 'echo ok'`；`systemctl status sshd`；检查 `/etc/ssh/sshd_config`。
* **说明**：所有目标服务器需提前创建同名 `ssh-user` 且密码相同；单机部署时本机 sshd 也必须运行。

### 6. psql 连接报 `command not found` 或动态库错误

* **原因**：`PATH` / `LD_LIBRARY_PATH` 未指向正确的二进制目录。
* **解决**：
  ```bash
  # 使用 ctl 部署后的运行时目录
  export LD_LIBRARY_PATH=$HOME/install/opentenbase/5.21.8/lib
  export PATH=$HOME/install/opentenbase/5.21.8/bin:$PATH
  # 或使用源码编译目录
  # export LD_LIBRARY_PATH=$HOME/install/opentenbase_bin_v5.0/lib
  # export PATH=$HOME/install/opentenbase_bin_v5.0/bin:$PATH
  ```
  建议写入 `~/.bashrc`。可用 `psql --version` 和 `ldd $(which psql) | grep "not found"` 验证。

### 7. 连接 CN 时 `psql: could not connect to server: Connection refused`

* **排查**：
  1. `opentenbase_ctl status -c opentenbase_config.ini` 确认 CN 为 Running，并从输出中获取实际端口（默认从 11000 起自动分配）。
  2. 防火墙/安全组放通 CN 端口（默认 11000）与 GTM 端口。
  3. `pg_hba.conf` 是否放通了客户端网段。

### 8. 其他建议

* **SELinux**：未禁用时可能阻断节点间通信，参考「禁用 SELinux 和防火墙」一节。
* **环境变量**：`su - opentenbase` 登录（带 `-`）才会加载 profile；直接 `su opentenbase` 可能导致 `PATH` 中找不到编译产物。
* **重复安装**：重新 install 前先用 `opentenbase_ctl delete -c opentenbase_config.ini` 清理旧实例，避免残留数据目录干扰。

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
