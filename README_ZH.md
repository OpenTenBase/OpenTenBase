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

* 对于 RedHat/CentOS:
```bash
yum -y install git sudo gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel sshpass libcurl-devel libxml2-devel
```

* 对于 Debian/Ubuntu:
```bash
apt install -y git sudo gcc make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass libxml2-dev language-pack-zh-hans
```

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
# 在高版本 GCC 环境建议在 CFLAGS 中加上 -mcx16 以支持 128 位 CPU 原子指令
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g -std=gnu99 -Wall -Wno-error=incompatible-pointer-types -mcx16"
make clean
# 为避免多进程并发生成词法分析器代码时抢夺临时文件 (lex.backup) 导致报错，建议先单进程顺序预生成代码
make -C src -j1 distprep
make -sj$(nproc)
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make CXXFLAGS="-include cstdint" -sj$(nproc)
make install
```

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
|                | conf             | 协调节点（CN）自定义 GUC 配置文件绝对路径。若无修改默认参数需求，需通过 touch postgres.conf 创建空文件并指明其绝对路径 |
| datanodes      | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | dn001-dn006 共 6 个节点，每个服务器分布 2 个节点                            |
|                | conf             | 数据节点（DN）自定义 GUC 配置文件绝对路径。若无修改默认参数需求，需通过 touch postgres.conf 创建空文件并指明其绝对路径 |
| server         | ssh-user         | 远程命令执行用户名，需要提前创建，所有服务器应有相同账户以简化配置管理          |
|                | ssh-password     | 远程命令执行密码，需要提前创建，所有服务器应有相同密码以简化配置管理            |
|                | ssh-port         | SSH 端口，所有服务器应保持一致以简化配置管理                                 |
| log            | level            | opentenbase_ctl 工具执行的日志级别（不是 opentenbase 节点的日志级别）        |

```

#### 1. 为实例创建配置文件 opentenbase\_config.ini

> **注意**：最新版的 `opentenbase_ctl` 工具要求 `[coordinators]` 和 `[datanodes]` 下必须指明 `conf` 参数。如不需要自定义内核启动 GUC 参数，请先创建一个空的配置文件：`touch /data/opentenbase/install/postgres.conf`。

```bash
mkdir -p ./logs
touch opentenbase_config.ini
touch postgres.conf # 创建空的自定义 GUC 参数文件
vim opentenbase_config.ini
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
slave=172.16.16.131
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

## 常见错误与排查

### 一、 基础环境与环境变量问题

1. **`psql: command not found` 或加载共享库报错 `cannot open shared object file`**
   * **原因**：当前终端的环境变量 `PATH` 和 `LD_LIBRARY_PATH` 未正确包含 OpenTenBase 的安装路径。
   * **解决**：在终端执行以下命令（或将其写入 `~/.bashrc` / `~/.bash_profile`）：
     ```bash
     export PG_HOME=/data/opentenbase/install/opentenbase_bin_v5.0
     export PATH="$PATH:$PG_HOME/bin"
     export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"
     ```

2. **创建 Linux 用户或命令提示权限不足 (`Permission denied`)**
   * **解决**：在非 root 账户下执行需特权管理操作（如 `useradd`、`mkdir /data` 等）时，请在命令前加上 `sudo` 命令，并确保用户已加入 `wheel` / `sudo` 用户组。

### 二、 源码编译阶段常见错误

1. **链接时报错 `undefined reference to '__sync_val_compare_and_swap_16'`**
   * **原因**：在某些操作系统中，`uname -p` 可能返回 `unknown`，导致 `configure` 脚本无法自动侦测并启用 x86_64 128 位 CPU 硬件原子指令。
   * **解决**：在 `./configure` 时将 `-mcx16` 显式加入到 `CFLAGS` 参数中：
     ```bash
     ./configure ... CFLAGS="-g -std=gnu99 -Wall -mcx16"
     make clean && make -sj$(nproc)
     ```

2. **多核并发编译报 `rm: cannot remove 'lex.backup': No such file or directory`**
   * **原因**：在 GNU Make 多进程高并发（`-j$(nproc)`）编译时，`src/fe_utils` 等目录下的多个 Flex 词法分析脚本同时尝试读写和清理临时文件 `lex.backup` 产生竞态冲突。
   * **解决**：在执行高并发全量编译前，先用单进程顺序将各类语法分析器源码生成好：
     ```bash
     make -C src -j1 distprep
     make -sj$(nproc)
     ```

3. **高版本 GCC 编译报错 `error: 'bool' cannot be defined via 'typedef'`**
   * **原因**：较新版本的 GCC 默认采用 C23 标准，`bool` 已作为原生关键字，不可再用 `typedef` 重新定义。
   * **解决**：在 `./configure` 的 `CFLAGS` 中明确追加 `-std=gnu99`。

4. **高版本 GCC 编译 `opentenbase_ctl` 报 `uint64_t does not name a type`**
   * **原因**：GCC 13/14 进行了标准库头文件清理（Header cleanups），`<string>` 等头文件不再间接包含 `<cstdint>`。
   * **解决**：进入 `contrib` 编译时加入 `CXXFLAGS="-include cstdint"`：
     ```bash
     cd contrib
     make CXXFLAGS="-include cstdint" -sj$(nproc)
     ```

5. **缺少压缩或加密依赖库 (`zstd library not found` / `lz4 library not found`)**
   * **解决**：通过包管理器安装对应开发库；若 `/usr/lib/` 存在共享库但无法找到静态连接库，可执行软连接修复：`sudo ln -s /usr/lib/liblz4.so /usr/local/lib/liblz4.a`。

### 三、 `opentenbase_ctl` 工具部署与网络连接问题

1. **SCP 传输失败报错 `SCP transfer failed with exit code 32512`**
   * **原因**：Linux shell 返回错误码 `32512` 对应十进制 `127`（Command not found）。`opentenbase_ctl` 工具底层进行 SSH 下发和 SCP 文件传输时，硬编码依赖 `sshpass` 命令行工具。若未安装 `sshpass` 则会直接导致分发卡死。
   * **解决**：通过包管理器安装 `sshpass`（如 Arch Linux 下执行 `sudo pacman -S --needed sshpass`，CentOS 下 `yum install -y sshpass`）。

2. **配置文件解析报错 `Failed to parse configuration file .`**
   * **原因**：`opentenbase_ctl` 在解析配置文件时，强行校验 `[coordinators]` 和 `[datanodes]` 下的 `conf=` 字段（指向 GUC 配置文件）。若模板中遗漏了 `conf=`，程序会尝试读取空路径并报错。
   * **解决**：通过 `touch /data/opentenbase/install/postgres.conf` 创建一个空的自定义配置文件，并在 ini 的 `[coordinators]` 和 `[datanodes]` 下分别加上一行 `conf=/data/opentenbase/install/postgres.conf`（绝对路径）。
