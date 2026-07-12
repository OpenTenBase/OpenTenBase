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
yum -y install git sudo gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel sshpass  libcurl-devel libxml2-devel
```

或者

```
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
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make clean
make -j"$(nproc)"
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -j"$(nproc)"
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

# 建议将以上环境变量写入 ~/.bashrc 以持久化，避免每次打开新终端都需要重新设置：
# echo 'export PG_HOME='"${PG_HOME}" >> ~/.bashrc
# echo 'export PATH="$PATH:$PG_HOME/bin"' >> ~/.bashrc
# echo 'export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"' >> ~/.bashrc
# echo 'export LC_ALL=C' >> ~/.bashrc
# source ~/.bashrc
```

#### 2. 禁用 SELinux 和防火墙（可选）

```
vi /etc/selinux/config
# 将 SELINUX=enforcing 改为 SELINUX=disabled，保存退出

# 禁用防火墙
sudo systemctl disable firewalld
sudo systemctl stop firewalld
```

#### 3. 创建用于初始化实例的 `.tar.gz` 包并检查部署工具。

```bash
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *
cd ${INSTALL_PATH}

# 安装前检查：工具和软件包路径必须存在且可读
"${PG_HOME}/bin/opentenbase_ctl" -h
test -r "${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz"
```

### 集群启动步骤

#### 生成并填写配置文件

配置模板位于仓库的 `contrib/opentenbase_ctl/config/config.ini`。将其复制到部署目录后，填写集群节点 IP、软件包绝对路径和 SSH 信息。`opentenbase_ctl` 当前不提供 `prepare config` 子命令。

* `opentenbase_config.ini` 中各字段说明

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
|                | conf             | 可选。自定义 postgresql.conf 的绝对路径，用于覆盖节点初始化后的 GUC 默认配置 |
| datanodes      | master           | 主节点 IP，自动生成节点名称，在每个 IP 上部署 nodes-per-server 个节点        |
|                | slave            | 从节点 IP，数量是主节点的整数倍                                             |
|                |                  | 示例：如果 1 主 1 从，IP 数量与主节点相同；如果 1 主 2 从，IP 数量是主节点的两倍 |
|                | nodes-per-server | 可选，默认 1。每个 IP 上部署的节点数。示例：主节点有 3 个 IP，配置为 2，则有 6 个节点 |
|                |                  | dn001-dn006 共 6 个节点，每个服务器分布 2 个节点                            |
|                | conf             | 可选。自定义 postgresql.conf 的绝对路径，用于覆盖节点初始化后的 GUC 默认配置 |
| server         | ssh-user         | 远程命令执行用户名，需要提前创建，所有服务器应有相同账户以简化配置管理          |
|                | ssh-password     | 远程命令执行密码，需要提前创建，所有服务器应有相同密码以简化配置管理            |
|                | ssh-port         | SSH 端口，所有服务器应保持一致以简化配置管理                                 |
| log            | level            | opentenbase_ctl 工具执行的日志级别（不是 opentenbase 节点的日志级别）        |

#### 1. 为实例创建配置文件 opentenbase\_config.ini
```bash
mkdir -p ./logs
cp ${SOURCECODE_PATH}/contrib/opentenbase_ctl/config/config.ini opentenbase_config.ini
vim opentenbase_config.ini
test -r opentenbase_config.ini
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

#### 2. 执行实例安装命令。

```bash
"${PG_HOME}/bin/opentenbase_ctl" install -c opentenbase_config.ini

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
[opentenbase@VM-16-49-tencentos opentenbase_ctl]$ "${PG_HOME}/bin/opentenbase_ctl" status -c opentenbase_config.ini

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

### 环境变量与路径问题

| 现象 | 原因 | 排查和处理 |
| --- | --- | --- |
| `opentenbase_ctl: command not found` | `PATH` 未包含 `opentenbase_ctl` 所在目录 | 确认 `PG_HOME` 指向安装目录（编译安装的 `--prefix` 路径），并执行 `export PATH="$PG_HOME/bin:$PATH"`。也可以直接使用 `"$PG_HOME/bin/opentenbase_ctl"` 绝对路径调用。 |
| `error while loading shared libraries: libpq.so` 或类似 | 动态库搜索路径未设置 | 执行 `export LD_LIBRARY_PATH="$PG_HOME/lib:${LD_LIBRARY_PATH}"`；若使用 `sudo` 或 `su` 切换用户后失效，可在 `~/.bashrc` 中持久化该变量。 |
| 新开终端后 `PG_HOME`、`PATH` 环境变量丢失 | 仅在当前 Shell 中 `export`，未持久化 | 将以下内容追加到 `~/.bashrc`：`export PG_HOME=<安装目录>`、`export PATH="$PG_HOME/bin:$PATH"`、`export LD_LIBRARY_PATH="$PG_HOME/lib:$LD_LIBRARY_PATH"`，然后 `source ~/.bashrc` 使其生效。 |

### 编译与依赖问题

| 现象 | 原因 | 排查和处理 |
| --- | --- | --- |
| `configure: error: readline library not found` | 缺少 readline 开发库 | RHEL/CentOS: `yum install readline-devel`；Debian/Ubuntu: `apt install libreadline-dev` |
| `configure: error: zlib library not found` | 缺少 zlib 开发库 | RHEL/CentOS: `yum install zlib-devel`；Debian/Ubuntu: `apt install zlib1g-dev` |
| `make: *** No targets specified and no makefile found.` | 未执行 `./configure` 或 configure 失败 | 检查 configure 输出中的 error，修复后重新 `./configure ...` 再 `make` |
| `./configure: No such file or directory` | 未在源码目录下执行，或源码目录路径错误 | 确认 `SOURCECODE_PATH` 指向 git clone 下来的源码目录，然后 `cd ${SOURCECODE_PATH}` 再执行 configure |
| configure 时提示 `--with-libxml` 但 libxml2 未安装 | 编译选项要求 libxml2 开发库 | RHEL/CentOS: `yum install libxml2-devel`；Debian/Ubuntu: `apt install libxml2-dev`；或从 configure 参数中移除 `--with-libxml` |

### SSH 与连通性问题

| 现象 | 原因 | 排查和处理 |
| --- | --- | --- |
| SSH 超时、`Permission denied` 或节点显示 `Unknown` | SSH 凭据、端口不正确，或 sshpass 未安装 | 在部署机上用 `ssh -p <ssh-port> <ssh-user>@<节点IP>` 验证能否免密登录（需输入密码也说明 sshpass 可工作）；检查 `[server]` 段中的 ssh-user、ssh-password、ssh-port 与实际一致；确认部署机已安装 `sshpass`（`which sshpass`）。 |
| `ssh: connect to host ... port 22: Connection refused` | SSH 端口配置错误（默认 22，实际使用了自定义端口如 36000） | 确认 `[server]` 中 `ssh-port` 与实际 SSH 服务监听端口一致；检查 `/etc/ssh/sshd_config` 中 `Port` 的值。 |
| 节点初始化成功但状态为 `Unknown` | SSH 能连接但 pg 进程检查失败 | 登录对应节点，执行 `ps -ef | grep <data_path> | grep -v grep` 确认进程状态；检查节点日志（位于 `<data_path>/pg_log/` 或 `<data_path>/gtm.log`）。 |

### 配置文件问题

| 现象 | 原因 | 排查和处理 |
| --- | --- | --- |
| `Failed to parse config file ...` | ini 文件路径不正确或格式有误 | 使用 `-c opentenbase_config.ini` 显式指定绝对路径；确认 `[instance]` 中的 `package` 是部署机（执行 opentenbase_ctl 的机器）上的文件路径且可读（`test -r <package>`）。 |
| `Package file not found` | `package` 字段指定的软件包不存在 | 检查 package 路径的绝对路径是否正确：编译安装则 tar.gz 应位于 `${INSTALL_PATH}` 上级目录；下载预编译包则确认已 wget 到正确位置。 |
| 实例名称含特殊字符导致部署失败 | 实例名称仅支持字母、数字、下划线 | 修改 `[instance]` 的 `name` 字段，确保不包含 `-`、`.`、空格等字符。 |
| `type` 配置为 `distributed` 但缺少 `[gtm]` 段 | 分布式实例需要 GTM 节点配置 | 确保 `[gtm]` 段至少包含一个 `master` IP。集中式实例使用 `type=centralized` 则无需 `[gtm]` 和 `[coordinators]` 段。 |

### 端口与防火墙问题

| 现象 | 原因 | 排查和处理 |
| --- | --- | --- |
| `psql` 连接 CN/DN 被拒绝（Connection refused） | 数据库节点未启动或端口被防火墙阻止 | 先执行 `opentenbase_ctl status -c opentenbase_config.ini` 确认目标节点为 `Running`；使用 status 输出中的连接串（IP:端口）；检查并放行对应端口：`sudo firewall-cmd --add-port=<端口>/tcp --permanent && sudo firewall-cmd --reload`（或 `sudo ufw allow <端口>/tcp`）。 |
| 端口冲突：启动节点时报端口已被占用 | 自动分配的端口与已有服务冲突 | `opentenbase_ctl` 从 11000 开始自动分配端口（每节点占用连续 3 个端口），检查冲突：`ss -tlnp \| grep <端口号>`；必要时在 `postgres.conf` 中手动指定端口。 |

### 其他常见问题

| 现象 | 原因 | 排查和处理 |
| --- | --- | --- |
| initdb 报 locale 错误（如 `zh_CN.utf8` 不可用） | 系统缺少对应 locale | `locale -a \| grep zh_CN` 检查是否已生成；若缺少则 `sudo locale-gen zh_CN.UTF-8`（Debian/Ubuntu）或 `sudo localedef -i zh_CN -f UTF-8 zh_CN.UTF-8`（RHEL/CentOS）。 |
| 节点启动后立即退出，日志显示 shared memory 相关错误 | 系统共享内存不足 | 检查 `sysctl kernel.shmmax` 和 `kernel.shmall`，适当增大：`sudo sysctl -w kernel.shmmax=<值> && sudo sysctl -w kernel.shmall=<值>`。 |
| SELinux 阻止节点进程启动 | SELinux 处于 enforcing 模式 | 临时关闭验证：`sudo setenforce 0`；永久关闭：编辑 `/etc/selinux/config`，设置 `SELINUX=disabled`，重启生效。 |

首次部署的验证过程、实际遇到的环境问题及复验步骤见[最小部署路径验证记录](doc/DEPLOYMENT_VALIDATION_ZH.md)。

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
