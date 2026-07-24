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

内存: 最小 8G RAM。单机分布式示例会同时初始化 CN 和 DN，低于该下限时 `initdb` 可能因无法分配共享内存而失败。

操作系统: TencentOS 2, TencentOS 3, OpenCloudOS 8.x, CentOS 7, CentOS 8, Ubuntu 18.04

如果只想体验 OpenTenBase 而不需要走源码编译流程，可以使用官方 [OpenTenBase-DevEnv](https://github.com/OpenTenBase/OpenTenBase-DevEnv) 中的 `example-distributed` 或 `example-centralized` Docker 示例。本节其余内容仍以源码编译和 `opentenbase_ctl` 部署为准。

### 依赖

依赖安装和创建系统用户的命令需要使用 `root` 用户执行，或按当前系统的权限配置添加 `sudo`。

下面的命令已在 CentOS 7 上校验。CentOS 7 自带的 GCC 版本过低，需要通过 SCL 使用 GCC 7；TencentOS、OpenCloudOS 等较新的 Red Hat 系发行版可使用本发行版提供的同名或等价开发包。

```bash
yum -y install centos-release-scl centos-release-scl-rh epel-release
yum -y install devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-binutils \
    git sudo make readline-devel zlib-devel openssl-devel uuid-devel bison flex \
    cmake postgresql-devel libssh2-devel sshpass libcurl-devel libxml2-devel \
    libzstd-devel libzstd-static lz4-devel lz4-static

# configure 会从 /usr/local/lib 查找这两个静态库
install -d /usr/local/lib
ln -s /usr/lib64/libzstd.a /usr/local/lib/libzstd.a
ln -s /usr/lib64/liblz4.a /usr/local/lib/liblz4.a
```

CentOS 7 已停止维护。如果 `yum` 报 `Cannot find a valid baseurl`，需要先把 CentOS、SCL 和 EPEL 仓库切换到仍可访问且可信任的镜像，再重新执行安装。项目官方的 [CentOS 7 DevEnv Dockerfile](https://github.com/OpenTenBase/OpenTenBase-DevEnv/blob/master/Dockerfile.centos) 给出了完整的镜像替换和依赖安装示例；不要通过关闭仓库签名检查来绕过错误。

Ubuntu 18.04：

```bash
apt-get update
apt-get install -y git sudo gcc g++ make libreadline-dev zlib1g-dev libssl-dev \
    libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass \
    libcurl4-openssl-dev libxml2-dev libzstd-dev liblz4-dev language-pack-zh-hans

# Ubuntu 的静态库位于 multiarch 目录，同样需要链接到 configure 使用的路径
ZSTD_STATIC=$(dpkg -L libzstd-dev | awk '/\/libzstd\.a$/ { print; exit }')
LZ4_STATIC=$(dpkg -L liblz4-dev | awk '/\/liblz4\.a$/ { print; exit }')
test -n "${ZSTD_STATIC}" && test -n "${LZ4_STATIC}"
install -d /usr/local/lib
ln -s "${ZSTD_STATIC}" /usr/local/lib/libzstd.a
ln -s "${LZ4_STATIC}" /usr/local/lib/liblz4.a
```

如果上述软链接已经存在，先用 `readlink -f` 核对目标，不需要重复创建或强制覆盖。

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

# 5. 仅在发行版尚未授权 wheel 组时，通过 visudo 编辑 sudoers
visudo
# 确认下面这条规则已启用（百分号与 wheel 之间没有空格）
# %wheel ALL=(ALL) ALL
```

### 编译

```bash
su - opentenbase
cd /data/opentenbase/
git clone https://github.com/OpenTenBase/OpenTenBase

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/

# CentOS 7 使用 SCL 提供的 GCC 7；其他发行版会跳过此行
test ! -f /opt/rh/devtoolset-7/enable || source /opt/rh/devtoolset-7/enable
gcc --version

cd "${SOURCECODE_PATH}"
rm -rf "${INSTALL_PATH:?}/opentenbase_bin_v5.0"
chmod +x configure*
./configure --prefix="${INSTALL_PATH}/opentenbase_bin_v5.0" --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"

# 默认使用 2 个并发任务；内存充足时可在执行前设置更大的 BUILD_JOBS
BUILD_JOBS=${BUILD_JOBS:-2}
make clean
make -j"${BUILD_JOBS}"
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -j"${BUILD_JOBS}"
make install
```

## 安装
使用 OPENTENBASE\_CTL 工具来搭建一个集群，例如：搭建一个具有1个全局事务管理节点(GTM)、1个协调器节点(COORDINATOR)以及2个数据节点(DATANODE)的集群。
<img src="images/topology.png" width="50%" />
### 准备工作

#### 1. 安装 opentenbase 并将 opentenbase 安装包的路径导入到环境变量中。

```shell
export PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
export PATH="$PG_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$PG_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export LC_ALL=C

# 确认当前 shell 使用的是刚刚编译的程序和动态库
command -v psql
command -v opentenbase_ctl
ldd "$PG_HOME/bin/psql" | grep 'not found' && echo "存在未找到的动态库"
```

以上环境变量只对当前 shell 生效。需要长期使用时，可将四条 `export` 命令写入 `~/.bash_profile`，然后执行 `source ~/.bash_profile`。CentOS 7 还应在该文件中加载 `/opt/rh/devtoolset-7/enable`。不要在 `PATH` 或 `LD_LIBRARY_PATH` 中写尚未展开的 `${INSTALL_PATH}` 占位符。

#### 2. 检查 SELinux 和防火墙

在隔离的测试环境中，可以临时停用安全策略来排除问题；生产环境不建议直接关闭 SELinux 或防火墙，应按实际分配的节点端口配置访问规则。`opentenbase_ctl status` 会显示 CN 的实际连接端口。

下面的命令适用于 Red Hat 系发行版。Ubuntu 可使用 `sudo ufw status` 检查防火墙状态。

```bash
getenforce
systemctl is-active firewalld

# 仅用于隔离测试环境中的临时排查，重启后会恢复
sudo setenforce 0
sudo systemctl stop firewalld
```

#### 3. 创建用于初始化实例的 *.tar.gz 包。

```bash
PACKAGE_ARCH=$(uname -m)
export PACKAGE_PATH="${INSTALL_PATH}/opentenbase-5.21.8-i.${PACKAGE_ARCH}.tar.gz"
tar -C "${PG_HOME}" -zcf "${PACKAGE_PATH}" .
test -s "${PACKAGE_PATH}"
printf 'package=%s\n' "${PACKAGE_PATH}"
```

软件包名称必须包含点分版本号，`opentenbase_ctl` 会从文件名中提取版本；架构后缀用于区分不同机器的构建产物。`package` 配置项应填写上述命令打印的压缩包绝对路径，而不是安装目录。

### 集群启动步骤

#### 生成并填写配置文件

当前版本的 `opentenbase_ctl` 没有 `prepare config` 子命令。仓库中的 [`contrib/opentenbase_ctl/config/config.ini`](contrib/opentenbase_ctl/config/config.ini) 可作为模板，也可以按下面的最小配置手工创建。

`opentenbase_config.ini` 中各字段说明：

| 配置类别 | 配置项 | 说明 |
| --- | --- | --- |
| instance | name | 实例名称，可使用字母、数字和下划线，例如 `opentenbase_instance01` |
| | type | `distributed` 表示分布式模式，需要 GTM、Coordinator 和 DataNode；`centralized` 表示集中式模式 |
| | package | 用于分发的安装包路径。建议使用包含版本号的 `.tar.gz` 文件绝对路径 |
| gtm | master | GTM 主节点 IP，只有一个 |
| | slave | GTM 从节点 IP；多个 IP 使用英文逗号分隔，不需要从节点时省略该行 |
| coordinators | master | CN 主节点 IP；每个 IP 部署 `nodes-per-server` 个节点 |
| | slave | CN 从节点 IP，数量应是主节点数量的整数倍；不需要从节点时省略该行 |
| | nodes-per-server | 可选，默认值为 1，最大值为 5 |
| | conf | CN 的自定义 GUC 配置文件绝对路径；没有自定义项时文件可为空，但必须存在 |
| datanodes | master | DN 主节点 IP；每个 IP 部署 `nodes-per-server` 个节点 |
| | slave | DN 从节点 IP，数量应是主节点数量的整数倍；不需要从节点时省略该行 |
| | nodes-per-server | 可选，默认值为 1，最大值为 5 |
| | conf | DN 的自定义 GUC 配置文件绝对路径；没有自定义项时文件可为空，但必须存在 |
| server | ssh-user | `opentenbase_ctl` 登录各节点并执行命令的用户 |
| | ssh-password | SSH 登录密码。配置文件包含明文密码，应设置为仅当前用户可读 |
| | ssh-port | SSH 服务实际监听的端口；示例值不代表数据库节点端口 |
| log | level | `opentenbase_ctl` 的日志级别，不是数据库节点的日志级别 |

#### 1. 为实例创建配置文件

```bash
cd "${INSTALL_PATH}"
mkdir -p logs
touch postgres.conf
touch opentenbase_config.ini
chmod 600 postgres.conf opentenbase_config.ini
```

以下配置在一台 Linux 主机上部署 1 个 GTM、1 个 CN 和 1 个 DN，是理解分布式拓扑的最小示例。即使所有节点都使用 `127.0.0.1`，`opentenbase_ctl` 仍会通过 SSH 执行命令，因此需要先启动 `sshd` 并将密码占位符替换为真实值。

首次连接前，分别核对 `localhost` 和 `127.0.0.1` 的主机指纹，确认无误后接受并写入 `~/.ssh/known_hosts`，然后再次执行以下检查。第二次连接不应再出现主机指纹警告，否则警告文本可能混入工具获取的远端路径，导致后续命令使用错误路径。多机部署时，还要对配置中的每台服务器执行相同检查。

```bash
for host in localhost 127.0.0.1; do
    ssh -p 22 "opentenbase@${host}" 'id && printf "ssh-ok\n"'
done
```

```ini
# 实例配置
[instance]
name=opentenbase_quickstart
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.REPLACE_WITH_UNAME_M.tar.gz

[gtm]
master=127.0.0.1

[coordinators]
master=127.0.0.1
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

[datanodes]
master=127.0.0.1
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

[server]
ssh-user=opentenbase
ssh-password=REPLACE_WITH_OPENTENBASE_PASSWORD
ssh-port=22

[log]
level=INFO
```

将最小配置中的 `REPLACE_WITH_UNAME_M` 替换为打包时 `uname -m` 的实际输出，例如 x86 主机通常为 `x86_64`，ARM64 主机通常为 `aarch64`。配置中的路径必须与打包命令打印的 `PACKAGE_PATH` 完全一致。

例如，在两台服务器 `172.16.16.49` 和 `172.16.16.131` 上部署带从节点的分布式实例时，可使用下面的配置。示例假设两台服务器的 SSH 服务都监听 `36000` 端口；请根据实际环境修改端口和密码。

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
ssh-password=REPLACE_WITH_OPENTENBASE_PASSWORD
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```

典型集中式实例的配置如下：

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
ssh-password=REPLACE_WITH_OPENTENBASE_PASSWORD
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```

#### 2. 执行实例安装命令。

```bash
cd "${INSTALL_PATH}"
"${PG_HOME}/bin/opentenbase_ctl" install -c opentenbase_config.ini
```

根据上述单机最小实例的实际验证，成功输出示例如下。CN 和 DN 并发安装时，相关行的先后顺序可能不同。

```text

====== Start to Install Instance opentenbase_quickstart  ======

step 1: Make *.tar.gz pkg ...
    Make opentenbase-5.21.8-i.x86_64.tar.gz successfully.

step 2: Transfer and extract pkg to servers ...
    Package_path: /data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz
    Transfer and extract pkg to servers successfully.

step 3: Install gtm master node ...
    Install gtm0001(127.0.0.1) ...
    Install gtm0001(127.0.0.1) successfully
    Success to install gtm master node.

step 4: Install cn/dn master node ...
    Install cn0001(127.0.0.1) ...
    Install dn0001(127.0.0.1) ...
    Install cn0001(127.0.0.1) successfully
    Install dn0001(127.0.0.1) successfully
    Success to install all cn/dn master nodes.

step 5: Install slave nodes ...
    Success to install all slave nodes.

step 6: Create node group ...
    Create node group successfully.

====== Installation completed successfully  ======
```

当看到 `Installation completed successfully` 时，安装流程已完成。继续检查所有节点是否为 `Running`：

```bash
"${PG_HOME}/bin/opentenbase_ctl" status -c opentenbase_config.ini
```

该实例的实际输出：

```text

------------- Instance status -----------  
Instance name: opentenbase_quickstart
Version: v5.21.8

-------------- Node status --------------  
Node dn0001(127.0.0.1:11006) is Running
Node gtm0001(127.0.0.1:11000) is Running
Node cn0001(127.0.0.1:11003) is Running
[Result] Total: 3, Running: 3, Stopped: 0, Unknown: 0

------- Master CN Connection Info -------  
[1] cn0001(127.0.0.1)
Environment variable: export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
PSQL connection: psql -h 127.0.0.1 -p 11003 -U opentenbase postgres
```

#### ARM64 原生验证记录

除上述 x86_64 验证外，本流程还在 Apple Silicon 宿主机的原生 `arm64v8/ubuntu:20.04` Docker 容器中完成了独立复验。使用 `opentenbase-5.21.8-i.aarch64.tar.gz` 部署 1 GTM、1 CN 和 1 DN 后，三个节点均为 `Running`；通过 CN 创建 SHARD 分布表、写入并读回两行数据后成功删除。关键证据如下：

```text
uname -m: aarch64
package: opentenbase-5.21.8-i.aarch64.tar.gz
version(): PostgreSQL 10.0 @ OpenTenBase_v5.0 on aarch64-unknown-linux-gnu
[Result] Total: 3, Running: 3, Stopped: 0, Unknown: 0
query rows: arm64-cn-to-dn-ok, second-row
ARM64_MINIMAL_DEPLOYMENT=PASS
```

这是一条可复现性验证记录，不代表扩大项目“系统要求”中列出的正式支持范围。生产部署仍应使用项目明确支持的操作系统和经过团队验证的架构组合。

节点端口由工具从可用端口中分配，不要根据上述示例猜测端口。请复制本次 `status` 输出中的 `PSQL connection` 命令；后续执行 `start`、`stop` 和 `status` 时也建议始终显式传入同一个 `-c` 配置文件。

## 使用

分布式模式下，连接到 CN 主节点执行 SQL。将主机和端口替换为 `opentenbase_ctl status` 的实际输出：

```bash
export CN_HOST=127.0.0.1
export CN_PORT=REPLACE_WITH_STATUS_PORT
psql -h "${CN_HOST}" -p "${CN_PORT}" -U opentenbase -d postgres
```

可以使用下面的 SQL 完成最小读写验证。`ON_ERROR_STOP` 确保任意一条语句失败时命令立即返回非零状态。

```bash
psql -h "${CN_HOST}" -p "${CN_PORT}" -U opentenbase -d postgres -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE IF EXISTS readme_smoke_test;
CREATE TABLE readme_smoke_test (
    id integer,
    note text
) DISTRIBUTE BY SHARD(id);
INSERT INTO readme_smoke_test VALUES (1, 'cn-to-dn-ok'), (2, 'second-row');
SELECT id, note FROM readme_smoke_test ORDER BY id;
DROP TABLE readme_smoke_test;
SQL
```

集中式模式没有独立 CN 和 GTM，客户端直接连接 DN。

## 常见错误与排查

### `psql` 或 `opentenbase_ctl` 命令未找到

确认 `PG_HOME` 指向编译时 `--prefix` 使用的目录，并检查路径优先级：

```bash
printf 'PG_HOME=%s\n' "$PG_HOME"
command -v psql opentenbase_ctl
printf '%s\n' "$PATH" | tr ':' '\n'
```

`$PG_HOME/bin` 应出现在 `PATH` 中，并且最好位于其他 PostgreSQL 安装目录之前。新开 shell 后失效，通常是环境变量只在旧 shell 中设置，或尚未 `source ~/.bash_profile`。

### 程序提示 `error while loading shared libraries`

这是运行时链接器没有找到 OpenTenBase 动态库。检查并保留原有库路径：

```bash
export LD_LIBRARY_PATH="$PG_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ldd "$PG_HOME/bin/psql" | grep 'not found'
```

如果 `ldd` 仍显示缺失库，先安装对应系统依赖；不要从不明来源复制同名 `.so` 文件。

### 安装末尾显示成功，但节点仍为 `Stopped`

不要只依据 `Installation completed successfully` 判断集群可用，应始终以 `status` 和 SQL 读写验证为准。先检查控制工具日志与节点启动日志中的真实错误：

```bash
grep -E 'Segmentation fault|Cannot allocate memory|syntax error|FATAL|ERROR' \
    "${INSTALL_PATH}"/logs/opentenbase_ctl_*.log
find /data/opentenbase/run/instance -name 'pg_ctl_start.log' -exec tail -n 30 {} \;
free -h
```

如果日志包含 `could not map anonymous shared memory: Cannot allocate memory`，说明当前环境无法满足初始化所需的共享内存，应先把主机资源提升到系统要求，再删除失败实例并重新安装。不要继续对不完整的数据目录执行 `start`。

如果日志中的路径被 `Warning: Permanently added ... to the list of known hosts` 截断，说明 SSH 主机指纹尚未正确写入运行 `opentenbase_ctl` 用户的 `~/.ssh/known_hosts`。按最小配置示例中的 SSH 检查处理后重新安装。

### 软件仓库失效、缺少头文件或 `zstd library not found`

Ubuntu 的新环境应先执行 `apt-get update`。`opentenbase_ctl` 是 C++ 程序，因此仅安装 `gcc` 不够，还需要 `g++`；出现 `curl/curl.h: No such file or directory` 时，确认已安装 `libcurl4-openssl-dev`（Red Hat 系为 `libcurl-devel`）。

CentOS 7 出现 `Cannot find a valid baseurl for repo: centos-sclo-rh` 时，按依赖章节引用的官方 DevEnv 示例修复已失效的 CentOS、SCL 和 EPEL 源。不要反复执行 `yum install`，也不要禁用证书或签名检查。

出现下面的错误，说明只安装编译器和常规开发包还不够：

```text
checking for ZSTD_compress in -lzstd... no
configure: error: zstd library not found.
```

在 CentOS 7 上确认 `libzstd-devel`、`libzstd-static`、`lz4-devel`、`lz4-static` 已安装，并检查 configure 所需的静态库路径：

```bash
ls -l /usr/local/lib/libzstd.a /usr/local/lib/liblz4.a
```

如果文件不存在，按依赖章节创建指向系统静态库的软链接后重新执行 `configure`。同时执行 `gcc --version`，确认当前 shell 已加载 SCL 的 GCC 7 环境。

### `opentenbase_ctl` 无输出或长时间等待

控制工具会在当前工作目录下创建并写入 `logs/`。如果从 `/` 等不可写目录运行，命令可能长时间没有可见输出。切换到安装目录，确认日志目录可写，再显式传入配置文件：

```bash
cd "${INSTALL_PATH}"
mkdir -p logs
test -w logs
"${PG_HOME}/bin/opentenbase_ctl" status -c opentenbase_config.ini
```

仍无结果时，检查 `logs/opentenbase_ctl_*.log`，并确认没有其他安装或状态命令正在占用同一配置。控制工具会在日志中遮盖 SSH 密码，但配置文件本身仍含明文密码，必须保持 `chmod 600`。

### `Failed to parse configuration file`

优先检查以下项目：

- `-c` 后的配置文件确实存在，且使用的是当前用户可读的绝对路径或正确的相对路径。
- `[coordinators]` 和 `[datanodes]` 的 `conf` 指向一个已存在的文件；没有自定义 GUC 时也应先 `touch postgres.conf`。
- 不需要从节点时直接省略 `slave` 行，不要保留只有空格的值。
- `nodes-per-server` 为 1 到 5 的整数，`ssh-port` 为实际 SSH 端口。

### 安装包不存在或无法提取版本号

```bash
grep '^package=' opentenbase_config.ini
PACKAGE_ARCH=$(uname -m)
PACKAGE_PATH="/data/opentenbase/install/opentenbase-5.21.8-i.${PACKAGE_ARCH}.tar.gz"
tar -tzf "${PACKAGE_PATH}" >/dev/null
```

`package` 应指向可读的 `.tar.gz` 文件，而不是目录；文件名需要采用 `名称-主版本.次版本...` 的形式，以便工具提取版本号。

### SSH 认证失败、连接超时或端口被拒绝

从运行 `opentenbase_ctl` 的主机逐台测试配置中的账号和端口：

```bash
for host in localhost 127.0.0.1; do
    ssh -p 22 "opentenbase@${host}" 'id && printf "ssh-ok\n"'
done
```

确认 `sshd` 已启动、`ssh-user` 的主目录与 `~/.ssh` 可写、主机指纹已经核验、密码与配置一致，并检查安全组和防火墙。配置文件含明文密码，只应在受信任的测试环境中使用，并应执行 `chmod 600 opentenbase_config.ini`。

### 节点显示 `Unknown`，或客户端连接被拒绝

先重新执行带相同 `-c` 参数的 `status`，再核对实际监听端口：

```bash
"${PG_HOME}/bin/opentenbase_ctl" status -c opentenbase_config.ini
ss -lntp
```

数据库端口会根据各主机的占用情况动态分配，SSH 端口也不是 CN 端口。防火墙和客户端命令必须使用本次状态输出中的实际端口。

### 文档中的路径与本机不一致

源码安装目录、部署包目录，以及 `opentenbase_ctl` 分发后的版本目录是不同路径。不要混用 `/home/opentenbase` 与本指南创建的 `/data/opentenbase`。可通过 `readlink -f "$PG_HOME"` 和 `status` 输出确认当前二进制及节点安装目录。

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
[版本历史](HISTORY)
