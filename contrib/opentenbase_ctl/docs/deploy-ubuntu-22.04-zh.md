# 在全新 Ubuntu 22.04 上部署 OpenTenBase v5.0

本文记录在**全新安装的 Ubuntu 22.04** 上从源码编译、打包到拉起单机集群的完整流程，包含官方 README 未覆盖的 3 处编译期兼容问题和 3 处安装后必查项。

对应 issue：[Complete deployment guide for fresh Ubuntu/CentOS servers (#177)](https://github.com/OpenTenBase/OpenTenBase/issues/177)

> **说明**：文中命令均在下列环境实际执行过（2026-11-06），输出为实测粘贴；凡是未实测的推断，会显式标注「预期」。

## 0. 验证环境

| 项目 | 值 |
|---|---|
| 发行版 | Ubuntu 22.04.4 LTS (x86_64) |
| 规格 | 4 vCPU / 7.7 GiB 内存 / 40 GiB 磁盘 |
| 工具链 | gcc 11.4.0、bison 3.8.2、GNU Make 4.3 |
| 源码 | OpenTenBase master（v5.0，`ac54d24`）+ 本文第 2 节的兼容补丁 |
| 部署形态 | 单机分布式：1 GTM + 1 CN + 2 DN，无备节点 |
| 运行用户 | `opentenbase`（非 root；PostgreSQL 内核拒绝以 root 启动，`opentenbase_ctl` 也要求非 root 账户） |
| 安装前缀 | `/data/opentenbase/install/opentenbase_bin_v5.0` |

下文所有路径都以该前缀为例，换成自己的路径时注意同步修改 `PG_HOME`。

## 1. 安装依赖

数据库内核和运维工具 `opentenbase_ctl` 的依赖是两套，前者编译通过不代表后者能编译，所以一次装全：

```bash
sudo apt update
sudo apt install -y \
  git curl sudo build-essential bison flex \
  libreadline-dev zlib1g-dev libssl-dev \
  libxml2-dev libxslt1-dev libperl-dev python3-dev \
  libicu-dev libpam0g-dev libevent-dev libyaml-dev \
  liblz4-dev libzstd-dev \
  libssh2-1-dev sshpass libcurl4-openssl-dev \
  libcli11-dev libpqxx-dev
```

各依赖的归属（装错了会以为「编译都过了为什么工具链还是缺库」）：

| 依赖包 | 用途 | 缺失时 |
|---|---|---|
| `build-essential` / `bison` / `flex` | 内核 C 编译与语法分析器生成 | `configure` 或 `make` 直接失败 |
| `libreadline-dev` `zlib1g-dev` `libssl-dev` | psql 行编辑、压缩、SSL | 内核核心功能缺失 |
| `libxml2-dev` `libxslt1-dev` | `--with-libxml` 依赖 | `configure` 报 `libxml2 not found` |
| `libicu-dev` | 排序/大小写规则 | 字符串排序相关编译失败 |
| `liblz4-dev` `libzstd-dev` | 压缩算法 | `configure` 报 `zstd library not found` |
| `libssh2-1-dev` `sshpass` `libcurl4-openssl-dev` `libcli11-dev` `libpqxx-dev` | **仅 `opentenbase_ctl` 需要** | `contrib` 阶段报找不到 `CLI11.hpp` / `pqxx` |

> `libossp-uuid-dev`（提供 `uuid-ossp` 扩展）在 Ubuntu 22.04 仓库中存在但**默认不装**。如果不需要该扩展，`configure` 就不要加 `--with-ossp-uuid`，否则链接阶段报找不到 OSSP 库。

## 2. 拉取源码后必须处理的三处兼容问题

```bash
su - opentenbase
mkdir -p /data
git clone https://github.com/OpenTenBase/OpenTenBase.git /data/opentenbase/OpenTenBase
cd /data/opentenbase/OpenTenBase
```

### 2.1 悬空符号链接（新克隆最容易卡在这里）

`src/include/opentenbase_ora/gram_ora.h` 在仓库里是一个**指向原作者机器绝对路径**的符号链接：

```text
src/include/opentenbase_ora/gram_ora.h -> /data/workspace/TBase-V3.0/src/backend/opentenbase_ora/gram_ora.h
```

只要你的源码不在 `/data/workspace/TBase-V3.0`，这个链接就是悬空的，而 `parser_ora.c`、`keywords_ora.c`、`gram_ora.y` 都会 `#include "opentenbase_ora/gram_ora.h"`，编译会停在：

```text
fatal error: opentenbase_ora/gram_ora.h: No such file or directory
```

处理方式是按实际路径重新指向（真身就在 `src/backend/opentenbase_ora/gram_ora.h`）：

```bash
ln -sfn "$(pwd)/src/backend/opentenbase_ora/gram_ora.h" src/include/opentenbase_ora/gram_ora.h
```

### 2.2 `configure` 把 zstd/lz4 写死成 `/usr/local/lib/*.a`

`configure` 中 zstd/lz4 的探测写死了静态库绝对路径，Ubuntu 上库装在 `/usr/lib/x86_64-linux-gnu`，于是检查必然失败：

```bash
# 把写死的静态库路径改为按 -l 链接
sed -i 's#/usr/local/lib/libzstd\.a#-lzstd#g; s#/usr/local/lib/liblz4\.a#-llz4#g' configure
sed -i 's#/usr/local/lib/libzstd\.a#-lzstd#g; s#/usr/local/lib/liblz4\.a#-llz4#g' configure.in
```

### 2.3 gcc 11 + ICU 的 `bool` 类型冲突

ICU 头文件会在编译单元中途引入 `<stdbool.h>`，使 `bool` 从 `char` 变成 `_Bool`，报类似：

```text
error: conflicting types for 'bool'
```

处理方式与上游 PG16 一致——在 `src/include/c.h` 提前引入 `<stdbool.h>`，让 `bool` 从头到尾只有一种定义：

```bash
python3 - <<'EOF'
p = 'src/include/c.h'
s = open(p).read()
anchor = '#ifndef bool\ntypedef char bool;\n#endif'
add = ('#if !defined(__bool_true_false_are_defined) && defined(__STDC_VERSION__) '
       '&& __STDC_VERSION__ >= 199901L\n#include <stdbool.h>\n#endif\n\n')
assert anchor in s and add not in s
open(p, 'w').write(s.replace(anchor, add + anchor, 1))
EOF
```

> 同样的修改也有人在 PR [#233](https://github.com/OpenTenBase/OpenTenBase/pull/233) 中提交，等它合入后本节可省略。

## 3. 编译

```bash
export INSTALL_PATH=/data/opentenbase/install
export PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0

cd /data/opentenbase/OpenTenBase
chmod +x configure*
./configure --prefix=${PG_HOME} \
            --enable-user-switch \
            --with-libxml \
            --disable-license \
            --with-openssl
make -sj"$(nproc)"
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib && make -sj"$(nproc)" && make install
```

`configure` 参数逐条说明：

| 参数 | 作用 | 是否需按环境替换 |
|---|---|---|
| `--prefix=${PG_HOME}` | 安装目录。**必须与后文 `PG_HOME` 完全一致**，`opentenbase_ctl status` 打印的路径模板与它无关（见 6.0） | 是，改成自己的路径 |
| `--enable-user-switch` | 允许 `opentenbase` 普通用户切换/提权执行，`opentenbase_ctl` 依赖此能力 | 否 |
| `--with-libxml` | 启用 XML 相关功能（`libxml2-dev`） | 否 |
| `--disable-license` | 关闭 license 校验，等价于 `-DNOLIC`，二者选一 | 否 |
| `--with-openssl` | 启用 SSL 连接 | 否 |
| ~~`--with-ossp-uuid`~~ | 仅 `uuid-ossp` 扩展需要，未装 `libossp-uuid-dev` 时不要加 | 视需求 |

编译完成后自检：

```bash
${PG_HOME}/bin/postgres --version
# 实测输出：postgres (PostgreSQL) 10.0 @ OpenTenBase_v5.0 OpenTenBase V5.21 2026-11-06 17:32:58
```

## 4. 打包与集群配置

`opentenbase_ctl` 的 `package` 字段只接受 **.tar.gz 全路径**（内部用 `cp` 处理，传目录会失败）：

```bash
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *
```

写 `~/opentenbase_config.ini`（下面是本文验证过的单机 1 GTM + 1 CN + 2 DN 配置）：

```ini
[instance]
name=opentenbase01                 # 集群名，后文 status 输出以此为准
type=distributed                   # 分布式模式，需要 gtm + coordinator + datanode
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

[gtm]
master=192.168.226.128             # GTM 所在 IP，单机就是本机
slave=

[coordinators]
master=192.168.226.128             # CN 所在 IP
slave=
nodes-per-server=1

[datanodes]
master=192.168.226.128,192.168.226.128   # 两个 IP 即两个 DN；写同一个 IP 就用不同端口
slave=
nodes-per-server=1

[server]
ssh-user=opentenbase               # 部署账户（需与运行账户一致）
ssh-password=otb@2026              # 该账户密码，opentenbase_ctl 通过 SSH 远程执行
ssh-port=22

[log]
level=INFO                         # 仅控制 opentenbase_ctl 自身日志，不影响节点日志
```

需要替换的字段：`name`、`package`、三个 `master` 的 IP、`ssh-user` / `ssh-password` / `ssh-port`。
`slave=` 留空即不部署备节点——本文验证的就是这种「1 GTM + 1 CN + 2 DN、无备节点」的形态。

`opentenbase_ctl` 通过 SSH 在各节点执行命令，所以**单机部署同样要配本机免密登录**，否则 `install` 会卡在密码输入：

```bash
mkdir -p ~/.ssh && chmod 700 ~/.ssh
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N "" -C "opentenbase@localhost"
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
ssh opentenbase@localhost echo "SSH OK"
```

## 5. 安装、启动与状态

```bash
export LD_LIBRARY_PATH=${PG_HOME}/lib
${PG_HOME}/bin/opentenbase_ctl install -c ~/opentenbase_config.ini   # 只做 initdb，不会启动节点
${PG_HOME}/bin/opentenbase_ctl start   -c ~/opentenbase_config.ini
${PG_HOME}/bin/opentenbase_ctl status  -c ~/opentenbase_config.ini
```

本环境的实测状态输出（4 个节点全部 Running 才算装好）：

```text
Instance name: opentenbase01
Version: 5.21.8

Node dn0002(192.168.226.128) is Running
Node dn0001(192.168.226.128) is Running
Node gtm0001(192.168.226.128) is Running
Node cn0001(192.168.226.128) is Running
[Result] Total: 4, Running: 4, Stopped: 0, Unknown: 0
```

## 6. 安装完成后必查的三项

### 6.0 `status` 打印的路径不能照抄

`status` 输出的最后两行是工具按模板打印的：

```text
Environment variable: export LD_LIBRARY_PATH=/home/opentenbase/install/opentenbase/5.21.8/lib ...
PSQL connection: psql -h 192.168.226.128 -p 11006 -U opentenbase postgres
```

其中 `opentenbase/5.21.8` 是模板里写死的版本目录，**不是**你 `--prefix` 指定的目录。照抄这一行会让 `LD_LIBRARY_PATH` 指向一个不存在的路径，`pg_ctl` 等工具随即报 `error while loading shared libraries`。请改用 `${PG_HOME}/lib` 与 `${PG_HOME}/bin`。

### 6.1 `pgxc_node.node_forward_port` 全为 0

这是本文环境里最隐蔽的一个坑：`opentenbase_ctl install` 不会写 `node_forward_port`，装完就是 0。此时单节点内查询正常，但一旦跨节点（`INSERT ... SELECT`、跨 DN 聚合）就会报：

```text
ERROR:  tcp fails to send data
```

并直接断开当前会话。检查方法（在 CN 上执行）：

```sql
SELECT node_name, node_port, node_forward_port FROM pgxc_node ORDER BY node_name;
```

正常应当满足 `node_forward_port = node_port + 2`。若为 0，在每个 CN / DN 节点上执行：

```sql
UPDATE pgxc_node SET node_forward_port = node_port + 2;
SELECT pgxc_pool_reload();
```

> 重新执行 `opentenbase_ctl install` 后需要重做这一步。

### 6.2 CN 侧并行查询的 DSM 问题

在本环境执行跨节点并行查询时出现过 DSM（dynamic shared memory）相关报错（完整报错文本未留存）。处置方式是在三个节点（CN 与各 DN）的 `postgresql.conf` 中关闭 gather 并行：

```ini
max_parallel_workers_per_gather = 0
```

> 这是**规避手段，不是修复**。面向生产的部署请自行评估并行度，或跟踪上游修复后再打开。

## 7. 验收清单

以下命令在本环境逐条执行过，输出为实测粘贴。

```sql
-- 1) 建一张按 id 分片的表
CREATE TABLE doc_check(id int, v text) DISTRIBUTE BY SHARD(id);
-- CREATE TABLE

-- 2) 跨节点写入 1000 行（走的是 CN → 两个 DN 的下发）
INSERT INTO doc_check SELECT g, 'v'||g FROM generate_series(1,1000) g;
-- INSERT 0 1000

-- 3) 总量
SELECT count(*) FROM doc_check;
-- 1000

-- 4) 分别到两个 DN 上确认数据确实分片落盘
EXECUTE DIRECT ON (dn0001) 'SELECT count(*) FROM doc_check';   -- 492
EXECUTE DIRECT ON (dn0002) 'SELECT count(*) FROM doc_check';   -- 508

-- 5) 事务可回滚（回滚后总数应仍是 1000）
BEGIN; INSERT INTO doc_check VALUES (2001,'x'); ROLLBACK;
SELECT count(*) FROM doc_check;
-- 1000

-- 6) 清理
DROP TABLE doc_check;
```

| 检查项 | 命令 | 本环境实测 |
|---|---|---|
| 节点全部在线 | `opentenbase_ctl status -c ~/opentenbase_config.ini` | `Total: 4, Running: 4, Stopped: 0` |
| 能连上 CN | `psql -h <CN_IP> -p 11006 -U opentenbase postgres -c "select 1;"` | 返回 1 行 |
| 分布式表可建 | 见上面第 1 条 | `CREATE TABLE` |
| 跨节点写入 | 见上面第 2 条 | `INSERT 0 1000` |
| 数据确实分片 | 见上面第 4 条 | dn0001 = 492，dn0002 = 508 |
| 事务回滚 | 见上面第 5 条 | 回滚后仍 1000 行 |

> **关于 `DISTRIBUTE BY`**：默认 `configure` 编译出的版本只接受 `SHARD` 和 `REPLICATION`。写 `DISTRIBUTE BY HASH(id)` / `MODULO(id)` 会直接报 `Cannot support distribute type: Hash`（判定见 `src/backend/tcop/utility.c` 中 `#ifndef ENABLE_ALL_TABLE_TYPE` 分支）。需要这两种分布类型时，`configure` 要加 `--enable-alltype-distri`（相关讨论见 issue [#506](https://github.com/OpenTenBase/OpenTenBase/issues/506)）。

## 8. 常见失败与对应处理

| 现象 | 原因 | 处理 |
|---|---|---|
| `configure` 报 `zstd library not found` | 见 2.2，静态库路径写死 | 改 `configure` 后重跑 |
| `error: conflicting types for 'bool'` | 见 2.3 | 补 `c.h` 后 `make clean` 重编 |
| `fatal error: opentenbase_ora/gram_ora.h: No such file or directory` | 见 2.1 悬空链接 | 重新指向 |
| `contrib` 编译报找不到 `CLI11.hpp` | 未装 `libcli11-dev` | `apt install libcli11-dev` |
| `opentenbase_ctl` 报 `Failed to extract version from package name` | `install` 未带 `-c`，或 `package` 不是 `.tar.gz` 全路径 | 见第 4 节 |
| `psql: error while loading shared libraries` | 抄了 `status` 的模板路径 | 用 `${PG_HOME}/lib` |
| 跨节点查询报 `tcp fails to send data` | 见 6.1 | 修 `node_forward_port` 并 `pgxc_pool_reload()` |
| `CREATE TABLE ... DISTRIBUTE BY HASH(id)` 报 `Cannot support distribute type: Hash` | 默认构建未开启 HASH/MODULO 分布（`--enable-alltype-distri`） | 改用 `DISTRIBUTE BY SHARD(id)`，或重新 `configure` 加该选项 |
| GTM 报 `FATAL: binding threads failed` | 核数过少时线程绑核失败（README 已收录） | 用 ≥ 4 核机器，或注入 `noaffinity.so` |
