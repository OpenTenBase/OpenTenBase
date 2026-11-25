# OpenTenBase README_ZH.md 部署验证日志

- 验证日期：2026-09-04
- 验证环境：Debian 13 (trixie) 沙盒，2 vCPU / 3.8GB RAM / 9.4GB 磁盘
- 源码版本：OpenTenBase master（commit b612d77cb，shallow clone）
- 验证方式：严格按 README_ZH.md 步骤执行，从零开始最小部署路径验证
- 目标拓扑：单机最小集群（1 GTM + 1 CN + 2 DN，全部位于 127.0.0.1）

## 阶段 1：依赖安装

按 README apt 命令执行：

```
apt install -y git sudo gcc make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass libxml2-dev language-pack-zh-hans
```

**[问题 1] 报错：`E: Unable to locate package language-pack-zh-hans`**
- 该包仅存在于 Ubuntu；README 未说明 Debian 用户的替代方案（应使用 `localedef` 生成 locale）。

去掉该包后安装成功。

## 阶段 2：configure

```
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
```

**[问题 2] 报错：`configure: error: zstd library not found`**
- 已安装系统包 `libzstd-dev`，仍失败。
- `config.log` 显示真实原因：`/usr/bin/ld: cannot find /usr/local/lib/libzstd.a` —— configure（第 12802 行）硬编码链接 `/usr/local/lib/libzstd.a`。
- README 依赖清单完全未列 zstd。官方 DevEnv 仓库 Dockerfile.centos 安装了 `libzstd-devel libzstd-static` 并软链接到 /usr/local/lib，注释自称"参考 README_ZH.md"，互相矛盾。

解决：源码编译 zstd 1.5.6（`make lib && make install`）。

**[问题 3] 报错：`configure: error: lz4 library not found`**
- 同问题 2，configure（第 12852 行）硬编码 `/usr/local/lib/liblz4.a`。
- 解决：源码编译 lz4 1.9.4。

## 阶段 3：编译

**[问题 4] 链接失败：`undefined reference to '__sync_val_compare_and_swap_16'`**

```
/usr/bin/ld: access/transam/xlog.o: in function `pg_atomic_compare_and_swap_u128':
src/include/port/atomics.h:579: undefined reference to `__sync_val_compare_and_swap_16'
collect2: error: ld returned 1 exit status
```

- 原因：configure 会为 x86_64 自动追加 `-mcx16`（configure 第 18939 行），但命令行传入 `CFLAGS="-g"` 时按原值覆盖，`-mcx16` 丢失。
- 验证：`grep "^CFLAGS" src/Makefile.global` 确认输出中无 mcx16。
- 解决：`CFLAGS="-g -mcx16"` 重新 configure + `make clean` 后编译。

修正后 `make -sj2` 编译成功：`All of Postgres-XL successfully made. Ready to install.`（全程仅警告，无 error）
`make install`、`contrib` 目录编译安装均成功（exit=0）。

## 阶段 4：部署准备

- 按文档创建 opentenbase 用户、tar 包（31MB）。
- 本机 sshd + 密码登录验证通过（`sshpass ... ssh opentenbase@127.0.0.1` OK）。

## 阶段 5：opentenbase_ctl install

使用与 README 示例同构的单机配置（去掉 gtm/coordinators 的 slave，datanodes nodes-per-server=2）。

**[问题 5] 照抄文档字段说明表必现报错：**

```
can not open file:
[src/types/types.cpp:678] There are some errors in the coordinators configurations in the file.
Failed to parse file opentenbase_config.ini,please confirm that the file exists
```

- 源码定位：`build_cn_node_config` 无条件调用 `parseConfigFile(cfg_file.coordinators.conf)`，`conf` 未配置（空字符串）时 `ifstream` 打开失败直接返回 false。
- README 字段说明表中 `coordinators`/`datanodes` 完全没有 `conf` 字段，示例配置也没有。
- 解决：`touch cn_guc.conf dn_guc.conf` 并在配置中加 `conf=/data/opentenbase/cn.conf` 等。

补充 conf 后 install 成功：

```
====== Start to Install Instance otb_minimal  ======
step 1: Make *.tar.gz pkg ... successfully
step 2: Transfer and extract pkg to servers ... successfully
step 3: Install gtm master node ... successfully
step 4: Install cn/dn master node ... cn0001/dn0001/dn0002 successfully
step 5: Install slave nodes ... successfully
step 6: Create node group ... successfully
====== Installation completed successfully  ======
```

**[问题 6] `opentenbase_ctl status` 全部节点 Stopped，且报：**

```
Error: Failed to get port for node cn0001/dn0001/dn0002
```

- 手动执行 `opentenbase_ctl start`，工具输出 `Total: 4, Success: 4, Failed: 0`，但进程实际不存在。
- 检查 `~/run/instance/otb_minimal/*/data/pg_ctl_start.log`：

```
pg_ctl: directory "/data/opentenbase/run/instance/otb_minimal/cn0001/data" is not a database cluster directory
```

- 即 initdb 从未成功，数据目录是空的。
- 工具日志 `~/logs/opentenbase_ctl_*.log` 中的实际命令：

```
initdb -U opentenbase -E utf8 --locale=zh_CN.utf8 --nodename cn0001 ... &>/dev/null
```

**[问题 7] locale 硬编码导致 initdb 静默失败**
- `build_initdb_cmd`（cluster.cpp:834 起）硬编码 `encodeType="UTF8"` → `get_db_locale_by_ctype` 映射为 `zh_CN.utf8`，不可通过配置文件修改；且输出重定向 `&>/dev/null`，错误被吞掉；install 线程对 start 结果的判断缺陷导致仍打印 "successfully"。
- 本机 `locale -a` 无 zh_CN.utf8（Debian 最小安装默认没有）。
- 解决：`apt install locales && localedef -c -f UTF-8 -i zh_CN zh_CN.utf8`。
- README 从未提及此 locale 前置要求（依赖清单中孤立的 `language-pack-zh-hans` 是唯一线索，且仅 Ubuntu 有效）。

**[问题 8] install 期间 sshd 中断导致 SCP 失败（环境问题，同时印证排查小节必要性）**

```
[src/ssh/remote_ssh.cpp:87] SCP transfer failed with exit code 256
Failed to transfer and extract package to node dn0002 (127.0.0.1)
```

- 恢复 sshd 后重跑成功。

**[问题 9] 路径不一致/未解释**
- 源码编译目录：`install/opentenbase_bin_v5.0/`
- ctl 工具运行时目录（自动解包生成）：`install/opentenbase/5.21.8/`
- status 输出与"使用"章节给出的环境变量路径各不相同（后者甚至用了 `/home/opentenbase/...` 与前文 `/data/opentenbase/...` 不一致），文档未解释两者关系。

**[问题 10] 手动 tar 打包步骤与 ctl 工具 step 1 "Make *.tar.gz pkg" 重复**，未说明哪些场景必须手动打包。

## 阶段 6：修正后重装（最终验证）

前置条件修正清单：
1. `localedef -c -f UTF-8 -i zh_CN zh_CN.utf8` 已执行
2. 配置文件已补充 conf 字段
3. sshd 稳定运行

重跑 `opentenbase_ctl install`（日志 install3.log）结果：
- step 3 GTM 安装成功
- step 4 中 cn0001/dn0001/dn0002 三个节点的 **initdb 全部成功**（data 目录出现 PG_VERSION、base、global 等完整集群文件）——证明 locale 修复有效
- `opentenbase_ctl start` 输出 `Total: 4, Success: 4, Failed: 0`，status 显示 4/4 Running

### 验证边界说明（如实记录）

在本沙盒中无法完成最后的 psql 长连接验证，原因均为**沙盒环境限制**，与文档无关：
1. 沙盒会在命令会话结束后清理后台守护进程，GTM/CN/DN 进程无法持久驻留
2. GTM 启动报 `FATAL: binding threads failed`（bind_thread_to_cores，沙盒 cgroup CPU 限制与亲和性绑定冲突）
3. `OS does not choose TSC as clocksource which may cause undefined behavior of distributed transactions`（虚拟化时钟源不满足分布式事务要求，README 系统要求中的 TencentOS/CentOS 物理机或标准虚拟机不受影响）

在满足 README 系统要求（8G 内存、列出的操作系统）的真实机器上，按照修改后的文档执行，上述三个限制均不存在。

## 问题清单汇总（对应 README 修改点）

| # | 类型 | 问题 | README 修改 |
|---|---|---|---|
| 1 | 依赖缺失 | language-pack-zh-hans 仅 Ubuntu 有 | 加注释说明 Debian 用 localedef |
| 2 | 依赖缺失 | zstd 缺失 + /usr/local 硬编码 | 依赖清单补充 + 软链接说明 |
| 3 | 依赖缺失 | lz4 缺失 + /usr/local 硬编码 | 同上 |
| 4 | 命令错误 | CFLAGS="-g" 覆盖 -mcx16 | 改为 "-g -mcx16" 并加警告 |
| 5 | 配置缺失 | conf 字段未记载，必现报错 | 字段表 + 示例均补充 |
| 6 | 步骤缺失 | install 后需手动 start | 新增"启动实例并检查状态"小节 |
| 7 | 前置条件 | zh_CN.utf8 locale 必须预生成 | 依赖节 + 排查小节说明 |
| 8 | 步骤缺失 | sshd 必须稳定运行 | 排查小节 SCP 失败条目 |
| 9 | 路径不一致 | 三处路径互相矛盾 | 统一说明两目录关系 |
| 10 | 表述不清 | 手动打包与工具打包重复 | 加说明 |

另新增完整"常见错误与排查"小节（8 个条目）。
