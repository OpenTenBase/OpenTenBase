# OpenTenBase 最小部署路径验证记录

本记录针对 `README_ZH.md` 中的源码编译、依赖安装、`opentenbase_ctl` 部署和 CN 连接路径。它区分了已执行的静态验证与因本机环境限制未执行的步骤，避免将源码检查或未完成的 Docker 流程表述为部署成功。

## 验证范围和环境

- 验证日期：2026-07-12
- 验证终端：Windows PowerShell（Git Bash）
- OpenTenBase 提交：`6ec38ff9`（`deploy-docu` 分支，基于 `master`）
- 参考资料：仓库 `README_ZH.md`、`contrib/opentenbase_ctl` 全部源码与配置模板、`OpenTenBase-Intro.pptx`

本机为 Windows 环境，未安装 Docker CLI 和 Linux 编译工具链，无法执行实际的编译、Docker 启动和集群部署操作。验证工作聚焦于**源码与文档的交叉核对**，确保 README 中的命令、配置项与实际代码一致。

## 已执行的静态验证

### 1. 编译命令验证

| 检查项 | 源码/文件依据 | 结果 |
| --- | --- | --- |
| `configure` 参数 `--with-libxml` | apt 依赖包含 `libxml2-dev`，yum 包含 `libxml2-devel` | 一致 |
| `configure` 参数 `--with-openssl` | apt 包含 `libssl-dev`，yum 包含 `openssl-devel` | 一致 |
| `configure` 参数 `--with-ossp-uuid` | apt 包含 `libossp-uuid-dev`，yum 包含 `uuid-devel` | 一致 |
| `chmod +x configure*` | 源码根目录存在 `configure` 可执行脚本 | 正确 |
| `chmod +x contrib/pgxc_ctl/make_signature` | 文件存在，为 Bash 脚本，生成 `signature.h` 和 `pgxc_ctl_bash.c` | 正确 |
| `make -j"$(nproc)"` | `nproc` 来自 coreutils，最小化系统可能未安装 | 潜在问题 |

### 2. opentenbase_ctl 命令验证

| 检查项 | 源码文件 | 结果 |
| --- | --- | --- |
| `install` 子命令 | `command.cpp:40-43` | 注册，支持 `-c/--config` |
| `delete` 子命令 | `command.cpp:46-48` | 注册，支持 `-c/--config` |
| `start` 子命令 | `command.cpp:51-61` | 注册，支持 `-c/--config` 和 `-n/--node` |
| `stop` 子命令 | `command.cpp:65-74` | 注册，支持 `-c/--config` 和 `-n/--node` |
| `status` 子命令 | `command.cpp:77-84` | 注册，支持 `-c/--config` 和 `-n/--node` |
| `scp` 子命令 | `command.cpp:87-98` | 注册 |
| `shell` 子命令 | `command.cpp:101-109` | 注册 |
| `sql` 子命令 | `command.cpp:112-124` | 注册 |
| `guc` 子命令 | `command.cpp:127-137` | 注册 |
| `prepare config` | 所有 `command.cpp` 注册项 | **不存在**，当前版本未实现此子命令 |

### 3. 配置文件验证

| 检查项 | 源码文件 | 结果 |
| --- | --- | --- |
| `config.ini` 模板存在 | `contrib/opentenbase_ctl/config/config.ini` | 存在，含 `instance`、`gtm`、`coordinators`、`datanodes`、`server`、`log` 段 |
| 模板与 README 示例包路径不一致 | 模板用 `opentenbase-3.16.9.301-i.x86_64.tar.gz`，示例用 `opentenbase-5.21.8-i.x86_64.tar.gz` | **不一致**，初学者容易困惑 |
| 模板与 README 实例名不一致 | 模板用 `test_cluster06`，示例用 `opentenbase01` | **不一致** |
| `conf` 字段支持 | `config.cpp:139-140,151-152` | 源码支持，但 README 配置表格未文档化 |
| `nodes-per-server` 最大值 | `types.cpp` | 默认 1，最大 5 |
| `ssh-port` 默认值 | README 示例用 `36000`，模板用 `36000` | 一致，但非标准 SSH 端口，需注意 |
| 端口自动分配逻辑 | `utils.cpp:68-96` | 从 11000 开始，每节点占用连续 3 个端口（node_port, pooler_port, forward_port） |

### 4. 环境变量与路径验证

| 检查项 | 源码文件 | 结果 |
| --- | --- | --- |
| `PG_HOME` 定义 | `README_ZH.md:98` | `PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0` |
| `LD_LIBRARY_PATH` 拼接 | `utils.cpp:278-281` (`buid_ld_library_path_str`) | 格式为 `export LD_LIBRARY_PATH=<binDir>/lib && export PATH=<binDir>/bin:${PATH}` |
| 默认安装目录 | `types.h:49` (`DEFAULT_INSTALL_DIR`) | `/usr/local/install/opentenbase`，与 README 中的 `${INSTALL_PATH}/opentenbase_bin_v5.0` **不一致** |
| `DEFAULT_USER_OF_INITDB` | `types.h:47` | `opentenbase`，与 README 一致 |
| `DEFAULT_DB` | `types.h:48` | `postgres`，与 README 一致 |
| 环境变量写入 `~/.bashrc` | `command.cpp:166-189` | 源码有 `setEnvironmentVariableInBashrc` 函数，但未在 README 中说明 |

### 5. 部署流程验证（install_distributed_instance）

| 步骤 | 源码文件 `cluster.cpp` | README 描述 |
| --- | --- | --- |
| Step 1: 制作 tar.gz | `pre_process_pkg()`:1005-1021 | 正确，先解 rpm 再打包 tar.gz |
| Step 2: 分发解压 | `pre_install_command()`:1023-1029 | 正确，仅对唯一 IP 分发 |
| Step 3: 安装 GTM 主节点 | `install_nodes_parallel(gtm_master)`:1032-1041 | 正确 |
| Step 4: 安装 CN/DN 主节点 | `install_nodes_parallel(cn_master, dn_master)`:1043-1051 | 正确 |
| Step 5: 安装备节点 | `install_nodes_parallel(cn_slave, dn_slave, gtm_slave)`:1054-1062 | 正确 |
| Step 6: 创建节点组 | `create_nodes_group()`:1065-1072 | 正确 |

### 6. 集中式实例验证（install_centralized_instance）

| 步骤 | 源码文件 `cluster.cpp` | README 描述 |
| --- | --- | --- |
| Step 1-2: 制作和分发 | 同分布式 | 正确 |
| Step 3: 安装 DN 主节点 | `install_nodes_parallel(dn_master)`:1106-1114 | 正确 |
| Step 4: 安装 DN 备节点 | `install_nodes_parallel(dn_slave)`:1117-1125 | 正确 |
| Step 5: 创建节点组 | `create_nodes_group()`:1128-1135 | 正确 |

## 发现的问题与已实施的修复

| 编号 | 问题 | 严重程度 | 影响范围 | 修复措施 |
| --- | --- | --- | --- | --- |
| 1 | SELinux 配置命令写成 `set SELINUX=disabled`，应为 `SELINUX=disabled` | 中 | 初学者照抄命令会导致 `/etc/selinux/config` 文件格式错误 | 已修正 README |
| 2 | 配置表格缺少 `conf` 字段说明 | 低 | 用户不知道可以为 CN/DN 指定自定义 postgresql.conf | 已在 coordinators 和 datanodes 配置表中补充 |
| 3 | 环境变量仅通过 `export` 临时设置，未说明持久化 | 中 | 新开终端后 PATH/LD_LIBRARY_PATH 丢失，opentenbase_ctl 找不到 | 已在环境变量设置处补充写入 `~/.bashrc` 的注释 |
| 4 | config.ini 模板（`test_cluster06`/`v3.16.9.301`）与 README 示例（`opentenbase01`/`v5.21.8`）不一致 | 低 | 初学者困惑为何模板与示例不同 | 已记录，建议后续统一版本号 |
| 5 | yum 依赖列了 `postgresql-devel` 但源码编译不需要它（源码自带 libpq）；同时 apt 缺少 `libcurl4-openssl-dev` | 低 | 多装无用包或少装导致 configure 缺项 | 已记录 |
| 6 | README 依赖列表与 `contrib/opentenbase_ctl/scripts/install.sh` 中的依赖列表不完全一致 | 低 | install.sh 还安装了额外依赖（如 libcurl 等） | 已记录，建议同步 |
| 7 | `nodes-per-server` 的硬上限为 5，README 未提及 | 低 | 配置超过 5 会导致部署失败 | 已记录 |

## 在 Linux/Docker 环境中的复验步骤

以下步骤尚未在本机执行，供具备 Linux、Docker 和网络条件的贡献者复验：

```bash
# 环境前置检查
docker --version
docker info

# 使用 OpenTenBase-DevEnv 快速搭建开发环境
git clone https://github.com/OpenTenBase/OpenTenBase-DevEnv.git
cd OpenTenBase-DevEnv
./otb-dev.sh build
./otb-dev.sh up

# 进入容器后编译
cd /data/opentenbase/
git clone https://github.com/OpenTenBase/OpenTenBase
export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/
cd ${SOURCECODE_PATH}
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make -j"$(nproc)"
make install
cd contrib
make -j"$(nproc)"
make install

# 设置环境变量
export PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
export PATH="$PATH:$PG_HOME/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"

# 准备安装包
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *

# 准备配置文件（单机集中式部署，无需多机SSH）
cd ${INSTALL_PATH}
cp ${SOURCECODE_PATH}/contrib/opentenbase_ctl/config/config.ini opentenbase_config.ini
# 编辑 opentenbase_config.ini：
#   type=centralized
#   package=<实际tar.gz路径>
#   [datanodes] master=127.0.0.1
#   [server] ssh-user=opentenbase, ssh-password=<密码>, ssh-port=22

# 安装
"${PG_HOME}/bin/opentenbase_ctl" install -c opentenbase_config.ini

# 验收
"${PG_HOME}/bin/opentenbase_ctl" status -c opentenbase_config.ini
# 按 status 输出连接 CN 后执行：
# psql -h <ip> -p <port> -U opentenbase postgres
# postgres=# select * from pgxc_node;
```

## 验收检查清单

验收时应保存以下输出到 PR 描述：

- [ ] 操作系统版本（`cat /etc/os-release`）
- [ ] Docker 版本（若使用容器：`docker --version`）
- [ ] 编译成功日志（`make install` 末尾输出）
- [ ] `"${PG_HOME}/bin/opentenbase_ctl" status -c opentenbase_config.ini` 的节点汇总
- [ ] psql 连接 CN 后 `select * from pgxc_node;` 的结果
- [ ] 基本 SQL 验证（CREATE TABLE / INSERT / SELECT）

## 结论

本机完成了 README 部署路径的全面静态核对，包括：
- 所有 `opentenbase_ctl` 子命令与实际源码注册项的交叉验证
- 配置模板字段与源码解析逻辑的一致性检查
- 部署步骤与实际 `install_distributed_instance`/`install_centralized_instance` 代码流的对比
- 环境变量/路径设定与 `utils.cpp` 中 `buid_ld_library_path_str` 的匹配

发现并修复了 3 处文档问题（SELinux 语法、conf 字段缺失、环境变量持久化），记录了 4 处需要后续关注的不一致项。由于本机为 Windows 且缺少 Docker CLI，未执行真实的集群部署。所有复验步骤已在本记录中列出，供具备 Linux 环境的贡献者执行。

本次文档生成和核验方式见 [AI 使用策略自我报告](AI_USAGE_REPORT_ZH.md)。
