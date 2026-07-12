# OpenTenBase 最小部署路径验证记录

本记录针对 `README_ZH.md` 中的部署流程，包含**静态源码核对**和 **WSL 实际部署**两个阶段。

## 验证范围和环境

- 验证日期：2026-07-12
- 静态验证终端：Windows PowerShell（Git Bash）
- 实际部署终端：WSL2 Ubuntu 22.04.5 LTS (x86_64, 31GB RAM)
- OpenTenBase 提交：`6ec38ff9`（`deploy-docu` 分支）
- 参考资料：`README_ZH.md`、`contrib/opentenbase_ctl` 全部源码与配置模板

## 第一阶段：静态源码核对

在 WSL 部署之前，对 README_ZH.md 与源码进行了交叉验证：

| 检查项 | 源码文件 | 结果 |
| --- | --- | --- |
| CLI 子命令列表 | `command.cpp` | install/delete/start/stop/status/scp/shell/sql/guc 均注册 |
| `-c/--config` 参数 | `command.cpp` | 所有部署命令均支持 |
| `conf` 字段解析 | `config.cpp:139-152` | 源码支持，READM 表格缺文档（已修复） |
| `nodes-per-server` 上限 | `types.cpp` | 默认 1，最大 5 |
| 部署流程 6 步骤 | `cluster.cpp:1009-1077` | 与实际代码一致 |
| LD_LIBRARY_PATH 拼接 | `utils.cpp:278-281` | 格式正确 |
| SELinux 配置语法 | README_ZH.md:112 | 原文 `set SELINUX=disabled` 错误（已修复） |

## 第二阶段：WSL 实际部署

### 部署环境准备

```
OS:       Ubuntu 22.04.5 LTS (WSL2)
Kernel:   Linux 5.15.x
CPU:      x86_64
RAM:      31 GB
Disk:     231 GB free
User:     opentenbase (uid 1000, sudo group)
SSH:      OpenSSH on port 22
```

### 部署过程记录

#### 尝试 1：源码编译 → 失败（GCC 11 兼容性）

使用 README 中的源码编译步骤：
```bash
./configure --prefix=... --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make -j"$(nproc)"
```

**错误**：
1. CRLF 换行符导致 Perl 脚本 `generate-lwlocknames.pl` 解析失败
2. sed 脚本 `Gen_dummy_probes.sed` 因 CRLF 报错
3. GCC 11 的 `_Bool`/`bool` 类型冲突导致 `htup_details.h:938` 编译失败

**结论**：该代码库（Postgres-XL 10beta3）与 GCC 11 不兼容，需要 GCC 7-9 或额外补丁。Ubuntu 22.04 不提供 GCC 7 包。

**解决方案**：改用 README 中提到的预编译包方案。

#### 尝试 2：预编译包部署 → 成功（经多轮调试）

**下载**：
```bash
curl -L -o opentenbase-5.21.8-i.x86_64.tar.gz \
  https://opentenbase-1302252972.cos.ap-nanjing.myqcloud.com/opentenbase-5.21.8-i.x86_64.tar.gz
curl -L -o opentenbase_ctl \
  https://opentenbase-1302252972.cos.ap-nanjing.myqcloud.com/opentenbase_ctl
```

**遇到的问题及解决**：

| 序号 | 问题 | 原因 | 解决方案 |
| --- | --- | --- | --- |
| 1 | `opentenbase_ctl: command not found` | 预编译包不含 opentenbase_ctl | 单独下载并复制到 bin/ |
| 2 | `libssl.so.10: cannot open shared object file` | 预编译二进制依赖 OpenSSL 1.0.x，Ubuntu 22.04 只有 3.x | 编译安装 OpenSSL 1.0.2u，创建 symlink |
| 3 | `libreadline.so.6: cannot open shared object file` | 预编译二进制依赖 readline 6，Ubuntu 22.04 只有 8 | symlink libreadline.so.8 → libreadline.so.6 |
| 4 | `Failed to parse configuration file .` | `datanodes.conf` 为空导致 `parseConfigFile("")` 失败 | 创建空的 postgres.conf 并在配置中指定 |
| 5 | SSH host key warning 污染命令 | "Warning: Permanently added..." 被插入 initdb 命令中间 | 配置 SSH `StrictHostKeyChecking no` + `LogLevel ERROR` |
| 6 | initdb 数据目录为空（仅 recovery.conf） | 单机部署时 master/slave 使用相同数据目录路径，slave 的 pg_basebackup 覆盖了 master 数据 | 设置 `slave=` 为空，仅部署单节点 |
| 7 | CREATE TABLE 报 "default group not defined" | opentenbase_ctl 创建的节点组不完整 | 手动执行 `CREATE NODE` + `CREATE DEFAULT node group` + `CREATE sharding group` + `clean sharding` |

#### 成功部署配置

```ini
[instance]
name=otb_mini
type=centralized
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

[datanodes]
master=127.0.0.1
nodes-per-server=1
conf=/data/opentenbase/install/postgres.conf

[server]
ssh-user=opentenbase
ssh-password=opentenbase123
ssh-port=22

[log]
level=DEBUG
```

**关键配置说明**：单机部署必须设置 `slave=` 为空，否则 master 和 slave 共享同一数据目录导致数据被覆盖。

#### 部署后手动修复步骤

```sql
-- 创建 PGXC 节点路由（集中式实例部署后需要手动执行）
CREATE NODE dn0001 WITH (TYPE='datanode', HOST='127.0.0.1', PORT=11000);

-- 创建默认节点组和分片组
CREATE DEFAULT node group default_group with (dn0001);
CREATE sharding group to group default_group;
CLEAN SHARDING;
```

### 最终验证结果

```sql
-- 版本
SELECT version();
-- PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: ac54d240f)

-- 节点信息
SELECT * FROM pgxc_node;
-- dn0001 | D | 11000 | 127.0.0.1

-- 基本 SQL 操作
CREATE TABLE deploy_test(id int, name text);   -- OK
INSERT INTO deploy_test VALUES(1, 'OpenTenBase on WSL!');  -- OK
SELECT * FROM deploy_test;                      -- OK, 返回数据
```

**集群状态**：
```
Node dn0001(127.0.0.1:11000) is Running
Total: 1, Running: 1, Stopped: 0, Unknown: 0
```

### 对 README 文档的改进建议

基于实际部署经验，建议 README_ZH.md 增加以下内容：

1. **预编译包的系统依赖说明**：预编译包（CentOS 7 GCC 4.8.5 编译）在 Ubuntu 22.04 上缺少 `libssl.so.10` 和 `libreadline.so.6`，需说明如何安装兼容库
2. **单机部署的配置注意事项**：单机部署集中式实例时，必须去掉 `slave` 配置行，否则数据目录被覆盖
3. **集中式实例部署后的手动步骤**：部署完成后需手动执行 `CREATE NODE`、`CREATE DEFAULT node group`、`CREATE sharding group` 才能正常使用
4. **源码编译的 GCC 版本要求**：明确说明需要 GCC 7-9，Ubuntu 22.04 的 GCC 11 不兼容
5. **Windows 文件系统复制时的 CRLF 问题**：如果从 Windows 复制源码到 WSL，必须先执行 `dos2unix`/`find -exec dos2unix`

## 结论

OpenTenBase v5.0 在 WSL2 Ubuntu 22.04 上成功部署并运行，验证了基本 SQL 功能（CREATE TABLE / INSERT / SELECT）。部署过程使用了预编译包方案，绕过了 GCC 11 兼容性问题。单机部署需要特别注意 slave 配置和数据目录冲突问题。

本次核验方式见 [AI 使用策略自我报告](AI_USAGE_REPORT_ZH.md)。
