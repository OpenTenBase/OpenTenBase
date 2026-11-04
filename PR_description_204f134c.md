# PR 描述（复制到 GitHub PR 正文即可）

**标题：** docs: 修复 README_ZH.md 部署文档多处错误并新增"常见错误与排查"小节

---

## 背景

在全新环境（Debian 13，2C/3.8G）中严格按 README_ZH.md 从零执行最小部署路径验证：源码编译 → opentenbase_ctl 部署单机最小集群（1 GTM + 1 CN + 2 DN）→ 连接 CN。过程中复现出 10 处文档问题，本 PR 修正其中影响部署成败的关键问题，并补充排查指引。

## 问题与修改点

### 1. 依赖清单缺失 zstd / lz4（编译阻断）

- `./configure` 报 `zstd library not found` / `lz4 library not found`
- 根因：configure 硬编码链接 `/usr/local/lib/libzstd.a`、`/usr/local/lib/liblz4.a`（静态库），系统包安装后仍不满足
- 佐证：官方 OpenTenBase-DevEnv 仓库 Dockerfile.centos 安装了 `libzstd-devel libzstd-static lz4-devel lz4-static` 并将静态库软链至 /usr/local/lib，注释自称"参考 README_ZH.md"，但 README 依赖清单并未包含
- 修改：依赖清单补充 4 个包，并新增 /usr/local/lib 软链接说明

### 2. `CFLAGS="-g"` 覆盖 `-mcx16` 导致链接失败（编译阻断）

- 现象：`undefined reference to '__sync_val_compare_and_swap_16'`
- 根因：configure 为 x86_64 自动追加 `-mcx16`，命令行 CFLAGS 会整体覆盖
- 修改：改为 `CFLAGS="-g -mcx16"`，并提示重新 configure 后必须 `make clean`

### 3. 配置文件 `conf` 字段缺失（部署阻断）

- 照抄文档示例配置必现 `There are some errors in the coordinators configurations`
- 根因：`opentenbase_ctl` 的 `build_cn_node_config`/`build_dn_node_config` 无条件解析 `conf` 指向的文件，字段为空即失败；字段说明表与示例均未提及该字段
- 修改：字段说明表补充 `conf` 行（标注必填），分布式/集中式示例均补充，并新增单机最小验证配置示例

### 4. zh_CN.utf8 locale 前置要求未说明（部署静默失败）

- install 显示成功，status 全部 Stopped；`pg_ctl_start.log` 报 `is not a database cluster directory`
- 根因：initdb 硬编码 `--locale=zh_CN.utf8` 且输出重定向 /dev/null，缺 locale 时静默失败
- 修改：依赖节加 locale 生成说明；排查小节给出完整定位路径

### 5. install 后需手动 start（文档直接跳到 Running 状态输出）

- 新增"启动实例并检查状态"小节

### 6. 路径不一致

- 编译目录 `install/opentenbase_bin_v5.0/` 与 ctl 运行时目录 `install/opentenbase/5.21.8/` 未解释，"使用"章节还出现第三种路径 `/home/opentenbase/install/opentenbase/5.21.8/`
- 修改：统一说明并给出两种可用环境变量写法

### 7. 新增「常见错误与排查」小节

覆盖：LD_LIBRARY_PATH/PATH 设置、zstd/lz4、-mcx16、conf 字段、locale 静默失败、SSH/sshd 与 SCP 失败、端口与防火墙、pg_hba.conf、`su -` 语义、重复安装清理等 8 个条目，全部来自实际复现或源码定位。

### 8. 其他小修正

- gtm slave 示例中不存在的 `172.16.16.50` IP 修正为文档正文声明的两台服务器
- `language-pack-zh-hans` 仅 Ubuntu 可用，补充 Debian 的 `localedef` 替代方案
- `postgresql-devel` 需 EPEL 提示
- 手动 tar 打包步骤与 ctl 工具 step 1 重复关系的说明

## 验证方式

- 全程部署日志（关键报错与解决过程）见部署日志附件
- 修改后的文档步骤已在本机验证走通：编译成功（exit=0）→ install 全流程成功 → 修正 locale/conf 后三个节点 initdb 数据目录完整初始化 → `opentenbase_ctl start` 4/4 Success、status 4/4 Running
- 说明：验证环境为受限沙盒（2C/3.8G、cgroup CPU 限制、非 TSC 时钟源），GTM 进程因沙盒的 CPU 亲和性绑定限制（`bind_thread_to_cores` FATAL）无法驻留，故 psql 长连接验证未能在该环境完成；该限制与文档正确性无关，标准环境中不存在。其余全部步骤均有真实执行记录。

## 部署日志摘要

```
# 问题 2/3：依赖缺失
configure: error: zstd library not found
configure: error: lz4 library not found
# config.log: /usr/bin/ld: cannot find /usr/local/lib/libzstd.a

# 问题 4：链接失败
/usr/bin/ld: ... undefined reference to `__sync_val_compare_and_swap_16'

# 问题 5：配置报错
can not open file:
[src/types/types.cpp:678] There are some errors in the coordinators configurations

# 问题 6/7：静默失败
pg_ctl: directory ".../cn0001/data" is not a database cluster directory
# 实际命令（工具日志）：initdb ... --locale=zh_CN.utf8 ... &>/dev/null

# 修正后
====== Installation completed successfully  ======
[Result] Total: 4, Success: 4, Failed: 0
```
