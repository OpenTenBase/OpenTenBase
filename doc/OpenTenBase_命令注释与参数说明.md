<!-- Copyright (c) 2026 kongdangforever. SPDX-License-Identifier: BSD-3-Clause -->

# 读懂 OpenTenBase 编译前的 8 行准备命令

第 3 题：命令注释与参数说明

## 1. 选取的文档与范围

来源：[OpenTenBase 官方仓库 README_ZH.md](https://github.com/OpenTenBase/OpenTenBase/blob/master/README_ZH.md)，「构建 → 编译」小节。

选段：从 `su - opentenbase` 到 `chmod +x configure*`，共 8 行非空命令。

核对日期：2026-11-07。来源为 master 分支，后续内容可能更新。

## 2. 修改前：原文命令

```bash
su - opentenbase
cd /data/opentenbase/
git clone https://github.com/OpenTenBase/OpenTenBase

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/
cd ${SOURCECODE_PATH}
rm -rf ${INSTALL_PATH}/opentenbase_bin_v5.0
chmod +x configure*
```

## 3. 修改后：保留命令，补充逐行注释

```bash
# 1. 切换到 opentenbase 用户，并启用该用户的登录环境。
#    “-”表示使用登录式环境；opentenbase 是系统用户名。
su - opentenbase

# 2. 进入示例工作目录；下一行会在这个目录下获取源码。
#    若采用其他工作目录，需同步修改下面的 SOURCECODE_PATH。
cd /data/opentenbase/

# 3. 从官方仓库克隆源码；未指定目标目录名时，通常创建 OpenTenBase 子目录。
#    若同名目录已存在且非空，这条命令不会自动更新其中的源码。
git clone https://github.com/OpenTenBase/OpenTenBase

# 4. 设置并导出源码路径变量，供后续命令引用。
#    此操作只设置变量，不会创建目录，也不会自动进入该目录。
export SOURCECODE_PATH=/data/opentenbase/OpenTenBase

# 5. 设置并导出安装基础路径；原文后续配置会在其下指定安装子目录。
#    此操作不会创建目录，也不会执行安装。
export INSTALL_PATH=/data/opentenbase/install/

# 6. 读取 SOURCECODE_PATH 的值，进入源码根目录。
#    后面的 configure* 是相对于此目录匹配的文件名。
cd ${SOURCECODE_PATH}

# 7. 递归、强制删除指定的安装子目录；不是只清理源码中的编译缓存。
#    -r 表示递归删除，-f 表示强制处理、不提示确认等行为。
#    执行前必须确认 INSTALL_PATH 的实际值，以及目标目录内没有需要保留的文件。
rm -rf ${INSTALL_PATH}/opentenbase_bin_v5.0

# 8. 为源码根目录下名称以 configure 开头的匹配项增加执行权限。
#    +x 表示增加执行权限；* 是 Shell 通配符，不是文件名的一部分。
#    这一步只调整权限，不运行配置脚本，也不开始编译。
chmod +x configure*
```

## 4. 哪些内容需要替换？

以下参数按实际环境替换。

| 命令中的内容 | 含义 | 是否需要替换 | 一致性要求 |
|---|---|---|---|
| `su - opentenbase` 中的 `opentenbase` | 执行后续步骤的系统用户 | 使用原文创建的用户时不改；使用其他已配置用户时替换 | 对应用户需具备相关目录权限；用户名和目录名并不必然相同 |
| `cd /data/opentenbase/` 中的路径 | 获取源码时所在的工作目录 | 与实际工作目录不同时替换 | 克隆后源码位置需与 `SOURCECODE_PATH` 对应 |
| `SOURCECODE_PATH=` 后的路径 | 本地源码根目录 | 与实际源码位置不同时替换 | 默认克隆目录名为 `OpenTenBase`；注意大小写 |
| `INSTALL_PATH=` 后的路径 | 安装基础目录 | 与预定安装位置不同时替换 | 同时影响本段删除目标和原文后续 `--prefix` 所用路径 |
| `opentenbase_bin_v5.0` | 原文使用的安装子目录名 | 本次注释稿保留；若自行改名，应同步调整全文相关引用 | 特别核对后续 `--prefix` 和 `PG_HOME`，不能只改删除命令 |
| `https://github.com/OpenTenBase/OpenTenBase` | 官方源码仓库地址 | 本题保留 | 不应当作个人目录替换 |

`SOURCECODE_PATH`、`INSTALL_PATH` 是变量名；`${SOURCECODE_PATH}`、`${INSTALL_PATH}` 是变量引用写法，本题均保留。`-`、`-rf`、`+x` 和 `*` 是命令选项或 Shell 语法，不属于待替换参数。

原文的变量引用没有加双引号。这些示例不应直接套用于含空格或 Shell 特殊字符的自定义路径。

## 5. 验证记录

| 核对项 | 核对方式 | 核验结果 |
|---|---|---|
| 示例确实来自指定项目 | 对照官方 `README_ZH.md` 的「编译」小节 | 已核对：选段包含 8 行非空命令 |
| 保留原命令行为 | 对比修改前后，排除新增注释和空行 | 命令内容与顺序一致 |
| 说明与上下文对应 | 对照前置章节、后续 `--prefix` 和 `PG_HOME` | 已核对：工作目录、源码路径和安装路径的关系与原文一致 |
| 替换说明完整一致 | 逐项检查选段中的用户名、路径、URL、变量与通配符 | 已逐项列出；没有新增未解释的占位符 |
| 是否实际执行 | 检查本次工作范围 | 未执行上述命令，未验证编译或安装结果 |

本次验证属于文档与命令语义核对，不保证这些命令在任意机器上执行成功。

## 6. 参考资料

- 命令来源：[README_ZH.md，编译小节](https://github.com/OpenTenBase/OpenTenBase/blob/master/README_ZH.md#编译)，「构建 → 编译」小节，从 `su - opentenbase` 到 `chmod +x configure*`，共 8 行非空命令。
- 安装目录核对：[README_ZH.md，编译小节的 configure 命令](https://github.com/OpenTenBase/OpenTenBase/blob/master/README_ZH.md#编译)，`./configure` 命令中的 `--prefix=${INSTALL_PATH}/opentenbase_bin_v5.0`。
- 环境变量核对：[README_ZH.md，安装准备的环境变量配置](https://github.com/OpenTenBase/OpenTenBase/blob/master/README_ZH.md#准备)，安装准备中的 `PG_HOME`、`PATH` 和 `LD_LIBRARY_PATH` 设置。
- 项目官网：[https://www.opentenbase.org/](https://www.opentenbase.org/)

以上链接指向 master 分支。不同版本的行号可能变化，请按章节标题及命令内容定位。
