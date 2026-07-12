# OpenTenBase 最小部署路径验证记录

本记录针对 `README_ZH.md` 中的源码编译、依赖安装、`opentenbase_ctl` 部署和 CN 连接路径。它区分了已执行的验证与因本机环境限制未执行的步骤，避免将静态检查或未完成的 Docker 流程表述为部署成功。

## 验证范围和环境

- 验证日期：2026-07-12
- 验证终端：Windows PowerShell
- OpenTenBase 提交：`b612d77c`（`master`）
- 参考资料：仓库 `README_ZH.md`、`contrib/opentenbase_ctl` 源码与配置模板、`OpenTenBase-Intro.pptx` 第 6 至 12 页。

PPT 提供了基于 Docker 的容器化开发环境：安装 Docker、配置镜像加速、执行 `otb-dev.sh build && otb-dev.sh up`，然后进入开发容器完成编译和集群部署。PPT 同时说明其脚本会通过 Gitee 镜像获取源码；因此不能假定它与 GitHub 上的 `OpenTenBase-DevEnv` 脚本完全一致。

## 已执行的检查

| 检查项 | 命令或依据 | 结果 |
| --- | --- | --- |
| 工作树检查 | `git status -sb`、`git diff --check` | 文档改动无空白错误。 |
| Docker 可用性 | `docker --version` | 失败：PowerShell 报告 `docker` 不是可识别的命令。未安装 Docker CLI，因此未运行容器构建或集群部署。 |
| 部署命令 | `contrib/opentenbase_ctl/src/command/command.cpp` | 已确认注册 `install`、`status`、`start`、`stop` 等子命令，并为部署命令提供 `-c/--config` 配置参数。 |
| 配置模板 | `contrib/opentenbase_ctl/config/config.ini` | 已确认仓库内存在可复制的 `config.ini` 模板，含 `instance`、`gtm`、`coordinators`、`datanodes`、`server` 和 `log` 段。 |
| 配置约束 | `contrib/opentenbase_ctl/src/types/types.cpp` | 已确认 `nodes-per-server` 默认模板值为 1，源码限制最大值为 5。 |

## 遇到的问题与文档修复

| 问题 | 影响 | 修复 |
| --- | --- | --- |
| README 提到 `prepare config`，但当前 CLI 源码未注册该子命令。 | 初学者会等待不会生成的配置模板。 | 改为从 `contrib/opentenbase_ctl/config/config.ini` 复制模板。 |
| README 用相对路径调用 `opentenbase_ctl`，与前文定义的 `PG_HOME` 不一致。 | 工作目录不同会导致找不到工具。 | 统一使用 `"${PG_HOME}/bin/opentenbase_ctl"`。 |
| 配置项表格包含在代码块中，且分隔行多出一个 `|`。 | Markdown 无法正确渲染，不利于理解配置。 | 恢复为标准 Markdown 表格。 |
| README 未集中说明动态库、SSH、端口和 CN 连接失败的排查方法。 | 首次部署遇错时缺少可执行的定位路径。 | 新增“常见错误与排查”小节。 |

## 在 Linux/Docker 环境中的复验步骤

以下步骤尚未在本机执行，供具备 Linux、Docker 和网络条件的贡献者复验。执行前请先检查目标环境，并以 `OpenTenBase-DevEnv` 仓库的实际 README 为准。

```bash
# 环境前置检查
docker --version
docker info
git clone https://github.com/OpenTenBase/OpenTenBase-DevEnv.git
cd OpenTenBase-DevEnv

# PPT 中给出的旧版脚本示例；仅在与当前脚本一致时使用。
./otb-dev.sh build
./otb-dev.sh up

# 部署完成后，在包含配置文件的目录验证状态和 CN 连接信息
"$PG_HOME/bin/opentenbase_ctl" status -c config.ini
```

验收时应保存以下输出到 PR 描述：`docker --version`、容器启动日志、`opentenbase_ctl status -c config.ini` 的节点汇总，以及按 `status` 输出连接 CN 后执行 `select * from pgxc_node;` 的结果。

## 结论

本机完成了 README 路径的静态核对和文档可用性修复，但没有完成真实的 Docker 集群部署，原因是 Docker CLI 缺失。该限制和原始错误已保留在本记录中，真实部署应在满足前置条件的 Linux/Docker 环境复验后再标记为通过。

本次文档生成和核验方式见 [AI 使用策略自我报告](AI_USAGE_REPORT_ZH.md)。
