# AI 使用策略自我报告

## 1. 使用的 AI 工具

- **千问办公（QwenWork）智能体**：全程唯一的 AI 工具。用于：
  - 阅读和理解 README_ZH.md、OpenTenBase 源码（configure、opentenbase_ctl 的 C++ 代码）、官方 DevEnv 仓库的 Dockerfile
  - 在沙盒环境中实际执行全部部署命令
  - 定位报错根因（读 config.log、ctl 工具日志、源码回溯）
  - 起草修改后的 README 与本报告

## 2. 如何验证 AI 的输出

所有结论都执行了"AI 判断 → 独立证据复核"流程，不接受未经验证的说法：

| AI 的判断 | 验证手段 | 结果 |
|---|---|---|
| zstd/lz4 是文档遗漏的依赖 | 1) 真实执行 `./configure` 两次报错；2) `config.log` 中 `cannot find /usr/local/lib/libzstd.a`；3) 读 configure 源码第 12802/12852 行确认硬编码；4) 与 DevEnv 仓库 Dockerfile 交叉比对（官方自己装了这 4 个包） | 证据一致，采纳 |
| `CFLAGS="-g"` 覆盖 `-mcx16` | 1) 复现链接错误；2) `grep CFLAGS src/Makefile.global` 确认无 mcx16；3) 读 configure 第 18939 行确认自动追加逻辑；4) 加 `-mcx16` + `make clean` 后编译成功 | 证据一致，采纳 |
| `conf` 字段必填 | 1) 照抄文档示例复现报错；2) 读 `config.h` 中结构体注释（写着"可选"）与 `types.cpp` 实际逻辑（无条件解析）矛盾；3) 补字段后 install 成功 | 代码行为与注释矛盾，以**实际行为**为准，采纳 |
| zh_CN.utf8 locale 是静默失败根因 | 1) status 全 Stopped 复现；2) `pg_ctl_start.log` 显示目录非集群目录；3) 工具日志中找到被 `&>/dev/null` 吞掉的 initdb 实际命令；4) 源码确认 locale 不可配置；5) 生成 locale 后重装成功 | 证据链完整，采纳 |

## 3. 被拒绝或修正的 AI 建议

1. **最初猜测"直接 apt 安装 libzstd-dev 即可解决"**——被拒绝：安装后 configure 仍失败，config.log 证明它只认 /usr/local/lib 下的静态库。改为源码编译 + 文档中写明软链接方案。
2. **曾考虑把"语言包 language-pack-zh-hans"直接从依赖清单删除**——被修正：深挖后发现它其实与 initdb 硬编码的 zh_CN.utf8 locale 相关，是"用错位置的线索"而非完全无用。最终方案是保留并补充说明 + 用 localedef 替代。
3. **AI 初判 `install/opentenbase/5.21.8` 路径是"旧版本残留应统一删除"**——被拒绝：实际部署发现这是 ctl 工具自动生成的运行时目录（工具日志 `Install Path: /data/opentenbase/install/opentenbase/5.21.8`），不能删，改为补充解释两个目录的关系。
4. **AI 建议在排查小节中写入"chmod -R 777"之类的权限速修**——被拒绝：属于不安全实践，未采纳；改为标准的权限排查建议。

## 4. AI 输出可信度声明

- 本 PR 的每处文档修改均对应沙盒中真实复现的报错或源码级定位，关键报错原文均保留在部署日志（deploy_log.md）中，可复查。
- AI 生成内容中未经证实的推测（如对官方意图的猜测）均未写入文档正文，仅以"注意/说明"形式给出可操作指引。
- 部署在 2C/3.8G 的受限环境完成，低于 README 建议的 8G 内存；编译、install、initdb、节点启动命令均完整走通。psql 长连接验证因沙盒的 CPU 亲和性与时钟源限制（GTM `binding threads failed`、非 TSC clocksource）未能完成，此限制已在部署日志与 PR 描述中如实声明，未将其伪装为成功。
