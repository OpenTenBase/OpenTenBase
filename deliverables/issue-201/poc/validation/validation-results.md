# Stage 4B 验证结果

## 日期

2026-07-12（Asia/Shanghai）

## 已执行命令

```sh
command -v kubectl || true
command -v kubeconform || true
command -v yq || true
python3 -c "import yaml; print(yaml.__version__)" 2>/dev/null || true
python3 work/issue-201/poc/validation/validate.py
kubectl apply --dry-run=client -f work/issue-201/poc/crd/opentenbasecluster-crd.yaml
```

以上路径记录 Stage 4B 当时真实执行的命令；复制到最终交付目录后，离线验证器通过 `python3 deliverables/issue-201/poc/validation/validate.py` 再次执行。

## 工具可用性

| 工具 | 结果 |
|---|---|
| `python3` | 可用；验证脚本执行完成。 |
| PyYAML | 不可用；import/version 命令无输出。 |
| `kubectl` | 可用，路径为 `/usr/local/bin/kubectl`。 |
| `kubeconform` | 不可用；`command -v` 无输出。 |
| `yq` | 不可用；`command -v` 无输出。 |

验证脚本没有安装软件包，也没有访问网络。

## 真实输出摘要

`validate.py` 以退出码 0 结束：

```text
SUMMARY: 38 passed, 0 failed, 1 warning
```

警告为：

```text
WARN: PyYAML unavailable; fallback text checks used (not full YAML validation).
```

`kubectl apply --dry-run=client` 以非零退出码结束，因为 `kubectl` 尝试从 `http://localhost:8080/openapi/v2` 获取 OpenAPI，但连接不被允许。没有应用任何资源。命令建议关闭 validation，但没有使用 `--validate=false`，因为这不能证明 schema 有效。

## 已通过检查

- 八个必需文件均存在且非空；
- 回退检查找到了 CRD API identity、group、kind、`v1alpha1` 和 status subresource；
- CRD schema 中包含全部十二个已批准阶段；
- 最小样例包含两个唯一 DN shard ID、一个 GTM primary/standby、两个 Coordinator primary、每个 primary 一个 standby，以及每个 DN shard 一个 standby；部分 status 中 Coordinator 期望数量与四个实例一致；
- 样例只使用显式不可运行镜像占位符；
- 伪结构未选择工作负载 primitive，也没有禁止的可运行 logical kind；
- README 和伪结构包含必需警告；
- 追踪矩阵包含四种已批准分类。

## 失败检查

- `validate.py` 内部检查：无；
- Kubernetes OpenAPI 外部验证：未完成，因为没有可访问的 API Server/OpenAPI endpoint。

## 警告

- PyYAML 不可用，因此回退检查只是文本检查，不是完整 YAML parser 或 OpenAPI validator；
- 当前环境中的 `kubectl --dry-run=client` 仍尝试发现服务端 OpenAPI；
- CRD 注册到 API Server 之前，client-side validation 不能证明自定义资源符合 CRD。

## 未验证事项

- Kubernetes API Server acceptance 与 structural-schema enforcement；
- 安装 CRD 后的样例自定义资源 admission；
- 草案字段以外的 CEL/admission enforcement；
- controller reconciliation、status 写入、所有权、工作负载生成、替换或存储行为；
- OpenTenBase 启动、hostname 兼容性、topology/group 操作、pool reload、重试幂等性、failover、backup/restore、resharding、upgrade、安全或监控。

## 为什么运行时行为仍未得到证明

这些文件只提供 API 与文档形态，不包含 Operator、可运行工作负载、已选镜像、bootstrap 命令、SQL、probe、port 或运行环境。因此，静态文件和 schema-oriented 检查不能证明任何数据库或分布式系统运行行为。

CRD 是字段显式类型化的 structural-schema-oriented 草案，目标是满足 Kubernetes structural-schema 要求，但本次没有通过 API Server CRD admission 或 structural-schema enforcement 对此进行外部验证。
