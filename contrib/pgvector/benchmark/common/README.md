# Common benchmark infrastructure

`common/` 是 IVFFlat Build Diagnosis 的共享基础层，不是独立 benchmark。

- `config.py`：数据集、metric、PostgreSQL 和运行目录配置。
- `db.py`：隔离 cluster、`pgvector_benchmark` 数据库、表和向量加载。
- `datasets/`：SIFT1M、GloVe100、Music100 数据适配。
- `ground_truth.py`：完整数据和子集 Ground Truth。
- `query_quality.py`：Recall、延迟分位数和单客户端 QPS。
- `manifest.py`、`schema.py`、`run_store.py`：运行身份、环境快照和证据落盘。

默认运行数据都位于 `benchmark/` 下并被 Git 忽略：

```text
data/       downloaded datasets
results/    Diagnosis Run outputs
pgdata/     isolated PostgreSQL cluster
.runtime/   generated Smoke fixtures
.venv/      Python environment
```

用户应通过根目录 `./benchmark` 或 `make` 调用，不直接执行本目录模块。
