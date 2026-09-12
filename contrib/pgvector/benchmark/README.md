# IVFFlat Build Diagnosis Benchmark

本目录是 pgvector IVFFlat 构建诊断的独立入口。它覆盖：

- P0：构建前检查、内存预测、构建进度、进程资源、PG IO/WAL、索引元数据和查询质量；
- P1：索引结构扫描、联合诊断、SLO 可行性、Pareto、参数建议和显式 Validation；
- 人类报告：每次运行在结果根目录生成 `README.md`，原始证据保存在 `raw/`。

Benchmark 不会在代码仓库中保存数据集、PGDATA、虚拟环境或运行结果。

## 1. 快速自动验证

已经具备 PostgreSQL/OpenTenBase 编译环境时，可以直接执行：

```bash
cd OpenTenBase/contrib/pgvector/benchmark
make bootstrap
make verify
```

`make verify` 会依次执行：

1. 35 项 Python 单元及异常路径测试；
2. pgvector 全量 SQL regression；
3. 生成固定随机种子的 2,000 行测试数据；
4. 执行 Build、Resource、Quality、Structure 和 Diagnosis；
5. 检查机器结果和人类报告是否完整。

Smoke 数据写入 `.runtime/smoke-data/`，不会下载或覆盖真实 SIFT1M。

> PostgreSQL 不允许由 root 初始化或启动。`make verify`、`make setup` 和真实 Benchmark
> 应由普通操作系统用户执行，并保证该用户对本目录有写权限。

## 2. 分步准备环境

### 2.1 运行环境与 Python 依赖

运行 Benchmark 需要：

- Linux：进程资源观测依赖 `/proc`；
- Python 3.10 或更高版本；
- PostgreSQL/OpenTenBase 服务端及开发环境；
- `make`、`pg_config`、`initdb`、`pg_ctl`、`psql` 和 `createdb`。

推荐直接创建虚拟环境并安装 Python 依赖：

```bash
cd OpenTenBase/contrib/pgvector/benchmark
make bootstrap
```
它等价于：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

`make bootstrap` 不会激活当前 Shell；后续 `make` 和 `./benchmark` 会直接调用
`.venv/bin/python`。如果希望在当前 Shell 中使用虚拟环境，手工执行：

```bash
source .venv/bin/activate
```

### 2.2 编译和安装 pgvector

Benchmark 的 P1 Structure 功能依赖本仓库提供的
`ivfflat_list_stats()` 和 `ivfflat_index_stats()`，因此必须先安装当前 pgvector：

```bash
cd OpenTenBase/contrib/pgvector
make
make install
```

如果系统存在多个 PostgreSQL/OpenTenBase 版本，应明确指定目标 `pg_config`：

```bash
make clean
make PG_CONFIG=/usr/local/pgsql/bin/pg_config
make PG_CONFIG=/usr/local/pgsql/bin/pg_config install
```

如果已有数据库安装的是 vector 0.8.6 时，连接该数据库执行：

```sql
ALTER EXTENSION vector UPDATE TO '0.8.7';
```

检查版本和 P1 函数：

```bash
psql -d pgvector_benchmark <<'SQL'
SELECT extversion FROM pg_extension WHERE extname = 'vector';
SELECT to_regprocedure('ivfflat_index_stats(regclass)');
SELECT to_regprocedure('ivfflat_list_stats(regclass,real)');
SQL
```

预期 extension 版本为 `0.8.7`，两个函数均返回函数签名而不是 `NULL`。

## 3. 准备数据库

数据库准备只负责初始化 cluster、启动 PostgreSQL、创建数据库和安装 vector extension。
真实向量表、数据导入和索引构建由每次 Diagnosis Run 自己完成。

可以使用本地隔离 cluster，也可以连接已有测试 cluster。

### 3.1 方式 A：使用 Benchmark 隔离 cluster

默认配置：

```text
PGDATA     benchmark/pgdata
PGHOST     /tmp
PGPORT     5432
PGDATABASE pgvector_benchmark
PGLOG      benchmark/pgdata/postgresql.log
```

在 `benchmark/` 目录执行：

```bash
export PGBIN=/usr/local/pgsql/bin
export PGUSER=opentenbase

./benchmark setup
```

这个命令可重复执行，内部会：

1. 在 `PGDATA` 不存在时执行 `initdb`；
2. 检查 server 状态，未启动时执行 `pg_ctl start`；
3. 创建 `pgvector_benchmark` 数据库；
4. 执行 `CREATE EXTENSION IF NOT EXISTS vector`；
5. 输出实际 PGDATA、host、port、database 和日志路径。

也可以覆盖默认位置和端口：

```bash
export PGBIN=/opt/opentenbase/bin
export PGDATA=/data/pgvector-benchmark-pgdata
export PGHOST=/tmp
export PGPORT=55432
export PGUSER=opentenbase
export PGDATABASE=pgvector_benchmark

./benchmark setup
```

确认数据库可连接：

```bash
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" \
  -d "$PGDATABASE" -c 'SELECT current_database(), version();'
```

需要手工停止隔离 cluster 时：

```bash
"$PGBIN/pg_ctl" -D "$PGDATA" stop
```

### 3.2 方式 B：连接已有测试 cluster

先由数据库管理员创建专用数据库并安装 extension：

```bash
createdb -h DB_HOST -p 5432 -U DB_USER pgvector_benchmark
psql -h DB_HOST -p 5432 -U DB_USER -d pgvector_benchmark \
  -c 'CREATE EXTENSION IF NOT EXISTS vector;'
```

然后设置连接信息：

```bash
export PGDATABASE=pgvector_benchmark
export PG_DSN='host=DB_HOST port=5432 user=DB_USER password=DB_PASSWORD dbname=postgres'
```

Runner 会把 `PG_DSN` 中的 `dbname` 替换为 `PGDATABASE`。因此目标数据库必须预先存在；
外部 cluster 模式下不要依赖 `./benchmark setup` 创建数据库。

如果需要执行 `make sql-test`，还应设置 libpq 标准变量，因为 PostgreSQL regression
工具不读取 `PG_DSN`：

```bash
export PGHOST=DB_HOST
export PGPORT=5432
export PGUSER=DB_USER
export PGPASSWORD=DB_PASSWORD
```

不要把生产数据库用于 Benchmark。运行过程会反复删除和重建 `vector_base` 表及其索引。

## 4. 下载数据集

支持三套固定数据集：

| metric | 数据集 | 行数 | 维度 | 距离/算子 | 本地目录 |
|---|---:|---:|---:|---|---|
| `l2` | SIFT1M | 1,000,000 | 128 | L2 `<->` | `data/sift1m/` |
| `cosine` | GloVe100 | 1,183,514 | 100 | Cosine `<=>` | `data/glove-100-angular/` |
| `ip` | Music100 | 1,000,000 | 100 | Inner Product `<#>` | `data/music100/` |

下载单个数据集：

```bash
./benchmark download --dataset l2
./benchmark download --dataset cosine
./benchmark download --dataset ip
```

下载全部数据集：

```bash
./benchmark download --dataset all
```

等价 Make 命令：

```bash
make download METRIC=l2
make download METRIC=cosine
make download METRIC=ip
make download METRIC=all
```

脚本会检查文件大小或数据 shape；已有完整文件会直接复用。下载中断时可能留下 `.part`
文件，确认下载未在进行后删除不完整文件再重试。

也可以手工准备数据，只要最终目录和文件名满足：

```text
data/
├── sift1m/
│   ├── sift_base.fvecs
│   ├── sift_query.fvecs
│   └── sift_groundtruth.ivecs
├── glove-100-angular/
│   └── glove-100-angular.hdf5
└── music100/
    ├── database_music100.bin
    ├── query_music100.bin
    └── groundtruth_music100_top100.txt
```

建议为原始数据、临时加载空间、索引和 PostgreSQL WAL 预留足够磁盘空间。三套原始
数据合计约 1.4 GiB，但完整实验所需空间会明显高于原始文件大小。

## 5. 运行 Diagnosis

### 5.1 先运行快速 Smoke

```bash
make smoke
```

Smoke 不使用真实数据集，适合确认 Python、数据库、pgvector、P1 函数和报告链路是否
正常。成功时最后会输出：

```text
Smoke verification passed: results/sift1m-l2/diag-automated-smoke-l2
```

### 5.2 运行单个数据集

SIFT1M / L2：

```bash
./benchmark run \
  --metric l2 \
  --rows 1000000 \
  --lists 1000 \
  --probes 32 \
  --maintenance-work-mem 256MB
```

GloVe100 / Cosine：

```bash
./benchmark run \
  --metric cosine \
  --rows 1183514 \
  --lists 1000 \
  --probes 32 \
  --maintenance-work-mem 256MB
```

Music100 / Inner Product：

```bash
./benchmark run \
  --metric ip \
  --rows 1000000 \
  --lists 1000 \
  --probes 32 \
  --maintenance-work-mem 256MB
```

也可以按矩阵运行多个参数：

```bash
./benchmark run \
  --metric l2 \
  --rows 100000 1000000 \
  --lists 100 1000 \
  --probes 1 10 32 \
  --query-count 100 \
  --repeat 3 \
  --warmup 20 \
  --k 10 \
  --sample-interval 0.5 \
  --maintenance-work-mem 256MB
```

`--rows 0` 表示使用当前数据集全部行。默认会严格检查 `requested_rows == actual_rows`；
不要在正常 Benchmark 中关闭该检查。

### 5.3 Make 参数入口

简单的单配置实验可以使用：

```bash
make run METRIC=l2 LISTS=1000 PROBES=32 MEMORY=256MB \
  ARGS='--rows 1000000 --query-count 100 --repeat 3'
```

复杂矩阵建议直接使用 `./benchmark run`，完整参数通过以下命令查看：

```bash
./benchmark run --help
```

### 5.4 启用 SLO 和 Recommendation Validation

只有用户显式提供 SLO，系统才会判断候选配置是否满足目标。例如：

```bash
./benchmark run \
  --metric l2 \
  --rows 1000000 \
  --lists 1000 \
  --probes 8 16 32 64 \
  --maintenance-work-mem 256MB \
  --min-recall 0.95 \
  --max-p95-ms 50 \
  --validate-recommendations \
  --max-validations 1
```

Validation 会创建新的 Build/Quality ID 进行 before/after 重跑，不会把原候选结果直接
当作验证结果。未提供 SLO 时只生成 Pareto 前沿，不声明全局最佳参数。

## 6. 关键参数说明

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--metric` | `all` | `l2`、`cosine`、`ip` 或全部 |
| `--rows` | `100000 0` | 数据规模，`0` 表示完整数据集 |
| `--lists` | `100 1000 3162` | IVFFlat lists 矩阵 |
| `--probes` | `10` | Quality Run probes 矩阵 |
| `--query-count` | `100` | 每轮测量查询数 |
| `--repeat` | `3` | Quality Run 重复次数 |
| `--warmup` | `20` | 每轮测量前预热查询数 |
| `--k` | `10` | Recall@K 和 Top-K |
| `--sample-interval` | `0.5` | Build 资源采样间隔，单位秒 |
| `--maintenance-work-mem` | 数据库当前值 | Preflight 和 CREATE INDEX 使用的会话配置 |
| `--on-predicted-memory-failure` | `skip` | 预测必然失败时跳过或强制执行 |
| `--structure-sample-rate` | `1.0` | P1 list 结构扫描比例 |
| `--validate-recommendations` | 关闭 | 显式执行建议验证 |

正常实验中，如果某个 lists 没有执行，应先看根结果 `README.md` 和对应 Build report。
常见原因包括：

- requested rows 与数据库实际 rows 不一致，Preflight 直接失败；
- 预测 pgvector checked memory 超过 `maintenance_work_mem`，Build 被跳过；
- extension/P1 函数版本不满足要求；
- 数据库连接或权限失败。

报告会区分 `failed`、`skipped`、`partial` 和指标 `unavailable`，并记录具体 reason。

## 7. 阅读结果

结果按 metric 写入：

```text
results/
└── sift1m-l2/
    └── diag-.../
        ├── README.md
        ├── summary.md
        ├── build-summary.csv
        ├── quality-summary.csv
        ├── builds/
        ├── quality/
        ├── validations/
        └── raw/
```

阅读顺序：

1. 根 `README.md`：本次实验结论、Build/Quality/Structure 和不可用能力；
2. `builds/.../report.md`：单次 Build 的阶段、资源和结构详情；
3. `quality/.../report.md`：单次 probes 的 Recall、延迟和 QPS；
4. `raw/`：JSON/JSONL 原始证据，主要用于追溯和程序处理。

顶层状态含义：

- `COMPLETE`：要求的 Build/Quality/Structure 数据均成功；
- `COMPLETE WITH PARTIAL DATA`：实验完成，但至少一个采集能力不可用或数据不完整；
- `COMPLETE WITH FAILURES`：至少存在 Build 或 Quality 失败。

所有结论只适用于报告中声明的数据集、metric、cache 和 single-client workload，不代表
生产并发容量，也不代表全局最优 IVFFlat 配置。

## 8. 独立测试入口

```bash
# Python 单元及 P0 异常路径测试
make test

# pgvector SQL regression，包括 ivfflat_stats
make sql-test

# 小型端到端测试
make smoke

# 三者全部执行
make verify
```

`make sql-test` 连接失败时，先确认 PostgreSQL 正在运行，并检查 `PGHOST`、`PGPORT`、
`PGUSER` 和 `PGPASSWORD`。它通过父目录 pgvector 的 `make installcheck` 执行。

## 9. 常见问题

### `ModuleNotFoundError: No module named 'psycopg'`

```bash
make bootstrap
```

或者在当前 Python 环境中安装：

```bash
python3 -m pip install 'psycopg[binary]>=3.1'
```

### `unrecognized configuration parameter "ivfflat.probes"`

确认当前数据库已安装并加载 vector extension：

```bash
psql -d pgvector_benchmark -c 'CREATE EXTENSION IF NOT EXISTS vector;'
```

Runner 会先加载 extension，再设置 `ivfflat.probes`；如果仍失败，应检查客户端实际连接的
数据库和 extension 安装版本。

### Structure 显示 unavailable

确认数据库中的 vector 为 0.8.7，并检查两个 Structure 函数：

```sql
SELECT extversion FROM pg_extension WHERE extname = 'vector';
SELECT to_regprocedure('ivfflat_index_stats(regclass)');
SELECT to_regprocedure('ivfflat_list_stats(regclass,real)');
```

### Build 被 memory preflight 跳过

先阅读报告中的 estimated checked memory 和实际 `maintenance_work_mem`。确认机器资源允许后，
提高实验会话参数重新运行。`--on-predicted-memory-failure run` 只适合故障验证，不建议作为
正常实验默认值。

### `/proc`、PSS 或 IO timing 不可用

这类问题不一定代表数据库构建失败。报告会把缺失指标标记为 `unavailable` 并说明原因。
PSS 依赖 `/proc/<pid>/smaps_rollup` 权限；PG IO timing 依赖 `track_io_timing`。

## 10. 目录说明

```text
benchmark/
├── benchmark                 # 用户命令入口
├── Makefile                  # 自动化入口
├── requirements.txt          # Python 依赖
├── common/                   # 数据、数据库、schema、质量计算基础层
├── ivfflat_diagnosis/        # P0/P1 主实现
├── scripts/                  # setup、download、Smoke 和结果校验
├── tests/                    # Python 自动测试
├── docs/                     # 需求、指标、功能设计和冻结记录
├── data/                     # 真实数据集，Git ignored
├── results/                  # Diagnosis 结果，Git ignored
├── pgdata/                   # 隔离 cluster，Git ignored
├── .runtime/                 # Smoke 数据，Git ignored
└── .venv/                    # Python 环境，Git ignored
```

设计资料位于 `docs/`，P1 冻结边界见 `docs/P1-FREEZE.md`。
