#!/usr/bin/env bash
# 验证两个诊断对象在本机可复现，并把报错/NOTICE 原文存档。
# 这是"环境是否可用"的真正判据，搭建完必须跑一次。
# 用法：bash 02_verify_phenomena.sh [run_id]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
RUN_ID=${1:-$(date +%Y%m%d-%H%M%S)}
OUT=/data/artifacts/$RUN_ID/phenomena
mkdir -p "$OUT"
PSQL="$PGHOME/bin/psql -p $PGPORT -d postgres -v ON_ERROR_STOP=0"

echo "run_id=$RUN_ID  输出目录=$OUT"

echo ">>> 现象一：IVFFlat 构建内存不足（预期 ERROR: memory required is N MB）"
# 预期命中的是 C2 检查点（ivfbuild.c:459，centers+samples 之后），不是 C3。
# 粗算（ABI 常数按 H=32、MAXALIGN(itemsize)=520 估，T0.6 实测后回填）：
#   C1 = 32 + 1000*520              ≈ 508 kB   → 不越 1MB，不触发
#   C2 = C1 + 32 + numSamples*520   ≈ 21.6 MB  → 越界，报错约 22 MB
#   其中 numSamples = min(max(1000*50,10000), relpages*MaxHeapTuplesPerPage)
# 所以本脚本证明的是"检查点存在且数字可预测"，**没有走到 kmeans 的 9 项**。
# M1 验证矩阵要覆盖 9 项时，maintenance_work_mem 必须落在 C2 与 C3 之间的窗口里，
# 这个窗口可能很窄，必须先用模型算出来再设参数。详见 02-设计文档/M1-* 第 5 节。
$PSQL <<'SQL' > "$OUT/ivfflat_mem.out" 2> "$OUT/ivfflat_mem.err"
\timing on
drop table if exists t_mem;
create table t_mem (id int, v vector(128));
insert into t_mem
select i, (select array_agg(random())::vector(128) from generate_series(1,128))
from generate_series(1,2000) i;
analyze t_mem;
set maintenance_work_mem = '1MB';
show maintenance_work_mem;
create index t_mem_ivf on t_mem using ivfflat (v vector_l2_ops) with (lists = 1000);
SQL
if grep -q 'memory required is' "$OUT/ivfflat_mem.err"; then
  echo "  [OK] 捕获到源码级检查点报错："
  grep 'memory required is' "$OUT/ivfflat_mem.err" | sed 's/^/    /'
else
  echo "  [WARN] 未捕获到该报错。可能 lists 太小或内存参数未生效，检查 $OUT/ivfflat_mem.err"
fi

echo ">>> 现象二：HNSW 图放不下 maintenance_work_mem 后落盘降级（预期 NOTICE）"
$PSQL <<'SQL' > "$OUT/hnsw_spill.out" 2> "$OUT/hnsw_spill.err"
\timing on
set client_min_messages = notice;
drop table if exists t_hnsw;
create table t_hnsw (id int, v vector(512));
insert into t_hnsw
select i, (select array_agg(random())::vector(512) from generate_series(1,512))
from generate_series(1,50000) i;
analyze t_hnsw;
set maintenance_work_mem = '32MB';
show maintenance_work_mem;
create index t_hnsw_g on t_hnsw using hnsw (v vector_l2_ops);
SQL
if grep -qi 'no longer fits into maintenance_work_mem' "$OUT/hnsw_spill.out" "$OUT/hnsw_spill.err"; then
  echo "  [OK] 捕获到图落盘降级 NOTICE："
  grep -i -A2 'no longer fits' "$OUT/hnsw_spill.out" "$OUT/hnsw_spill.err" | sed 's/^/    /'
else
  echo "  [WARN] 未捕获到该 NOTICE。加大行数或维度、或调小 maintenance_work_mem 再试"
  echo "         注意 client_min_messages 与 log_min_messages 都要能放过 notice"
fi

echo ">>> 现象三：构建进度可观测（上游已有能力，用于划清边界）"
# 必须先 LOAD 'vector'：GUC 在 _PG_init() 里注册，共享库是惰性加载的。
# 不 LOAD 就查 pg_settings 会得到空结果，误以为"上游没有任何 GUC"——这是假阴性。
$PSQL -Atc "load 'vector'; select name||' = '||setting from pg_settings
            where name like 'ivfflat.%' or name like 'hnsw.%' order by name;" \
  > "$OUT/upstream_gucs.txt" 2>&1
if [ -s "$OUT/upstream_gucs.txt" ]; then
  echo "  上游 GUC 清单（$(grep -c . "$OUT/upstream_gucs.txt") 个）-> $OUT/upstream_gucs.txt"
  sed 's/^/    /' "$OUT/upstream_gucs.txt"
else
  echo "  [WARN] GUC 清单为空。确认 LOAD 'vector' 是否成功，不要直接当成'上游没有 GUC'"
fi
$PSQL -c "\d+ pg_stat_progress_create_index" > "$OUT/progress_view.txt" 2>&1
echo "  进度视图定义 -> $OUT/progress_view.txt"
echo "  注意：这两项是 PostgreSQL/pgvector 自带能力，报告中不得宣称原创。"

echo ">>> 清理测试表"
$PSQL -c 'drop table if exists t_mem; drop table if exists t_hnsw;' >/dev/null 2>&1

echo ">>> 归档哈希"
( cd "$OUT" && sha256sum ./* > SHA256SUMS 2>/dev/null; cat SHA256SUMS )

echo
echo "完成。两个 [OK] 都出现则环境就绪。"
echo "所有 stderr 原文已存档，写文档时原样引用，不要转述或翻译。"
