#!/usr/bin/env bash
# T2.7 · 构建参数扫描：把"参数建议表"里的每个数字都变成本机实测
#
# 为什么需要它：M1/M2 只回答"会不会失败/会不会降级"，不回答"那我该用什么参数"。
# 参数建议要站得住，必须同时给出**代价**与**收益**两侧的实测：
#   代价侧 = 构建耗时、索引体积、图内存（本项目职责范围内）
#   收益侧 = 召回率（属方向一的指标，这里**只作为参数取舍的质量轴**引用，
#            不作为方向一交付；报告里必须这么写）
#
# ground truth 口径（关键，评审会问）：
#   公开数据集 ANN_SIFT1M 自带的 sift_groundtruth.ivecs 是针对**全量 100 万底库**的，
#   本脚本用的是子集，所以自带 ground truth 在这里**不成立**，不能直接拿来用。
#   因此 exact top-K 在库内用顺序扫描重算（enable_indexscan=off），
#   这是子集场景下唯一正确的 ground truth 来源。查询向量仍取公开 query 集，
#   不自己造查询。
#
# 用法（以 postgres 身份）：bash tools/param_sweep.sh [run_id] [行数] [重复次数]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
DATA=${DATA:-/data/datasets}
RUN_ID=${1:-t27-$(date +%Y%m%d-%H%M%S)}
ROWS=${2:-100000}
REPEATS=${3:-3}
NQ=${NQ:-100}                      # 参与召回测量的查询条数
TOPK=${TOPK:-10}
MWM=${MWM:-512MB}                  # 固定住，保证各配置的耗时可比（并记进 CSV）
OUT=${OUTDIR:-/data/artifacts/$RUN_ID}
mkdir -p "$OUT"
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=0"

# 数值参数白名单（T4.6：恶意输入在进 psql 之前就被拒）
for v in ROWS REPEATS NQ TOPK; do
  case "${!v}" in ''|*[!0-9]*) echo "[FAIL] $v 必须是十进制整数" >&2; exit 2;; esac
done

CSV="$OUT/param_sweep.csv"
echo "am,m,ef_construction,lists,rows,dims,mwm,iter,build_ms,index_mb,spilled,recall_at_k,query_ms_mean" > "$CSV"

say() { printf '>>> %s\n' "$*"; }

# ---------------------------------------------------------------------------
# 1) 子集底库 + 公开查询集
# ---------------------------------------------------------------------------
say "准备子集底库 ps_base（$ROWS 行，取自公开数据集 sift_base 的前 $ROWS 条）"
$PSQL -c "drop table if exists ps_base;
          create table ps_base as select id, v from sift_base order by id limit $ROWS;
          analyze ps_base;" || exit 1

say "灌入公开查询集 sift_query.fvecs 的前 $NQ 条（查询向量不自造）"
$PSQL -c "drop table if exists ps_query; create table ps_query (qid int, v vector(128));" || exit 1
python3 - "$DATA/sift_query.fvecs" "$NQ" <<'PY' | $PSQL -c "copy ps_query (qid, v) from stdin with (format csv)"
import struct, sys
path, limit = sys.argv[1], int(sys.argv[2])
with open(path, 'rb') as f:
    for i in range(limit):
        head = f.read(4)
        if len(head) < 4:
            break
        d = struct.unpack('<i', head)[0]
        buf = f.read(4 * d)
        vec = struct.unpack('<%df' % d, buf)
        sys.stdout.write('%d,"[%s]"\n' % (i, ','.join('%g' % x for x in vec)))
PY
$PSQL -Atc "select count(*) from ps_query;"

# ---------------------------------------------------------------------------
# 2) exact ground truth：库内顺序扫描重算（子集场景下自带 gt 不适用）
# ---------------------------------------------------------------------------
say "重算 exact top-$TOPK ground truth（顺序扫描，无索引）"
$PSQL -c "drop table if exists ps_gt;
          create table ps_gt (qid int, rnk int, id int);" || exit 1
# 先取 top-K 再编号：把 row_number() 放在 limit 外面会对全表做窗口计算，
# 100 个查询下白烧几分钟。rnk 只用于人工查看，召回计算只用 id 集合。
$PSQL -c "set enable_indexscan=off; set enable_bitmapscan=off; set jit=off;
          insert into ps_gt
          select q.qid, row_number() over (partition by q.qid), g.id
          from ps_query q
          cross join lateral (
            select b.id from ps_base b order by b.v <-> q.v limit $TOPK
          ) g;" || exit 1
$PSQL -Atc "select count(*) from ps_gt;"

# ---------------------------------------------------------------------------
# 3) 一次"建索引 + 量体积 + 量召回"
#    降级 NOTICE 直接从 psql 的 stderr 里抓，不改 pgvector、不重编译
# ---------------------------------------------------------------------------
build_and_score() {
  local am=$1 param=$2 probe_gucs=$3 log=$4
  local t0 t1 ms
  $PSQL -c "drop index if exists ps_ix;" >/dev/null 2>&1
  t0=$(date +%s%3N)
  $PSQL -c "set maintenance_work_mem='$MWM';
            create index ps_ix on ps_base using $am (v vector_l2_ops) with ($param);" \
        >"$log.out" 2>"$log.err"
  t1=$(date +%s%3N)
  ms=$((t1 - t0))
  local spilled=no
  grep -qi 'hnsw graph no longer fits' "$log.err" && spilled=yes
  local mb
  mb=$($PSQL -Atc "select round(pg_relation_size('ps_ix')/1048576.0, 1);" 2>/dev/null)

  # 召回：索引 top-K 与 exact top-K 的交集比例，按查询取平均
  local rc
  rc=$($PSQL -Atc "set enable_seqscan=off; $probe_gucs
        with hit as (
          select q.qid, g.id
          from ps_query q
          cross join lateral (
            select b.id from ps_base b order by b.v <-> q.v limit $TOPK
          ) g)
        select round(avg(c), 4) from (
          select h.qid, count(*) filter (where t.id is not null)::numeric / $TOPK as c
          from hit h left join ps_gt t on t.qid = h.qid and t.id = h.id
          group by h.qid) s;" 2>/dev/null)

  # 单查询平均耗时：**在库内用 clock_timestamp 计时**，不用 shell 包 psql 的墙钟——
  # 后者把 psql 进程启动与建连（本机约 100 ms）算进了 100 条查询里，会把 0.6 ms 这种
  # 明显不可能的数字算出来。冒烟测试就踩到了这个坑，故改成 DO 块内计时。
  local qms
  qms=$($PSQL -Atc "set enable_seqscan=off; $probe_gucs
        do \$\$
        declare t0 timestamptz := clock_timestamp(); c int := 0; r record;
        begin
          for r in select v from ps_query loop
            perform b.id from ps_base b order by b.v <-> r.v limit $TOPK;
            c := c + 1;
          end loop;
          raise notice 'QUERY_MS_MEAN=%',
            round(extract(epoch from clock_timestamp() - t0) * 1000 / greatest(c, 1), 3);
        end \$\$;" 2>&1 | sed -n 's/.*QUERY_MS_MEAN=\([0-9.]*\).*/\1/p')
  [ -n "$qms" ] || qms=NA

  # 一次性留证：召回查询确实走了索引（否则"召回 1.0"可能只是顺序扫描的假象）
  if [ ! -s "$OUT/explain_${am}.txt" ]; then
    $PSQL -c "set enable_seqscan=off; $probe_gucs
              explain (costs off) select b.id from ps_base b
              order by b.v <-> (select v from ps_query limit 1) limit $TOPK;" \
          > "$OUT/explain_${am}.txt" 2>&1
  fi

  echo "$ms|$mb|$spilled|$rc|$qms"
}

emit() {
  local am=$1 m=$2 ef=$3 lists=$4 iter=$5 r=$6
  IFS='|' read -r ms mb sp rc qms <<<"$r"
  echo "$am,$m,$ef,$lists,$ROWS,128,$MWM,$iter,$ms,$mb,$sp,$rc,$qms" >> "$CSV"
  printf '    第 %s 次：%s ms，索引 %s MB，降级=%s，recall@%s=%s，单查询 %s ms\n' \
    "$iter" "$ms" "$mb" "$sp" "$TOPK" "$rc" "$qms"
}

# ---------------------------------------------------------------------------
# 4) HNSW 网格：m × ef_construction
#    ef_search 固定在上游默认 40，否则"召回变化"分不清是构建参数还是查询参数带来的
# ---------------------------------------------------------------------------
HNSW_GUCS="set hnsw.ef_search=40;"
for m in ${M_LIST:-8 16 32}; do
  for ef in ${EF_LIST:-64 200}; do
    say "HNSW m=$m ef_construction=$ef"
    for i in $(seq 1 "$REPEATS"); do
      emit hnsw "$m" "$ef" "" "$i" \
        "$(build_and_score hnsw "m=$m, ef_construction=$ef" "$HNSW_GUCS" "$OUT/hnsw_m${m}_ef${ef}_$i")"
    done
  done
done

# ---------------------------------------------------------------------------
# 5) IVFFlat lists 扫描
#    档位来自上游 README 的经验式：rows<=100 万取 rows/1000，超过取 sqrt(rows)。
#    两个都算出来一起测，才能说"经验式在本机成立/不成立"。
# ---------------------------------------------------------------------------
L_RULE=$((ROWS / 1000))
L_SQRT=$(awk -v r="$ROWS" 'BEGIN{printf "%d", sqrt(r)}')
for lists in ${LISTS_LIST:-$L_RULE $L_SQRT 1000}; do
  say "IVFFlat lists=$lists（probes=10）"
  for i in $(seq 1 "$REPEATS"); do
    emit ivfflat "" "" "$lists" "$i" \
      "$(build_and_score ivfflat "lists=$lists" "set ivfflat.probes=10;" "$OUT/ivf_l${lists}_$i")"
  done
done

$PSQL -c "drop index if exists ps_ix;" >/dev/null 2>&1

# ---------------------------------------------------------------------------
# 6) 汇总：每配置 min/median/max + 召回
# ---------------------------------------------------------------------------
say "汇总（构建耗时 min/median/max，召回取各次均值）"
STAT="$OUT/param_sweep_stats.csv"
echo "am,param,n,build_min_ms,build_median_ms,build_max_ms,index_mb,spilled,recall_at_k,query_ms_mean" > "$STAT"
awk -F, -v OFS=, 'NR>1 {
    key = ($1=="hnsw") ? sprintf("hnsw,m=%s;ef_construction=%s", $2, $3) : sprintf("ivfflat,lists=%s", $4)
    n[key]++; t[key,n[key]]=$9; mb[key]=$10; sp[key]=$11; rc[key]+=$12; qm[key]+=$13
  }
  END {
    for (k in n) {
      c = n[k]
      # 插入排序取中位数
      for (i=1;i<=c;i++) v[i]=t[k,i]+0
      for (i=2;i<=c;i++) { x=v[i]; j=i-1; while (j>0 && v[j]>x) { v[j+1]=v[j]; j-- } v[j+1]=x }
      printf "%s,%d,%d,%d,%d,%s,%s,%.4f,%.2f\n", k, c, v[1], v[int((c+1)/2)], v[c], mb[k], sp[k], rc[k]/c, qm[k]/c
      delete v
    }
  }' "$CSV" | sort >> "$STAT"
cat "$STAT"

( cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS )
say "原始数据 -> $CSV；统计 -> $STAT"
say "把统计结果写进参数建议表：bash tools/load_param_facts.sh $OUT/param_sweep_stats.csv"
