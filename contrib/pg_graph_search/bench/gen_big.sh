#!/usr/bin/env bash
# 并行灌大规模测试数据（支持 OpenTenBase / 原生 PostgreSQL）
#
# 用法:
#   [DIST=replication|shard|none] \
#   [PGHOST=.. PGPORT=11000 PGUSER=opentenbase PGDATABASE=postgres] \
#   bash bench/gen_big.sh <节点数> <每点出边> <分群数> <并发数>
#
# DIST:
#   replication (默认) —— DISTRIBUTE BY REPLICATION，每个 DN 全量图，中小图首选
#   shard              —— DISTRIBUTE BY shard (c1)，大图存储友好，但 CN 递归要跨节点取数
#                         （注意：本编译未开 ENABLE_ALL_TABLE_TYPE，只支持 shard/replication）
#   none               —— 不写分布子句（仅原生 PostgreSQL；OpenTenBase 下会走默认 HASH）
set -euo pipefail
NODES=${1:-20000}
FANOUT=${2:-50}
BUCKETS=${3:-40}
JOBS=${4:-4}
DIST=${DIST:-replication}

case "$DIST" in
  replication) DIST_CLAUSE="DISTRIBUTE BY REPLICATION" ;;
  shard)       DIST_CLAUSE="DISTRIBUTE BY shard (c1)" ;;
  none)        DIST_CLAUSE="" ;;
  *) echo "DIST 只能是 replication | shard | none"; exit 2 ;;
esac

PSQL=${PSQL:-psql}
psql_x() { $PSQL -X -q -v ON_ERROR_STOP=1 "$@"; }

BUCKET_SIZE=$(( NODES / BUCKETS ))
echo ">>> nodes=$NODES fanout=$FANOUT buckets=$BUCKETS jobs=$JOBS dist=$DIST 预计边数=$((NODES*FANOUT/1000000))M"

psql_x -d "${PGDATABASE:-postgres}" -c "
  -- 先删装过实例时生成的 schema（里面是指向 big_edge 的视图），
  -- 否则 DROP TABLE big_edge 会被依赖检查挡下。
  DROP SCHEMA IF EXISTS gbench CASCADE;
  DROP SCHEMA IF EXISTS gbench_u CASCADE;
  DROP TABLE IF EXISTS big_edge CASCADE;
  CREATE UNLOGGED TABLE big_edge (c1 int NOT NULL, c2 int NOT NULL, prop jsonb,
                                  PRIMARY KEY (c1,c2)) ${DIST_CLAUSE};
  ALTER TABLE big_edge SET (autovacuum_enabled=off, toast.autovacuum_enabled=off);
"

STEP=$(( NODES / JOBS ))
pids=()
for ((j=0; j<JOBS; j++)); do
  LO=$(( j * STEP + 1 ))
  HI=$(( (j+1) * STEP ))
  [[ $j -eq $((JOBS-1)) ]] && HI=$NODES
  psql_x -d "${PGDATABASE:-postgres}" \
     -v nodes=$NODES -v fanout=$FANOUT -v buckets=$BUCKETS -v bucket_size=$BUCKET_SIZE \
     -v lo=$LO -v hi=$HI \
     -c "INSERT INTO big_edge (c1,c2,prop)
         SELECT id, ((width_bucket(id,1,$NODES,$BUCKETS)-1)*$BUCKET_SIZE
                     + ceil(random()*($BUCKET_SIZE-1))::int),
                jsonb_build_object('weight', round((random()*1000)::numeric/1000,3))
           FROM generate_series($LO,$HI) s(id), LATERAL generate_series(1,$FANOUT) g
         ON CONFLICT (c1,c2) DO NOTHING;" &
  pids+=($!)
done
for p in "${pids[@]}"; do wait $p; done

echo ">>> vacuum analyze..."
psql_x -d "${PGDATABASE:-postgres}" -c 'VACUUM ANALYZE big_edge;'

echo ">>> 安装图搜索实例..."
psql_x -d "${PGDATABASE:-postgres}" -c "
  CREATE SCHEMA gbench; CREATE SCHEMA gbench_u;
  SELECT graph_search.install(jsonb_build_object('table','public.big_edge','c1','c1','c2','c2',
    'prop','prop','target_schema','gbench','prefix','b','directed',true,'max_depth_cap',10));
  SELECT graph_search.install(jsonb_build_object('table','public.big_edge','c1','c1','c2','c2',
    'prop','prop','target_schema','gbench_u','prefix','bu','directed',false,'max_depth_cap',10));
"

psql_x -d "${PGDATABASE:-postgres}" -c "
  SELECT count(*) AS edges,
         pg_size_pretty(pg_total_relation_size('big_edge')) AS total_size
    FROM big_edge;"

echo ">>> 提醒：分布式下请同时记录 CN 侧 CPU/内存与 pg_stat_statements 里的远程查询次数"
echo "           sudo -u opentenbase psql -p 11000 -c \"SELECT graph_search.registry_distribution();\""
