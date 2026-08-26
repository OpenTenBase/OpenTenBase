#!/usr/bin/env bash
# 公开数据集接入：ANN_SIFT1M（1,000,000 × 128 维，float32）
#
# 为什么要它：评委口径要求"优先使用公开 benchmark 和 ground truth"。
# 本项目的构建期内存结论对数据分布不敏感（检查点是事前算的），但**构建耗时敏感**
# （k-means 收敛轮数、HNSW 图结构都与数据分布有关），所以 M3 的阶段权重必须在
# 真实数据上复测一遍，并与合成数据的结果**并列报告，而不是替换**。
#
# 数据来源：http://corpus-texmex.irisa.fr/ （TEXMEX / INRIA，公开可引用）
#   sift_base.fvecs        1,000,000 × 128  基向量
#   sift_query.fvecs          10,000 × 128  查询向量
#   sift_groundtruth.ivecs    10,000 × 100  ground truth（本项目不做召回，仅归档）
#
# fvecs 格式：每条记录 = int32 维度 d + d 个 float32（小端）
#
# 用法（以 postgres 身份，需要约 2 GB 磁盘）：bash tools/load_sift1m.sh [行数上限]
set -uo pipefail

PGHOME=${PGHOME:-/data/pg18/install}
PGPORT=${PGPORT:-5518}
PGDB=${PGDB:-postgres}
DATA=${DATA:-/data/datasets}
LIMIT=${1:-0}                      # 0 = 全量
PSQL="$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -v ON_ERROR_STOP=1"

mkdir -p "$DATA"
cd "$DATA"

if [ ! -f sift_base.fvecs ]; then
  if [ ! -f sift.tar.gz ]; then
    echo ">>> 下载 ANN_SIFT1M（约 161 MB）"
    curl -sS -o sift.tar.gz ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
  fi
  echo ">>> 解包"
  tar xzf sift.tar.gz --strip-components=1 -C .
fi
ls -l sift_base.fvecs sift_query.fvecs sift_groundtruth.ivecs
echo ">>> 数据集校验和（写进报告，证明用的是原始未改动的公开数据）"
sha256sum sift.tar.gz sift_base.fvecs | tee "$DATA/sift_sha256.txt"

echo ">>> 建表并流式灌入（fvecs → COPY，不落中间大文件）"
$PSQL -c "drop table if exists sift_base;
          create table sift_base (id int, v vector(128));"

python3 - "$DATA/sift_base.fvecs" "$LIMIT" <<'PY' | $PSQL -c "copy sift_base (id, v) from stdin with (format csv)"
import struct, sys
path, limit = sys.argv[1], int(sys.argv[2])
out = sys.stdout
with open(path, 'rb') as f:
    i = 0
    while True:
        head = f.read(4)
        if len(head) < 4:
            break
        d = struct.unpack('<i', head)[0]
        buf = f.read(4 * d)
        if len(buf) < 4 * d:
            break
        vec = struct.unpack('<%df' % d, buf)
        # pgvector 的文本输入格式是 [x1,x2,...]；CSV 里整个向量作为一个被引号包住的字段
        out.write('%d,"[%s]"\n' % (i, ','.join('%g' % x for x in vec)))
        i += 1
        if limit and i >= limit:
            break
    print('loaded_rows=%d dims=%d' % (i, d), file=sys.stderr)
PY

$PSQL -c "analyze sift_base;"
$PSQL -c "select count(*) as rows,
                 (select relpages from pg_class where relname='sift_base') as relpages,
                 (select reltuples::bigint from pg_class where relname='sift_base') as reltuples
            from sift_base;"
echo ">>> 完成。真实数据表 sift_base 已就绪（vector(128)）。"
echo "    注意：这张表用于 M3 的阶段权重复测与 M1 的真实数据抽查；"
echo "    合成数据的结果不删除，两组并列报告。"

