#!/bin/bash
# OpenTenBase K8s 初始化脚本（Operator sidekick init-container 模板）
# 顺序：GTM → DN → CN
# 此脚本作为 Operator 控制器生成的 init-container 入口点
#
# 兼容性说明:
#   - nc (netcat) 用于端口检测，若不可用则回退到 /dev/tcp (bash 内建)
#   - pg_basebackup 超时: 默认 300 秒，可通过 PG_BASEBACKUP_TIMEOUT 环境变量调整
#   - 重试策略: 失败操作最多重试 3 次，间隔 5 秒

set -euo pipefail

ROLE="${ROLE:-unknown}"  # gtm-master | gtm-standby | coordinator | datanode
CLUSTER_NAME="${CLUSTER_NAME:-demo-cluster}"
NAMESPACE="${NAMESPACE:-opentenbase}"
PG_BASEBACKUP_TIMEOUT="${PG_BASEBACKUP_TIMEOUT:-300}"  # 秒
RETRY_COUNT="${RETRY_COUNT:-3}"
RETRY_INTERVAL="${RETRY_INTERVAL:-5}"  # 秒

# ── 通用工具函数 ──
log() { echo "[init] $(date '+%H:%M:%S') $*"; }

# 端口检测：优先使用 nc，回退到 bash /dev/tcp
check_port() {
  local host="$1" port="$2"
  if command -v nc &>/dev/null; then
    nc -z "$host" "$port" 2>/dev/null
  elif [[ -e /dev/tcp ]]; then
    # bash 内建 /dev/tcp (需 bash 编译时启用)
    (echo > /dev/tcp/"$host"/"$port") 2>/dev/null
  else
    # Python fallback (most containers include python3)
    python3 -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('${host}',${port})); s.close()" 2>/dev/null
  fi
}

wait_for_service() {
  local host="$1" port="$2" max="${3:-60}"
  log "等待 $host:$port 可达 (超时: ${max}s) ..."
  for i in $(seq 1 "$max"); do
    if check_port "$host" "$port"; then
      log "$host:$port 已就绪 (${i}s)"
      return 0
    fi
    sleep 1
  done
  log "超时: $host:$port 不可达 (${max}s)"
  return 1
}

# 带重试的命令执行
retry_cmd() {
  local desc="$1" cmd="$2"
  local attempt=0
  while [ $attempt -lt $RETRY_COUNT ]; do
    attempt=$((attempt + 1))
    log "执行: $desc (尝试 $attempt/$RETRY_COUNT)"
    if eval "$cmd"; then
      log "成功: $desc (尝试 $attempt)"
      return 0
    fi
    log "失败: $desc (尝试 $attempt)，${RETRY_INTERVAL}s 后重试"
    sleep $RETRY_INTERVAL
  done
  log "放弃: $desc ($RETRY_COUNT 次尝试均失败)"
  return 1
}

# ── GTM 初始化 ──
init_gtm() {
  log "初始化 GTM Master ..."
  # GTM 不依赖其他组件，直接启动
  if [ ! -f "$PGDATA/gtm.control" ]; then
    initdb -D "$PGDATA" --no-locale --encoding=UTF8
    # 写入 gtm.conf
    cat > "$PGDATA/gtm.conf" <<EOF
listen_addresses = '*'
port = 6666
gtm_standby_mode = off
EOF
    log "GTM initdb 完成"
  else
    log "GTM 数据目录已存在，跳过 initdb"
  fi
}

init_gtm_standby() {
  log "初始化 GTM Standby ..."
  # 等待 GTM Master 就绪
  wait_for_service "${CLUSTER_NAME}-gtm-master" 6666 90  # GTM Standby 需要更长时间等待
  # 从 GTM Master 同步数据 (带超时和重试)
  if [ ! -f "$PGDATA/gtm.control" ]; then
    retry_cmd "pg_basebackup GTM Master" \
      "timeout ${PG_BASEBACKUP_TIMEOUT} pg_basebackup -h '${CLUSTER_NAME}-gtm-master' -p 6666 -D '$PGDATA' -X stream -P"
    # 修改 gtm.conf 为 standby 模式
    sed -i 's/gtm_standby_mode = off/gtm_standby_mode = on/' "$PGDATA/gtm.conf"
    echo "active_host = '${CLUSTER_NAME}-gtm-master'" >> "$PGDATA/gtm.conf"
    echo "active_port = 6666" >> "$PGDATA/gtm.conf"
    log "GTM Standby 同步完成"
  fi
}

# ── DataNode 初始化 ──
init_datanode() {
  local dn_id="${DN_ID:-1}"
  log "初始化 DataNode $dn_id ..."

  # 等待 GTM 就绪
  wait_for_service "${CLUSTER_NAME}-gtm-master" 6666 90

  if [ ! -f "$PGDATA/PG_VERSION" ]; then
    retry_cmd "DN initdb" "initdb -D '$PGDATA' --no-locale --encoding=UTF8"
    # 写入 postgresql.conf（DN 模式）
    cat >> "$PGDATA/postgresql.conf" <<EOF
# OpenTenBase DataNode 配置
pgxc_node_name = 'dn${dn_id}'
gtm_host = '${CLUSTER_NAME}-gtm-master'
gtm_port = 6666
listen_addresses = '*'
port = 5432
EOF
    log "DN initdb 完成"
  else
    log "DN 数据目录已存在，跳过 initdb"
  fi

  # 启动临时 PostgreSQL 用于节点注册 (带超时)
  retry_cmd "DN pg_ctl start" "pg_ctl -D '$PGDATA' -l '$PGDATA/logfile' start -w -t 60"

  # 健康检查: 确认 DN 本地 PostgreSQL 可以接受连接
  log "DN 健康检查 ..."
  for i in $(seq 1 10); do
    if psql -h localhost -p 5432 -d postgres -c "SELECT 1" &>/dev/null; then
      log "DN 健康检查通过 (${i}s)"
      break
    fi
    sleep 1
  done

  # 注册 GTM 连接
  psql -d postgres -c "ALTER NODE dn${dn_id} WITH (TYPE='datanode', HOST='${CLUSTER_NAME}-dn${dn_id}', PORT=5432);" 2>/dev/null || true

  pg_ctl -D "$PGDATA" stop -w -t 30
  log "DataNode $dn_id 初始化完成"
}

# ── Coordinator 初始化 ──
init_coordinator() {
  local cn_id="${CN_ID:-1}"
  log "初始化 Coordinator $cn_id ..."

  # 等待 GTM + 所有 DataNode 就绪
  wait_for_service "${CLUSTER_NAME}-gtm-master" 6666 90
  for i in $(seq 1 "${DN_COUNT:-2}"); do
    wait_for_service "${CLUSTER_NAME}-dn${i}" 5432 90
  done

  if [ ! -f "$PGDATA/PG_VERSION" ]; then
    retry_cmd "CN initdb" "initdb -D '$PGDATA' --no-locale --encoding=UTF8"
    # 写入 postgresql.conf（CN 模式）
    cat >> "$PGDATA/postgresql.conf" <<EOF
# OpenTenBase Coordinator 配置
pgxc_node_name = 'cn${cn_id}'
gtm_host = '${CLUSTER_NAME}-gtm-master'
gtm_port = 6666
listen_addresses = '*'
port = 5432
EOF
    log "CN initdb 完成"
  else
    log "CN 数据目录已存在，跳过 initdb"
  fi

  # 启动临时 PostgreSQL 用于节点注册 (带超时)
  retry_cmd "CN pg_ctl start" "pg_ctl -D '$PGDATA' -l '$PGDATA/logfile' start -w -t 60"

  # 健康检查: 确认 CN 本地 PostgreSQL 可以接受连接
  log "CN 健康检查 ..."
  for i in $(seq 1 10); do
    if psql -h localhost -p 5432 -d postgres -c "SELECT 1" &>/dev/null; then
      log "CN 健康检查通过 (${i}s)"
      break
    fi
    sleep 1
  done

  # 注册 GTM
  psql -d postgres -c "CREATE NODE cn${cn_id} WITH (TYPE='coordinator', HOST='${CLUSTER_NAME}-cn${cn_id}', PORT=5432, PRIMARY=true);" 2>/dev/null || true

  # 注册所有 DataNode (带重试)
  for i in $(seq 1 "${DN_COUNT:-2}"); do
    retry_cmd "注册 DN${i}" \
      "psql -d postgres -c \"CREATE NODE dn${i} WITH (TYPE='datanode', HOST='${CLUSTER_NAME}-dn${i}', PORT=5432, PRIMARY=true);\" 2>/dev/null || \
       psql -d postgres -c \"ALTER NODE dn${i} WITH (HOST='${CLUSTER_NAME}-dn${i}', PORT=5432);\" 2>/dev/null"
  done

  pg_ctl -D "$PGDATA" stop -w -t 30
  log "Coordinator $cn_id 初始化完成"
}

# ── 主入口 ──
case "$ROLE" in
  gtm-master)    init_gtm ;;
  gtm-standby)   init_gtm_standby ;;
  datanode)      init_datanode ;;
  coordinator)   init_coordinator ;;
  *)             log "未知角色: $ROLE"; exit 1 ;;
esac

log "初始化完成，等待 Operator 启动主进程"
