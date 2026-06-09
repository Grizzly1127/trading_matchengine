#!/usr/bin/env bash
# 采集 PostgreSQL checkpoint / IO 快照，用于 L3 压测前后对比。
#
# 用法:
#   ./scripts/bench/collect-pg-stats.sh reports/xxx/pg-stats-pre.txt
#   ./scripts/bench/collect-pg-stats.sh --delta pre.txt post.txt delta.txt
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATABASE_URL="${DATABASE_URL:-postgres://trading:trading@localhost:5432/trading?sslmode=disable}"

run_psql() {
  if command -v psql >/dev/null 2>&1; then
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -At -F $'\t' "$@"
    return 0
  fi
  local pg_cid
  pg_cid="$(docker ps -qf 'name=postgres' | head -1)"
  if [[ -z "$pg_cid" ]]; then
    pg_cid="$(docker ps -qf 'ancestor=postgres:16-alpine' | head -1)"
  fi
  if [[ -z "$pg_cid" ]]; then
    echo "ERROR: 无法连接 Postgres（无 psql 且无 postgres 容器）" >&2
    return 1
  fi
  docker exec -i "$pg_cid" psql -U trading -d trading -v ON_ERROR_STOP=1 -At -F $'\t' "$@"
}

snapshot() {
  local out="$1"
  mkdir -p "$(dirname "$out")"
  {
    echo "# PostgreSQL checkpoint / IO snapshot"
    echo "collected_at=$(date -Iseconds)"
    echo ""
    echo "## settings"
    run_psql -c "
SELECT name || '=' || setting
FROM pg_settings
WHERE name IN (
  'shared_buffers', 'effective_cache_size', 'max_wal_size', 'min_wal_size',
  'checkpoint_timeout', 'checkpoint_completion_target', 'wal_compression',
  'synchronous_commit', 'random_page_cost', 'effective_io_concurrency',
  'log_checkpoints'
)
ORDER BY name;"
    echo ""
    echo "## pg_stat_bgwriter"
    run_psql -c "
SELECT v FROM (
  SELECT 1 AS o, 'checkpoints_timed=' || checkpoints_timed AS v FROM pg_stat_bgwriter
  UNION ALL SELECT 2, 'checkpoints_req=' || checkpoints_req FROM pg_stat_bgwriter
  UNION ALL SELECT 3, 'checkpoint_write_time_ms=' || round(checkpoint_write_time::numeric, 2) FROM pg_stat_bgwriter
  UNION ALL SELECT 4, 'checkpoint_sync_time_ms=' || round(checkpoint_sync_time::numeric, 2) FROM pg_stat_bgwriter
  UNION ALL SELECT 5, 'buffers_checkpoint=' || buffers_checkpoint FROM pg_stat_bgwriter
  UNION ALL SELECT 6, 'buffers_clean=' || buffers_clean FROM pg_stat_bgwriter
  UNION ALL SELECT 7, 'maxwritten_clean=' || maxwritten_clean FROM pg_stat_bgwriter
  UNION ALL SELECT 8, 'buffers_backend=' || buffers_backend FROM pg_stat_bgwriter
  UNION ALL SELECT 9, 'buffers_backend_fsync=' || buffers_backend_fsync FROM pg_stat_bgwriter
) s ORDER BY o;"
    echo ""
    echo "## pg_stat_database (trading)"
    run_psql -c "
SELECT v FROM (
  SELECT 1 AS o, 'xact_commit=' || xact_commit AS v FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 2, 'xact_rollback=' || xact_rollback FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 3, 'tup_inserted=' || tup_inserted FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 4, 'tup_updated=' || tup_updated FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 5, 'blks_read=' || blks_read FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 6, 'blks_hit=' || blks_hit FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 7, 'blk_read_time_ms=' || round(blk_read_time::numeric, 2) FROM pg_stat_database WHERE datname = 'trading'
  UNION ALL SELECT 8, 'blk_write_time_ms=' || round(blk_write_time::numeric, 2) FROM pg_stat_database WHERE datname = 'trading'
) s ORDER BY o;"
    echo ""
    echo "## hot_tables"
    run_psql -c "
SELECT relname || E'\t' || n_live_tup || E'\t' || n_dead_tup || E'\t' || COALESCE(last_autovacuum::text, '-')
FROM pg_stat_user_tables
WHERE relname IN ('account_balances', 'orders', 'order_outbox', 'client_order_idempotency')
ORDER BY relname;"
  } >"$out" 2>&1 || {
    echo "# ERROR: 采集失败，见 stderr" >"$out"
    return 1
  }
}

write_delta() {
  local pre="$1" post="$2" out="$3"
  awk -v pre="$pre" -v post="$post" '
    function load(path,    f, line, k, v) {
      while ((getline line < path) > 0) {
        if (line ~ /^#/ || line ~ /^$/ || line ~ /^##/) continue
        if (line ~ /^[a-z_]+=/) {
          split(line, a, "=")
          k = a[1]; v = a[2]
          vals[path, k] = v
        }
      }
      close(path)
    }
    BEGIN {
      load(pre); load(post)
      keys["checkpoints_timed"]=1
      keys["checkpoints_req"]=1
      keys["checkpoint_write_time_ms"]=1
      keys["checkpoint_sync_time_ms"]=1
      keys["buffers_checkpoint"]=1
      keys["buffers_clean"]=1
      keys["maxwritten_clean"]=1
      keys["xact_commit"]=1
      keys["tup_inserted"]=1
      keys["tup_updated"]=1
      keys["blks_read"]=1
      keys["blk_read_time_ms"]=1
      keys["blk_write_time_ms"]=1
      print "# PostgreSQL stats delta (post - pre)"
      print "pre=" pre
      print "post=" post
      print ""
      for (k in keys) {
        p = vals[pre, k] + 0
        q = vals[post, k] + 0
        d = q - p
        printf "%s_delta=%s (pre=%s post=%s)\n", k, d, p, q
      }
      print ""
      print "# hint: checkpoints_req_delta>0 且 checkpoint_sync_time_ms_delta 很大 → 压测期间触发强制 checkpoint"
    }
  ' >"$out"
}

usage() {
  cat <<EOF
用法:
  $(basename "$0") <output.txt>
  $(basename "$0") --delta <pre.txt> <post.txt> <delta.txt>
EOF
}

case "${1:-}" in
  --delta)
    [[ $# -eq 4 ]] || { usage; exit 1; }
    write_delta "$2" "$3" "$4"
    ;;
  -h|--help)
    usage
    ;;
  "")
    usage
    exit 1
    ;;
  *)
    snapshot "$1"
    ;;
esac
