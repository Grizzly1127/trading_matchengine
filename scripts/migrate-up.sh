#!/usr/bin/env bash
# 对 PostgreSQL 依次执行 migrations/*.up.sql（与 Order Service 内嵌迁移一致）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATABASE_URL="${DATABASE_URL:-postgres://trading:trading@localhost:5432/trading?sslmode=disable}"

shopt -s nullglob
files=("${ROOT}"/migrations/*.up.sql)
if ((${#files[@]} == 0)); then
  echo "no migrations/*.up.sql found" >&2
  exit 1
fi

IFS=$'\n' sorted=($(sort <<<"${files[*]}"))
unset IFS

# 本机无 psql 时回退到 postgres 容器内执行（与 reset-dev.sh::reset_postgres 行为一致）
PG_CID=""
if ! command -v psql >/dev/null 2>&1; then
  PG_CID="$(docker ps -qf 'name=postgres' | head -1)"
  if [[ -z "$PG_CID" ]]; then
    PG_CID="$(docker ps -qf 'ancestor=postgres:16-alpine' | head -1)"
  fi
  if [[ -z "$PG_CID" ]]; then
    echo "psql not found 且未发现 postgres 容器；请安装 postgresql-client 或启动 docker compose postgres" >&2
    exit 1
  fi
fi

for f in "${sorted[@]}"; do
  echo "applying $(basename "$f") ..."
  if [[ -n "$PG_CID" ]]; then
    docker exec -i "$PG_CID" psql -U trading -d trading -v ON_ERROR_STOP=1 <"$f"
  else
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
  fi
done

echo "ok: ${#sorted[@]} migration(s) applied"
