#!/usr/bin/env bash
# 对 PostgreSQL 依次执行 migrations/*.up.sql（与 Order Service 内嵌迁移一致）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATABASE_URL="${DATABASE_URL:-postgres://trading:trading@localhost:5432/trading?sslmode=disable}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found; install postgresql-client or start ./bin/order (migrate_on_start=true)" >&2
  exit 1
fi

shopt -s nullglob
files=("${ROOT}"/migrations/*.up.sql)
if ((${#files[@]} == 0)); then
  echo "no migrations/*.up.sql found" >&2
  exit 1
fi

IFS=$'\n' sorted=($(sort <<<"${files[*]}"))
unset IFS

for f in "${sorted[@]}"; do
  echo "applying $(basename "$f") ..."
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done

echo "ok: ${#sorted[@]} migration(s) applied"
