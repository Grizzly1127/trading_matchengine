#!/usr/bin/env bash
# 对 docker compose PostgreSQL 执行 migrations（001_create_orders）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATABASE_URL="${DATABASE_URL:-postgres://trading:trading@localhost:5432/trading?sslmode=disable}"

if command -v psql >/dev/null 2>&1; then
  psql "$DATABASE_URL" -f "${ROOT}/migrations/001_create_orders.up.sql"
  echo "ok: migrations applied via psql"
  exit 0
fi

echo "psql not found; run migrations via order service (migrate_on_start=true) or install postgresql-client" >&2
exit 1
