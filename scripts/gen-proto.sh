#!/usr/bin/env bash
# 从仓库根目录执行: ./scripts/gen-proto.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROTO_ROOT="${ROOT}/proto"
OUT_DIR="${ROOT}/pkg/pb"

mkdir -p "$OUT_DIR"

protoc \
  --proto_path="${PROTO_ROOT}" \
  --go_out="${OUT_DIR}" \
  --go_opt=paths=source_relative \
  common/v1/types.proto \
  matching/v1/commands.proto \
  matching/v1/snapshot.proto

echo "ok: generated under ${OUT_DIR}"
