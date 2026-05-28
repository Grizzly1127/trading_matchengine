#!/usr/bin/env bash
# 从仓库根目录执行: ./scripts/gen-proto.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROTO_ROOT="${ROOT}/proto"
OUT_DIR="${ROOT}/pkg/pb"

mkdir -p "$OUT_DIR"

if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  echo "install: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" >&2
  exit 1
fi

protoc \
  --proto_path="${PROTO_ROOT}" \
  --go_out="${OUT_DIR}" \
  --go_opt=paths=source_relative \
  --go-grpc_out="${OUT_DIR}" \
  --go-grpc_opt=paths=source_relative \
  common/v1/types.proto \
  matching/v1/commands.proto \
  matching/v1/snapshot.proto \
  matching/v1/envelope.proto \
  matching/v1/events.proto \
  order/v1/order.proto \
  order/v1/balance.proto \
  marketdata/v1/marketdata.proto

echo "ok: generated under ${OUT_DIR}"
