#!/usr/bin/env bash
# 生成 dev 用自签 CA + 服务端/客户端证书（仅本地 mTLS 联调）。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/configs/dev-mtls"
mkdir -p "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$OUT/ca.key" -out "$OUT/ca.crt" -days 3650 \
  -subj "/CN=Trading Dev CA"

openssl req -newkey rsa:2048 -nodes -keyout "$OUT/gateway.key" -out "$OUT/gateway.csr" \
  -subj "/CN=gateway.local"
openssl x509 -req -in "$OUT/gateway.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/gateway.crt" -days 825

openssl req -newkey rsa:2048 -nodes -keyout "$OUT/client.key" -out "$OUT/client.csr" \
  -subj "/CN=web-bff"
openssl x509 -req -in "$OUT/client.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/client.crt" -days 825

echo "wrote certs under $OUT"
echo "gateway: cert=$OUT/gateway.crt key=$OUT/gateway.key client_ca=$OUT/ca.crt"
echo "client:  cert=$OUT/client.crt key=$OUT/client.key"
