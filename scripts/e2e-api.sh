#!/usr/bin/env bash
# 联调 / E2E：通过 Gateway REST 覆盖下单、撮合、充值、余额、深度、K 线等。
#
# 前置：./scripts/dev.sh start --build（matching + order + marketdata + kline + push + gateway）
#
# 用法:
#   ./scripts/e2e-api.sh              # 跑全流程
#   ./scripts/e2e-api.sh step health
#   ./scripts/e2e-api.sh step deposit
#   ./scripts/e2e-api.sh step orders
#   ./scripts/e2e-api.sh step market
#   ./scripts/e2e-api.sh step query
#
# 环境变量:
#   BASE_URL          Gateway 地址（默认 http://localhost:8080）
#   TOKEN             Bearer（默认与 configs/gateway.json 一致）
#   SYMBOL            交易对（默认 BTC-USDT）
#   USER_BUYER        买方 user_id（默认 1）
#   USER_SELLER       卖方 user_id（默认 2）
#   MATCH_WAIT_SEC    下单后等待撮合秒数（默认 2）
#   LIMIT_PRICE       限价单价格（默认 65000）
#   LIMIT_QTY         限价单数量（默认 0.01）
#   MARKET_QTY        市价单数量（默认 0.001）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-dev-token-change-me}"
SYMBOL="${SYMBOL:-BTC-USDT}"
USER_BUYER="${USER_BUYER:-1}"
USER_SELLER="${USER_SELLER:-2}"
MATCH_WAIT_SEC="${MATCH_WAIT_SEC:-2}"
LIMIT_PRICE="${LIMIT_PRICE:-65000}"
LIMIT_QTY="${LIMIT_QTY:-0.01}"
MARKET_QTY="${MARKET_QTY:-0.001}"

RUN_ID="${RUN_ID:-$(date +%s)}"
STEP_FILTER="${1:-all}"
[[ "${1:-}" == "step" ]] && STEP_FILTER="${2:-all}"

log() { printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

have_jq() { command -v jq >/dev/null 2>&1; }

pretty() {
  if have_jq; then jq .; else cat; fi
}

# curl 封装：自动加鉴权头（公开 GET 也可带）
api() {
  local method=$1
  shift
  curl -sS -X "$method" "$@" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Accept: application/json"
}

api_json() {
  local method=$1
  shift
  api "$method" "$@" -H "Content-Type: application/json"
}

check_code_zero() {
  local body=$1
  if have_jq; then
    local code
    code="$(echo "$body" | jq -r '.code // -1')"
    [[ "$code" == "0" ]] || die "api failed: $(echo "$body" | jq -c '.')"
  else
    echo "$body" | grep -q '"code":0' || die "api failed (install jq for details): $body"
  fi
}

step_health() {
  log "=== 健康检查 / 时间 ==="
  api GET "${BASE_URL}/v1/health" | pretty
  api GET "${BASE_URL}/v1/time" | pretty
}

step_deposit() {
  log "=== 用户充值（调账）==="
  # 买方：USDT；卖方：USDT + BTC（用于卖单）
  local bid
  bid=$((RUN_ID % 1000000))
  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_BUYER},
    \"asset\": \"USDT\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 1)),
    \"change\": \"100000\"
  }" | tee /tmp/e2e_deposit_buyer_usdt.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_buyer_usdt.log)"

  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_SELLER},
    \"asset\": \"USDT\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 2)),
    \"change\": \"100000\"
  }" | tee /tmp/e2e_deposit_seller_usdt.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_seller_usdt.log)"

  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_SELLER},
    \"asset\": \"BTC\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 3)),
    \"change\": \"10\"
  }" | tee /tmp/e2e_deposit_seller_btc.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_seller_btc.log)"

  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_BUYER},
    \"asset\": \"BTC\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 4)),
    \"change\": \"1\"
  }" | tee /tmp/e2e_deposit_buyer_btc.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_buyer_btc.log)"
}

step_balances() {
  log "=== 资产查询 ==="
  log "买方 user_id=${USER_BUYER}"
  api GET "${BASE_URL}/v1/balances?user_id=${USER_BUYER}" | pretty
  api GET "${BASE_URL}/v1/balances/USDT?user_id=${USER_BUYER}" | pretty

  log "卖方 user_id=${USER_SELLER}"
  api GET "${BASE_URL}/v1/balances?user_id=${USER_SELLER}" | pretty
  api GET "${BASE_URL}/v1/balances/BTC?user_id=${USER_SELLER}" | pretty
}

place_order() {
  local user_id=$1
  local client_id=$2
  local side=$3
  local type=$4
  local price=${5:-}
  local qty=$6
  local body
  if [[ -n "$price" ]]; then
    body=$(cat <<EOF
{
  "user_id": ${user_id},
  "client_order_id": "${client_id}",
  "symbol": "${SYMBOL}",
  "side": "${side}",
  "type": "${type}",
  "price": "${price}",
  "quantity": "${qty}",
  "time_in_force": "GTC"
}
EOF
)
  else
    body=$(cat <<EOF
{
  "user_id": ${user_id},
  "client_order_id": "${client_id}",
  "symbol": "${SYMBOL}",
  "side": "${side}",
  "type": "${type}",
  "quantity": "${qty}",
  "time_in_force": "IOC"
}
EOF
)
  fi
  api_json POST "${BASE_URL}/v1/orders" -d "$body"
}

step_orders() {
  log "=== 限价单 + 撮合（卖单先入簿，买单同价成交）==="
  local sell_coid="e2e-${RUN_ID}-sell-limit"
  local buy_coid="e2e-${RUN_ID}-buy-limit"

  log "卖方挂限价卖 ${LIMIT_QTY} @ ${LIMIT_PRICE}"
  place_order "$USER_SELLER" "$sell_coid" "SELL" "LIMIT" "$LIMIT_PRICE" "$LIMIT_QTY" \
    | tee /tmp/e2e_sell_limit.log | pretty
  check_code_zero "$(cat /tmp/e2e_sell_limit.log)"

  log "等待 ${MATCH_WAIT_SEC}s ..."
  sleep "$MATCH_WAIT_SEC"

  log "深度（应有卖盘）"
  api GET "${BASE_URL}/v1/market/depth?symbol=${SYMBOL}&limit=10" | pretty

  log "买方挂限价买 ${LIMIT_QTY} @ ${LIMIT_PRICE}（预期与卖单撮合）"
  place_order "$USER_BUYER" "$buy_coid" "BUY" "LIMIT" "$LIMIT_PRICE" "$LIMIT_QTY" \
    | tee /tmp/e2e_buy_limit.log | pretty
  check_code_zero "$(cat /tmp/e2e_buy_limit.log)"

  if have_jq; then
    LAST_ORDER_ID="$(jq -r '.data.order_id // empty' /tmp/e2e_buy_limit.log)"
    export LAST_ORDER_ID
  fi

  log "等待撮合 ${MATCH_WAIT_SEC}s ..."
  sleep "$MATCH_WAIT_SEC"
}

step_market() {
  log "=== 市价单 ==="
  # 先补一笔卖单提供流动性，再市价买
  local sell_coid="e2e-${RUN_ID}-sell-for-market"
  log "卖方限价卖 ${LIMIT_QTY} @ ${LIMIT_PRICE}（供市价买吃单）"
  place_order "$USER_SELLER" "$sell_coid" "SELL" "LIMIT" "$LIMIT_PRICE" "$LIMIT_QTY" \
    | tee /tmp/e2e_sell_for_market.log | pretty
  check_code_zero "$(cat /tmp/e2e_sell_for_market.log)"
  sleep "$MATCH_WAIT_SEC"

  local market_coid="e2e-${RUN_ID}-buy-market"
  log "买方市价买 ${MARKET_QTY}（IOC，无需 price）"
  place_order "$USER_BUYER" "$market_coid" "BUY" "MARKET" "" "$MARKET_QTY" \
    | tee /tmp/e2e_buy_market.log | pretty
  check_code_zero "$(cat /tmp/e2e_buy_market.log)"

  sleep "$MATCH_WAIT_SEC"
}

step_query() {
  log "=== 行情：深度 / Ticker / K 线 ==="
  api GET "${BASE_URL}/v1/market/depth?symbol=${SYMBOL}&limit=20" | pretty
  api GET "${BASE_URL}/v1/market/ticker?symbol=${SYMBOL}" | pretty
  api GET "${BASE_URL}/v1/klines?symbol=${SYMBOL}&interval=1m&limit=10" | pretty
  api GET "${BASE_URL}/v1/klines?symbol=${SYMBOL}&interval=1s&limit=5" | pretty

  log "=== 订单查询 ==="
  api GET "${BASE_URL}/v1/orders?user_id=${USER_BUYER}&symbol=${SYMBOL}&limit=20" | pretty
  if [[ -n "${LAST_ORDER_ID:-}" ]]; then
    log "单笔订单 order_id=${LAST_ORDER_ID}"
    api GET "${BASE_URL}/v1/orders/${LAST_ORDER_ID}?user_id=${USER_BUYER}&symbol=${SYMBOL}" | pretty
  fi

  log "=== 撮合后余额 ==="
  step_balances
}

run_step() {
  case "$1" in
    health) step_health ;;
    deposit) step_deposit ;;
    balances) step_balances ;;
    orders) step_orders ;;
    market) step_market ;;
    query) step_query ;;
    all)
      step_health
      step_deposit
      step_balances
      step_orders
      step_market
      step_query
      log "=== E2E 完成 ==="
      ;;
    *)
      die "unknown step: $1 (use: all|health|deposit|balances|orders|market|query)"
      ;;
  esac
}

usage() {
  cat <<EOF
用法: $(basename "$0") [step <name>|all]

步骤:
  health    健康检查、服务时间
  deposit   双用户充值（USDT/BTC）
  balances  资产列表与单资产查询
  orders    限价卖 + 限价买（撮合）
  market    限价卖 + 市价买
  query     深度、Ticker、K 线、订单列表、余额

环境: BASE_URL=${BASE_URL}  TOKEN=***  SYMBOL=${SYMBOL}
      USER_BUYER=${USER_BUYER}  USER_SELLER=${USER_SELLER}

详见: scripts/e2e-api.md
EOF
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi
  log "E2E API  BASE_URL=${BASE_URL}  SYMBOL=${SYMBOL}  RUN_ID=${RUN_ID}"
  run_step "$STEP_FILTER"
}

main "$@"
