#!/usr/bin/env bash
# 联调 / E2E：通过 Gateway REST 覆盖下单、撮合、充值、余额、深度、K 线等。
#
# 前置：./scripts/dev.sh start --build（matching + order + marketdata + kline + push + gateway）
#
# 用法:
#   ./scripts/e2e-api.sh              # static token 全流程（需 jq）
#   ./scripts/e2e-api.sh jwt          # 从 auth 取 JWT 后跑全流程（需 dev.sh --auth --jwt）
#   ./scripts/e2e-api.sh step health
#   ./scripts/e2e-api.sh step deposit
#   ./scripts/e2e-api.sh step orders
#   ./scripts/e2e-api.sh step market
#   ./scripts/e2e-api.sh step query
#
# 环境变量:
#   BASE_URL          Gateway 地址（默认 http://localhost:8080）
#   E2E_AUTH          static（默认）| jwt
#   AUTH_URL          JWT 签发地址（默认 http://localhost:8090）
#   AUTH_CLIENT_ID    默认 web-bff
#   AUTH_CLIENT_SECRET 默认与 configs/auth.json 一致
#   TOKEN             Bearer（static 时默认 dev-token-change-me；jwt 时自动换取）
#   MM_TOKEN          做市商 static token（默认 dev-mm-token-change-me，用于 ticker/all）
#   SYMBOL            交易对（默认 BTC-USDT）
#   USER_BUYER        买方 user_id（默认 1）
#   USER_SELLER       卖方 user_id（默认 2）
#   PIPELINE_WAIT_SEC  Outbox→撮合→行情 轮询超时（默认 30）
#   PIPELINE_POLL_SEC  轮询间隔秒（默认 1）
#   MATCH_WAIT_SEC     已废弃，请用 PIPELINE_WAIT_SEC
#   LIMIT_PRICE       限价单价格（默认 65000）
#   LIMIT_QTY         限价单数量（默认 0.01）
#   MARKET_QTY        市价单数量（默认 0.001）
#   SKIP_ASSERT       设为 1 时仅打印响应、不做 jq 断言（无 jq 时自动等同）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-dev-token-change-me}"
MM_TOKEN="${MM_TOKEN:-dev-mm-token-change-me}"
SYMBOL="${SYMBOL:-BTC-USDT}"
USER_BUYER="${USER_BUYER:-1}"
USER_SELLER="${USER_SELLER:-2}"
PIPELINE_WAIT_SEC="${PIPELINE_WAIT_SEC:-${MATCH_WAIT_SEC:-30}}"
PIPELINE_POLL_SEC="${PIPELINE_POLL_SEC:-1}"
LIMIT_PRICE="${LIMIT_PRICE:-65000}"
LIMIT_QTY="${LIMIT_QTY:-0.01}"
MARKET_QTY="${MARKET_QTY:-0.001}"

RUN_ID="${RUN_ID:-$(date +%s)}"
E2E_AUTH_MODE="${E2E_AUTH:-static}"
AUTH_URL="${AUTH_URL:-http://localhost:8090}"
AUTH_CLIENT_ID="${AUTH_CLIENT_ID:-web-bff}"
AUTH_CLIENT_SECRET="${AUTH_CLIENT_SECRET:-dev-client-secret-change-me}"

# 解析首个参数：jwt | step ...
if [[ "${1:-}" == "jwt" ]]; then
  E2E_AUTH_MODE=jwt
  shift
fi

STEP_FILTER="${1:-all}"
[[ "${1:-}" == "step" ]] && STEP_FILTER="${2:-all}"

log() { printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

have_jq() { command -v jq >/dev/null 2>&1; }

ASSERT=1
if [[ "${SKIP_ASSERT:-0}" == "1" ]]; then
  ASSERT=0
elif ! have_jq; then
  if [[ "$STEP_FILTER" == "all" ]]; then
    die "full e2e (step all) requires jq; install jq or set SKIP_ASSERT=1"
  fi
  ASSERT=0
fi

pretty() {
  if have_jq; then jq .; else cat; fi
}

init_auth_token() {
  if [[ "$E2E_AUTH_MODE" != "jwt" ]]; then
    TOKEN="${TOKEN:-dev-token-change-me}"
    return 0
  fi
  have_jq || die "jwt e2e requires jq"
  log "=== 换取服务 JWT (${AUTH_URL}) ==="
  local body
  body="$(curl -sS -X POST "${AUTH_URL}/v1/token" \
    -H "Content-Type: application/json" \
    -d "{\"client_id\":\"${AUTH_CLIENT_ID}\",\"client_secret\":\"${AUTH_CLIENT_SECRET}\"}")"
  echo "$body" | pretty
  local err
  err="$(echo "$body" | jq -r '.error // empty')"
  [[ -z "$err" ]] || die "auth token: $err"
  TOKEN="$(echo "$body" | jq -r '.access_token // empty')"
  [[ -n "$TOKEN" ]] || die "auth token: empty access_token (is auth running? dev.sh start --auth --jwt)"
  export TOKEN
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
  local ctx=${2:-api}
  if [[ "$ASSERT" != "1" ]]; then
    return 0
  fi
  local code
  code="$(echo "$body" | jq -r '.code // -1')"
  [[ "$code" == "0" ]] || die "${ctx}: code=${code} body=$(echo "$body" | jq -c '.')"
}

assert_jq() {
  local body=$1
  local filter=$2
  local msg=${3:-assertion failed}
  if [[ "$ASSERT" != "1" ]]; then
    return 0
  fi
  echo "$body" | jq -e "$filter" >/dev/null || die "${msg}: filter=${filter} body=$(echo "$body" | jq -c '.')"
}

api_code() {
  echo "$1" | jq -r '.code // -1'
}

# 等待订单离开 PENDING（撮合已接单）；深度依赖 marketdata 消费 ORDER_ACCEPTED。
wait_for_order_status() {
  local user_id=$1
  local order_id=$2
  local expect=$3
  local label=$4
  local i=0
  local body="" st=""
  while (( i < PIPELINE_WAIT_SEC )); do
    body="$(api GET "${BASE_URL}/v1/orders/${order_id}?user_id=${user_id}&symbol=${SYMBOL}")"
    if [[ "$(api_code "$body")" == "0" ]]; then
      st="$(echo "$body" | jq -r '.data.status // empty')"
      case "$expect" in
        ACCEPTED)
          if [[ "$st" == "ACCEPTED" || "$st" == "PARTIAL" || "$st" == "FILLED" ]]; then
            echo "$body"
            return 0
          fi
          ;;
        FILLED)
          if [[ "$st" == "FILLED" || "$st" == "PARTIAL" ]]; then
            echo "$body"
            return 0
          fi
          ;;
        *)
          if [[ "$st" == "$expect" ]]; then
            echo "$body"
            return 0
          fi
          ;;
      esac
    fi
    sleep "$PIPELINE_POLL_SEC"
    ((i++)) || true
  done
  echo "$body" | pretty >&2 || true
  die "${label}: order ${order_id} not ${expect} within ${PIPELINE_WAIT_SEC}s (last=${st:-unknown})"
}

wait_for_depth() {
  local label=$1
  local require_asks=${2:-false}
  local i=0
  local body=""
  while (( i < PIPELINE_WAIT_SEC )); do
    body="$(api GET "${BASE_URL}/v1/market/depth?symbol=${SYMBOL}&limit=10")"
    if [[ "$(api_code "$body")" == "0" ]]; then
      if [[ "$require_asks" != "true" ]] || echo "$body" | jq -e '.data.asks | length > 0' >/dev/null 2>&1; then
        echo "$body"
        return 0
      fi
    fi
    sleep "$PIPELINE_POLL_SEC"
    ((i++)) || true
  done
  echo "$body" | pretty >&2 || true
  die "${label}: market depth not ready within ${PIPELINE_WAIT_SEC}s (check matching/marketdata/kafka)"
}

wait_for_trades_min() {
  local user_id=$1
  local label=$2
  local min_count=${3:-1}
  local i=0
  local body=""
  while (( i < PIPELINE_WAIT_SEC )); do
    body="$(api GET "${BASE_URL}/v1/trades?user_id=${user_id}&symbol=${SYMBOL}&limit=20")"
    if [[ "$(api_code "$body")" == "0" ]]; then
      if echo "$body" | jq -e ".data.items | length >= ${min_count}" >/dev/null 2>&1; then
        echo "$body"
        return 0
      fi
    fi
    sleep "$PIPELINE_POLL_SEC"
    ((i++)) || true
  done
  echo "$body" | pretty >&2 || true
  die "${label}: trades not ready within ${PIPELINE_WAIT_SEC}s"
}

step_health() {
  log "=== 健康检查 / 时间 ==="
  local health time
  health="$(api GET "${BASE_URL}/v1/health")"
  echo "$health" | pretty
  check_code_zero "$health" "health"
  assert_jq "$health" '.data.status == "ok"' "health status"

  time="$(api GET "${BASE_URL}/v1/time")"
  echo "$time" | pretty
  check_code_zero "$time" "time"
  assert_jq "$time" '.data.unix_ms > 0' "server unix_ms"
}

step_deposit() {
  log "=== 用户充值（调账）==="
  local bid
  bid=$((RUN_ID % 1000000))
  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_BUYER},
    \"asset\": \"USDT\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 1)),
    \"change\": \"100000\"
  }" | tee /tmp/e2e_deposit_buyer_usdt.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_buyer_usdt.log)" "deposit buyer USDT"

  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_SELLER},
    \"asset\": \"USDT\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 2)),
    \"change\": \"100000\"
  }" | tee /tmp/e2e_deposit_seller_usdt.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_seller_usdt.log)" "deposit seller USDT"

  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_SELLER},
    \"asset\": \"BTC\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 3)),
    \"change\": \"10\"
  }" | tee /tmp/e2e_deposit_seller_btc.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_seller_btc.log)" "deposit seller BTC"

  api_json POST "${BASE_URL}/v1/balances" -d "{
    \"user_id\": ${USER_BUYER},
    \"asset\": \"BTC\",
    \"business\": \"deposit\",
    \"business_id\": $((bid + 4)),
    \"change\": \"1\"
  }" | tee /tmp/e2e_deposit_buyer_btc.log | pretty
  check_code_zero "$(cat /tmp/e2e_deposit_buyer_btc.log)" "deposit buyer BTC"
}

step_balances() {
  log "=== 资产查询 ==="
  local buyer_list seller_btc
  log "买方 user_id=${USER_BUYER}"
  buyer_list="$(api GET "${BASE_URL}/v1/balances?user_id=${USER_BUYER}")"
  echo "$buyer_list" | pretty
  check_code_zero "$buyer_list" "list balances buyer"
  assert_jq "$buyer_list" '.data.items | type == "array"' "buyer balances array"

  api GET "${BASE_URL}/v1/balances/USDT?user_id=${USER_BUYER}" | pretty

  log "卖方 user_id=${USER_SELLER}"
  api GET "${BASE_URL}/v1/balances?user_id=${USER_SELLER}" | pretty
  seller_btc="$(api GET "${BASE_URL}/v1/balances/BTC?user_id=${USER_SELLER}")"
  echo "$seller_btc" | pretty
  check_code_zero "$seller_btc" "seller BTC balance"
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
  check_code_zero "$(cat /tmp/e2e_sell_limit.log)" "sell limit"
  assert_jq "$(cat /tmp/e2e_sell_limit.log)" '.data.order_id != null and .data.order_id != ""' "sell order_id"
  local sell_order_id
  sell_order_id="$(jq -r '.data.order_id' /tmp/e2e_sell_limit.log)"

  log "等待卖单撮合入簿（轮询最多 ${PIPELINE_WAIT_SEC}s）..."
  wait_for_order_status "$USER_SELLER" "$sell_order_id" ACCEPTED "sell order accepted" >/dev/null

  log "深度（应有卖盘）"
  local depth
  depth="$(wait_for_depth "depth after sell" true)"
  echo "$depth" | pretty
  check_code_zero "$depth" "depth after sell"
  assert_jq "$depth" '.data.symbol == "'"${SYMBOL}"'"' "depth symbol"
  assert_jq "$depth" '(.data.asks | type) == "array"' "depth asks"

  log "买方挂限价买 ${LIMIT_QTY} @ ${LIMIT_PRICE}（预期与卖单撮合）"
  place_order "$USER_BUYER" "$buy_coid" "BUY" "LIMIT" "$LIMIT_PRICE" "$LIMIT_QTY" \
    | tee /tmp/e2e_buy_limit.log | pretty
  check_code_zero "$(cat /tmp/e2e_buy_limit.log)" "buy limit"
  assert_jq "$(cat /tmp/e2e_buy_limit.log)" '.data.order_id != null and .data.order_id != ""' "buy order_id"

  local buy_order_id
  buy_order_id="$(jq -r '.data.order_id' /tmp/e2e_buy_limit.log)"
  export LAST_ORDER_ID="$buy_order_id"

  log "等待买单成交（轮询最多 ${PIPELINE_WAIT_SEC}s）..."
  wait_for_order_status "$USER_BUYER" "$buy_order_id" FILLED "buy order filled" >/dev/null
}

step_market() {
  log "=== 市价单 ==="
  local sell_coid="e2e-${RUN_ID}-sell-for-market"
  log "卖方限价卖 ${LIMIT_QTY} @ ${LIMIT_PRICE}（供市价买吃单）"
  place_order "$USER_SELLER" "$sell_coid" "SELL" "LIMIT" "$LIMIT_PRICE" "$LIMIT_QTY" \
    | tee /tmp/e2e_sell_for_market.log | pretty
  check_code_zero "$(cat /tmp/e2e_sell_for_market.log)" "sell for market"
  local sell_for_market_id
  sell_for_market_id="$(jq -r '.data.order_id' /tmp/e2e_sell_for_market.log)"
  wait_for_order_status "$USER_SELLER" "$sell_for_market_id" ACCEPTED "sell for market accepted" >/dev/null
  wait_for_depth "depth for market" true >/dev/null

  local market_coid="e2e-${RUN_ID}-buy-market"
  log "买方市价买 ${MARKET_QTY}（IOC，无需 price）"
  place_order "$USER_BUYER" "$market_coid" "BUY" "MARKET" "" "$MARKET_QTY" \
    | tee /tmp/e2e_buy_market.log | pretty
  check_code_zero "$(cat /tmp/e2e_buy_market.log)" "buy market"
  local market_buy_id
  market_buy_id="$(jq -r '.data.order_id' /tmp/e2e_buy_market.log)"
  wait_for_order_status "$USER_BUYER" "$market_buy_id" FILLED "market buy filled" >/dev/null
}

assert_trades() {
  local user_id=$1
  local label=$2
  local body
  log "等待成交落库（${label}，轮询最多 ${PIPELINE_WAIT_SEC}s）..."
  body="$(wait_for_trades_min "$user_id" "trades ${label}" 1)"
  echo "$body" | pretty
  check_code_zero "$body" "trades ${label}"
  assert_jq "$body" '.data.items | length >= 1' "trades ${label}: expect >=1"
  assert_jq "$body" '.data.items[0].trade_id != ""' "trades ${label}: trade_id"
  assert_jq "$body" '.data.items[0].symbol == "'"${SYMBOL}"'"' "trades ${label}: symbol"
  if [[ -n "${LAST_ORDER_ID:-}" ]]; then
    local by_order
    by_order="$(api GET "${BASE_URL}/v1/trades?user_id=${user_id}&symbol=${SYMBOL}&order_id=${LAST_ORDER_ID}&limit=10")"
    echo "$by_order" | pretty
    check_code_zero "$by_order" "trades by order_id"
    assert_jq "$by_order" '.data.items | length >= 1' "trades filtered by order_id"
    assert_jq "$by_order" '[.data.items[] | .order_id == "'"${LAST_ORDER_ID}"'"] | all' "trades order_id match"
  fi
}

step_query() {
  log "=== 行情：深度 / Ticker / K 线 ==="
  local depth ticker klines
  depth="$(wait_for_depth "depth query" false)"
  echo "$depth" | pretty
  check_code_zero "$depth" "depth"
  assert_jq "$depth" '(.data.bids | type) == "array" and (.data.asks | type) == "array"' "depth sides"

  ticker="$(api GET "${BASE_URL}/v1/market/ticker?symbol=${SYMBOL}")"
  echo "$ticker" | pretty
  check_code_zero "$ticker" "ticker"
  assert_jq "$ticker" '.data.symbol == "'"${SYMBOL}"'"' "ticker symbol"

  log "=== 全市场 Ticker 快照（做市商 token）==="
  local tall
  tall="$(curl -sS -X GET "${BASE_URL}/v1/market/ticker/all?quote_asset=USDT" \
    -H "Authorization: Bearer ${MM_TOKEN}" \
    -H "Accept: application/json")"
  echo "$tall" | pretty
  if [[ "$ASSERT" == "1" ]]; then
    check_code_zero "$tall" "ticker/all"
    assert_jq "$tall" '.data.snapshot_id != "" and (.data.items | type) == "array"' "ticker/all snapshot"
  fi

  klines="$(api GET "${BASE_URL}/v1/klines?symbol=${SYMBOL}&interval=1m&limit=10")"
  echo "$klines" | pretty
  check_code_zero "$klines" "klines 1m"
  assert_jq "$klines" '.data.items | type == "array"' "klines array"

  api GET "${BASE_URL}/v1/klines?symbol=${SYMBOL}&interval=1s&limit=5" | pretty

  log "=== 订单查询 ==="
  local orders
  orders="$(api GET "${BASE_URL}/v1/orders?user_id=${USER_BUYER}&symbol=${SYMBOL}&limit=20")"
  echo "$orders" | pretty
  check_code_zero "$orders" "list orders"
  assert_jq "$orders" '.data.items | length >= 1' "orders list non-empty"

  if [[ -n "${LAST_ORDER_ID:-}" ]]; then
    log "单笔订单 order_id=${LAST_ORDER_ID}"
    local one
    one="$(api GET "${BASE_URL}/v1/orders/${LAST_ORDER_ID}?user_id=${USER_BUYER}&symbol=${SYMBOL}")"
    echo "$one" | pretty
    check_code_zero "$one" "get order"
    assert_jq "$one" '.data.order_id == "'"${LAST_ORDER_ID}"'"' "get order id"
  fi

  log "=== 成交列表 ==="
  assert_trades "$USER_BUYER" "buyer"

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
      log "=== E2E 完成（断言通过）==="
      ;;
    jwt-auth)
      init_auth_token
      log "JWT 已就绪（未跑业务步骤）"
      ;;
    *)
      die "unknown step: $1 (use: all|health|deposit|balances|orders|market|query|jwt-auth)"
      ;;
  esac
}

usage() {
  cat <<EOF
用法: $(basename "$0") [jwt] [step <name>|all]

模式:
  (默认)     static token（configs/gateway.json）
  jwt        先向 cmd/auth 换取 Bearer，再跑后续步骤（Gateway 需 jwt 配置）

步骤:
  health    健康检查、服务时间
  deposit   双用户充值（USDT/BTC）
  balances  资产列表与单资产查询
  orders    限价卖 + 限价买（撮合）
  market    限价卖 + 市价买
  query     深度、Ticker、K 线、订单/成交列表、余额
  jwt-auth  仅换取 JWT 并打印（调试用）

环境: BASE_URL=${BASE_URL}  E2E_AUTH=${E2E_AUTH_MODE}  SYMBOL=${SYMBOL}
      PIPELINE_WAIT_SEC=${PIPELINE_WAIT_SEC}  PIPELINE_POLL_SEC=${PIPELINE_POLL_SEC}
      jwt 时: AUTH_URL=${AUTH_URL}  AUTH_CLIENT_ID=${AUTH_CLIENT_ID}
      USER_BUYER=${USER_BUYER}  USER_SELLER=${USER_SELLER}
      全流程 all 需要 jq（或 SKIP_ASSERT=1 仅打印）

详见: scripts/e2e-api.md
EOF
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi
  log "E2E API  BASE_URL=${BASE_URL}  AUTH=${E2E_AUTH_MODE}  SYMBOL=${SYMBOL}  RUN_ID=${RUN_ID}  PIPELINE_WAIT=${PIPELINE_WAIT_SEC}s  ASSERT=${ASSERT}"
  if [[ "$STEP_FILTER" != "jwt-auth" ]]; then
    init_auth_token
  fi
  run_step "$STEP_FILTER"
}

main "$@"
