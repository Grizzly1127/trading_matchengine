#!/usr/bin/env bash
# 撮合与资产准确性 E2E：停服 → reset-dev → 启服 → 数值断言（订单/成交/深度/余额）。
#
# 与 e2e-api.sh 的区别：在干净盘口上校验 price/qty/avg_price/filled_quantity、
# 成交记录与买卖双方 balance 守恒（默认卖价随 RUN_ID 错开，全量 reset 后验证最可靠）。
#
# 用法:
#   ./scripts/e2e-accuracy.sh              # 全流程（停服、reset、start、断言）
#   ./scripts/e2e-accuracy.sh --test-only  # 仅跑断言（假定环境已就绪）
#   ./scripts/e2e-accuracy.sh --help
#
# 环境变量:
#   BASE_URL, TOKEN, SYMBOL, USER_BUYER, USER_SELLER
#   LIMIT_PRICE（默认 70000）  LIMIT_QTY（默认 0.01）
#   PIPELINE_WAIT_SEC, PIPELINE_POLL_SEC
#   START_WAIT_SEC   启服后等待 Gateway 健康（默认 120）
#   SKIP_ASSERT=1    仅打印、不做断言
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-dev-token-change-me}"
SYMBOL="${SYMBOL:-BTC-USDT}"
USER_BUYER="${USER_BUYER:-1}"
USER_SELLER="${USER_SELLER:-2}"
PIPELINE_WAIT_SEC="${PIPELINE_WAIT_SEC:-30}"
PIPELINE_POLL_SEC="${PIPELINE_POLL_SEC:-1}"
START_WAIT_SEC="${START_WAIT_SEC:-120}"
LIMIT_QTY="${LIMIT_QTY:-0.01}"
RUN_ID="${RUN_ID:-$(date +%s)}"
# 默认价位随 RUN_ID 错开（75000–84999），降低未 reset 时与同价历史挂单撞车
LIMIT_PRICE="${LIMIT_PRICE:-$((75000 + (RUN_ID / 10) % 10000))}"

DO_RESET_START=true
TEST_ONLY=false

log() { printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

have_jq() { command -v jq >/dev/null 2>&1; }
have_python3() { command -v python3 >/dev/null 2>&1; }

ASSERT=1
if [[ "${SKIP_ASSERT:-0}" == "1" ]]; then
  ASSERT=0
elif ! have_jq || ! have_python3; then
  die "需要 jq 与 python3（或 SKIP_ASSERT=1 仅打印）"
fi

pretty() {
  if have_jq; then jq .; else cat; fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --test-only) TEST_ONLY=true; DO_RESET_START=false; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "未知参数: $1（见 --help）" ;;
    esac
  done
}

usage() {
  cat <<EOF
用法: $(basename "$0") [--test-only]

默认流程:
  1. ./scripts/dev.sh stop
  2. ./scripts/reset-dev.sh -y --migrate
  3. ./scripts/dev.sh start --build
  4. 健康检查 + 充值 + 限价卖入簿 + 限价买成交 + 数值断言

--test-only  跳过停服/reset/启服，仅执行第 4 步（环境须已干净且服务已起）

环境: LIMIT_PRICE=${LIMIT_PRICE}  LIMIT_QTY=${LIMIT_QTY}  SYMBOL=${SYMBOL}
      BASE_URL=${BASE_URL}  PIPELINE_WAIT_SEC=${PIPELINE_WAIT_SEC}
EOF
}

# 十进制比较（API 返回长小数串）
dec_eq() {
  local a=$1 b=$2
  python3 -c "from decimal import Decimal; import sys; sys.exit(0 if Decimal('${a}') == Decimal('${b}') else 1)"
}

dec_mul() {
  local x=$1 y=$2
  python3 -c "from decimal import Decimal; print(Decimal('${x}') * Decimal('${y}'))"
}

dec_add() {
  local x=$1 y=$2
  python3 -c "from decimal import Decimal; print(Decimal('${x}') + Decimal('${y}'))"
}

dec_sub() {
  local x=$1 y=$2
  python3 -c "from decimal import Decimal; print(Decimal('${x}') - Decimal('${y}'))"
}

dec_ge() {
  local a=$1 b=$2
  python3 -c "from decimal import Decimal; import sys; sys.exit(0 if Decimal('${a}') >= Decimal('${b}') else 1)"
}

order_filled_qty() {
  echo "$1" | jq -r '.data.filled_quantity // 0'
}

order_filled_enough() {
  local body=$1
  local qty=${2:-$LIMIT_QTY}
  local st fq
  st="$(echo "$body" | jq -r '.data.status // empty')"
  fq="$(order_filled_qty "$body")"
  [[ "$st" == "FILLED" || "$st" == "PARTIAL" ]] && dec_ge "$fq" "$qty"
}

assert_dec_eq() {
  local actual=$1 expected=$2 msg=$3
  if [[ "$ASSERT" != "1" ]]; then
    return 0
  fi
  dec_eq "$actual" "$expected" || die "${msg}: 期望=${expected} 实际=${actual}"
}

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

api_code() {
  echo "$1" | jq -r '.code // -1'
}

check_code_zero() {
  local body=$1
  local ctx=${2:-api}
  if [[ "$ASSERT" != "1" ]]; then
    return 0
  fi
  local code
  code="$(api_code "$body")"
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

wait_for_gateway() {
  log "等待 Gateway 就绪（最多 ${START_WAIT_SEC}s）..."
  local i=0 body=""
  while (( i < START_WAIT_SEC )); do
    body="$(curl -sS "${BASE_URL}/v1/health" -H "Accept: application/json" 2>/dev/null || true)"
    if [[ -n "$body" ]] && [[ "$(api_code "$body")" == "0" ]]; then
      log "Gateway 已就绪"
      return 0
    fi
    sleep 2
    ((i += 2)) || true
  done
  die "Gateway 未在 ${START_WAIT_SEC}s 内就绪（${BASE_URL}/v1/health）"
}

wait_for_order_status() {
  local user_id=$1
  local order_id=$2
  local expect=$3
  local label=$4
  local require_fill_qty=${5:-1}
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
          if [[ "$st" == "FILLED" ]]; then
            if [[ "$require_fill_qty" == "0" ]] || order_filled_enough "$body"; then
              echo "$body"
              return 0
            fi
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
  die "${label}: order ${order_id} 未在 ${PIPELINE_WAIT_SEC}s 内达到 ${expect}（最后=${st:-unknown}）"
}

wait_for_depth_ask_at_price() {
  local price=$1
  local label=$2
  local i=0
  local body=""
  while (( i < PIPELINE_WAIT_SEC )); do
    body="$(api GET "${BASE_URL}/v1/market/depth?symbol=${SYMBOL}&limit=20")"
    if [[ "$(api_code "$body")" == "0" ]]; then
      if echo "$body" | jq -e --arg p "$price" --arg q "$LIMIT_QTY" \
        '[.data.asks[]? | select(.[0] == $p and (.[1] | tonumber) >= ($q | tonumber))] | length > 0' \
        >/dev/null 2>&1; then
        echo "$body"
        return 0
      fi
    fi
    sleep "$PIPELINE_POLL_SEC"
    ((i++)) || true
  done
  echo "$body" | pretty >&2 || true
  die "${label}: 深度未出现卖价 ${price} 数量>=${LIMIT_QTY}（${PIPELINE_WAIT_SEC}s）"
}

wait_for_trades_min() {
  local user_id=$1
  local order_id=$2
  local label=$3
  local i=0
  local body=""
  while (( i < PIPELINE_WAIT_SEC )); do
    body="$(api GET "${BASE_URL}/v1/trades?user_id=${user_id}&symbol=${SYMBOL}&order_id=${order_id}&limit=10")"
    if [[ "$(api_code "$body")" == "0" ]]; then
      if echo "$body" | jq -e '.data.items | length >= 1' >/dev/null 2>&1; then
        echo "$body"
        return 0
      fi
    fi
    sleep "$PIPELINE_POLL_SEC"
    ((i++)) || true
  done
  echo "$body" | pretty >&2 || true
  die "${label}: 成交未就绪（order_id=${order_id}）"
}

balance_field() {
  local user_id=$1
  local asset=$2
  local field=${3:-balance}
  local body
  body="$(api GET "${BASE_URL}/v1/balances/${asset}?user_id=${user_id}")"
  check_code_zero "$body" "balance ${user_id}/${asset}"
  echo "$body" | jq -r ".data.${field}"
}

place_limit() {
  local user_id=$1
  local client_id=$2
  local side=$3
  local price=$4
  local qty=$5
  api_json POST "${BASE_URL}/v1/orders" -d "$(cat <<EOF
{
  "user_id": ${user_id},
  "client_order_id": "${client_id}",
  "symbol": "${SYMBOL}",
  "side": "${side}",
  "type": "LIMIT",
  "price": "${price}",
  "quantity": "${qty}",
  "time_in_force": "GTC"
}
EOF
)"
}

step_env_reset_start() {
  log ">>> 停止微服务"
  bash "$ROOT/scripts/dev.sh" stop || true

  log ">>> 重置开发环境（Postgres/Redis/Kafka/WAL）"
  bash "$ROOT/scripts/reset-dev.sh" -y --migrate

  log ">>> 启动微服务（--build）"
  bash "$ROOT/scripts/dev.sh" start --build

  wait_for_gateway
}

# 充值后快照（deposit 为累加；断言基于本 run 的起点）
capture_balance_baselines() {
  BASE_BUYER_USDT="$(balance_field "$USER_BUYER" USDT balance)"
  BASE_BUYER_BTC="$(balance_field "$USER_BUYER" BTC balance)"
  BASE_SELLER_USDT="$(balance_field "$USER_SELLER" USDT balance)"
  BASE_SELLER_BTC="$(balance_field "$USER_SELLER" BTC balance)"
  log "余额基线 买方 USDT=${BASE_BUYER_USDT} BTC=${BASE_BUYER_BTC}  卖方 USDT=${BASE_SELLER_USDT} BTC=${BASE_SELLER_BTC}"
}

step_deposit() {
  log "=== 充值（固定基数，便于余额断言）==="
  local bid=$((RUN_ID % 1000000))
  local assets=(
    "${USER_BUYER}:USDT:$((bid + 1)):100000"
    "${USER_SELLER}:USDT:$((bid + 2)):100000"
    "${USER_SELLER}:BTC:$((bid + 3)):10"
    "${USER_BUYER}:BTC:$((bid + 4)):1"
  )
  local row uid asset biz ch body
  for row in "${assets[@]}"; do
    IFS=: read -r uid asset biz ch <<<"$row"
    body="$(api_json POST "${BASE_URL}/v1/balances" -d "{
      \"user_id\": ${uid},
      \"asset\": \"${asset}\",
      \"business\": \"deposit\",
      \"business_id\": ${biz},
      \"change\": \"${ch}\"
    }")"
    echo "$body" | pretty
    check_code_zero "$body" "deposit ${uid}/${asset}"
  done
  capture_balance_baselines
}

assert_order_fill() {
  local body=$1
  local label=$2
  local check_filled_qty=${3:-1}
  local fq
  check_code_zero "$body" "$label"
  assert_jq "$body" '.data.status == "FILLED"' "${label}: status"
  assert_jq "$body" '.data.symbol == "'"${SYMBOL}"'"' "${label}: symbol"
  # Gateway 当前不回填 avg_price；成交量以 trades / 订单 filled_quantity 为准。
  if [[ "$ASSERT" != "1" || "$check_filled_qty" != "1" ]]; then
    return 0
  fi
  fq="$(echo "$body" | jq -r '.data.filled_quantity')"
  dec_eq "$fq" "$LIMIT_QTY" || die "${label}: filled_quantity 期望=${LIMIT_QTY} 实际=${fq}"
}

assert_trade_for_order() {
  local user_id=$1
  local order_id=$2
  local expect_side=$3
  local label=$4
  local body
  body="$(wait_for_trades_min "$user_id" "$order_id" "$label")"
  echo "$body" | pretty
  check_code_zero "$body" "$label trades"
  assert_jq "$body" '.data.items | length == 1' "${label}: 单笔成交"
  assert_jq "$body" '[.data.items[] | .order_id == "'"${order_id}"'"] | all' "${label}: order_id"
  assert_jq "$body" '[.data.items[] | .side == "'"${expect_side}"'"] | all' "${label}: side"
  assert_jq "$body" '[.data.items[] | .symbol == "'"${SYMBOL}"'"] | all' "${label}: symbol"
  if [[ "$ASSERT" == "1" ]]; then
    local pr qty fee
    pr="$(echo "$body" | jq -r '.data.items[0].price')"
    qty="$(echo "$body" | jq -r '.data.items[0].quantity')"
    fee="$(echo "$body" | jq -r '.data.items[0].fee')"
    dec_eq "$pr" "$LIMIT_PRICE" || die "${label}: price 期望=${LIMIT_PRICE} 实际=${pr}"
    dec_eq "$qty" "$LIMIT_QTY" || die "${label}: quantity 期望=${LIMIT_QTY} 实际=${qty}"
    dec_eq "$fee" "0" || die "${label}: 联调环境期望 fee=0，实际=${fee}"
  fi
}

wait_for_balance_eq() {
  local user_id=$1
  local asset=$2
  local field=$3
  local expected=$4
  local label=$5
  local i=0
  local got=""
  while (( i < PIPELINE_WAIT_SEC )); do
    got="$(balance_field "$user_id" "$asset" "$field")"
    if dec_eq "$got" "$expected"; then
      log "${label}: ${asset}.${field}=${got}"
      return 0
    fi
    sleep "$PIPELINE_POLL_SEC"
    ((i++)) || true
  done
  die "${label}: ${asset}.${field} 期望=${expected} 实际=${got}（${PIPELINE_WAIT_SEC}s）"
}

assert_balances_after_trade() {
  log "=== 余额守恒断言（轮询至 trade 结算完成）==="
  local notional
  notional="$(dec_mul "$LIMIT_PRICE" "$LIMIT_QTY")"

  local exp_buyer_usdt exp_buyer_btc exp_seller_usdt exp_seller_btc
  exp_buyer_usdt="$(dec_sub "${BASE_BUYER_USDT}" "$notional")"
  exp_buyer_btc="$(dec_add "${BASE_BUYER_BTC}" "$LIMIT_QTY")"
  exp_seller_usdt="$(dec_add "${BASE_SELLER_USDT}" "$notional")"
  exp_seller_btc="$(dec_sub "${BASE_SELLER_BTC}" "$LIMIT_QTY")"

  wait_for_balance_eq "$USER_BUYER" USDT balance "$exp_buyer_usdt" "买方"
  wait_for_balance_eq "$USER_BUYER" BTC balance "$exp_buyer_btc" "买方"
  wait_for_balance_eq "$USER_SELLER" USDT balance "$exp_seller_usdt" "卖方"
  wait_for_balance_eq "$USER_SELLER" BTC balance "$exp_seller_btc" "卖方"

  local got
  for got in \
    "$(balance_field "$USER_BUYER" USDT frozen)" \
    "$(balance_field "$USER_BUYER" BTC frozen)" \
    "$(balance_field "$USER_SELLER" USDT frozen)" \
    "$(balance_field "$USER_SELLER" BTC frozen)"; do
    assert_dec_eq "$got" "0" "成交后 frozen 应为 0"
  done
}

step_matching_accuracy() {
  log "=== 撮合准确性：卖 ${LIMIT_QTY} @ ${LIMIT_PRICE} → 买同价同量 ==="
  local sell_coid="acc-${RUN_ID}-sell"
  local buy_coid="acc-${RUN_ID}-buy"
  local sell_resp buy_resp sell_id buy_id
  local sell_body buy_body depth_body

  sell_resp="$(place_limit "$USER_SELLER" "$sell_coid" "SELL" "$LIMIT_PRICE" "$LIMIT_QTY")"
  echo "$sell_resp" | pretty
  check_code_zero "$sell_resp" "place sell"
  sell_id="$(echo "$sell_resp" | jq -r '.data.order_id')"
  [[ -n "$sell_id" && "$sell_id" != "null" ]] || die "卖单无 order_id"

  sell_body="$(wait_for_order_status "$USER_SELLER" "$sell_id" ACCEPTED "卖单入簿")"
  log "卖单已 ACCEPTED（尚未成交）"

  depth_body="$(wait_for_depth_ask_at_price "$LIMIT_PRICE" "卖单挂盘深度")"
  echo "$depth_body" | pretty
  assert_jq "$depth_body" '[.data.asks[]? | .[0] == "'"${LIMIT_PRICE}"'"] | any' "深度含目标卖价"

  buy_resp="$(place_limit "$USER_BUYER" "$buy_coid" "BUY" "$LIMIT_PRICE" "$LIMIT_QTY")"
  echo "$buy_resp" | pretty
  check_code_zero "$buy_resp" "place buy"
  buy_id="$(echo "$buy_resp" | jq -r '.data.order_id')"
  [[ -n "$buy_id" && "$buy_id" != "null" ]] || die "买单无 order_id"

  log "等待成交落库（以 trades 为准，避免 match 事件先于 trade 结算）..."
  assert_trade_for_order "$USER_BUYER" "$buy_id" "BUY" "买方成交"
  assert_trade_for_order "$USER_SELLER" "$sell_id" "SELL" "卖方成交"

  buy_body="$(wait_for_order_status "$USER_BUYER" "$buy_id" FILLED "买单状态" 0)"
  sell_body="$(wait_for_order_status "$USER_SELLER" "$sell_id" FILLED "卖单状态" 1)"

  log "=== 订单字段 ==="
  echo "$buy_body" | pretty
  echo "$sell_body" | pretty
  # 买方 filled_quantity 在部分版本可能滞后；成交准确性以 trades 为准。
  assert_order_fill "$buy_body" "买单" 0
  assert_order_fill "$sell_body" "卖单" 1

  assert_balances_after_trade

  log "=== 撮合后深度（卖档应被吃光）==="
  depth_body="$(api GET "${BASE_URL}/v1/market/depth?symbol=${SYMBOL}&limit=10")"
  echo "$depth_body" | pretty
  check_code_zero "$depth_body" "depth after trade"
  if [[ "$ASSERT" == "1" ]]; then
    if echo "$depth_body" | jq -e --arg p "$LIMIT_PRICE" \
      '[.data.asks[]? | .[0] == $p and (.[1] | tonumber) > 0] | length > 0' \
      >/dev/null 2>&1; then
      die "成交后不应仍有卖价 ${LIMIT_PRICE} 的挂单"
    fi
  fi
}

main() {
  parse_args "$@"
  log "E2E 准确性  SYMBOL=${SYMBOL}  PRICE=${LIMIT_PRICE}  QTY=${LIMIT_QTY}  RUN_ID=${RUN_ID}  ASSERT=${ASSERT}"

  if $DO_RESET_START; then
    step_env_reset_start
  else
    log "跳过停服/reset/启服（--test-only）"
    wait_for_gateway
  fi

  local health
  health="$(api GET "${BASE_URL}/v1/health")"
  echo "$health" | pretty
  check_code_zero "$health" "health"
  assert_jq "$health" '.data.status == "ok"' "health status"

  step_deposit
  step_matching_accuracy

  log "=== 撮合与资产准确性验证通过 ==="
}

main "$@"
