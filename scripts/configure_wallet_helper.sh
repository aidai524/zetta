#!/usr/bin/env bash
set -euo pipefail

NODE_ID="${1:?usage: configure_wallet_helper.sh <node-id> <master-host> [worker-processes]}"
MASTER_HOST="${2:?usage: configure_wallet_helper.sh <node-id> <master-host> [worker-processes]}"
WORKER_PROCESSES="${3:-6}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZETTA_HOME="${ZETTA_HOME:-/opt/zetta}"
ENV_FILE="${ENV_FILE:-/etc/zetta/zetta.env}"
WALLET_RAW_DIR="${WALLET_RAW_DIR:-/var/lib/zetta/wallet-raw}"
WALLET_STATE_DIR="${WALLET_STATE_DIR:-/var/lib/zetta/wallet-state}"

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo \
    ZETTA_HOME="$ZETTA_HOME" \
    ENV_FILE="$ENV_FILE" \
    WALLET_RAW_DIR="$WALLET_RAW_DIR" \
    WALLET_STATE_DIR="$WALLET_STATE_DIR" \
    "$0" "$@"
fi

set_env() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
  fi
}

install -d -m 0755 "$(dirname "$ENV_FILE")" "$WALLET_RAW_DIR" "$WALLET_STATE_DIR"

if [[ -d "$ZETTA_HOME/src" ]]; then
  rsync -a --delete "$REPO_ROOT/src/" "$ZETTA_HOME/src/"
fi
install -m 0755 "$REPO_ROOT/infra/scripts/zetta-runner" /usr/local/bin/zetta-runner

set_env ZETTA_NODE_ID "$NODE_ID"
set_env ZETTA_POSTGRES_DSN "postgresql://zetta:zetta@${MASTER_HOST}:55432/zetta"
set_env ZETTA_CLICKHOUSE_HOST "$MASTER_HOST"
set_env ZETTA_CLICKHOUSE_PORT "8123"
set_env ZETTA_CLICKHOUSE_USER "zetta"
set_env ZETTA_CLICKHOUSE_PASSWORD "zetta"
set_env ZETTA_CLICKHOUSE_DATABASE "zetta"
set_env ZETTA_RAW_DIR "$WALLET_RAW_DIR"
set_env ZETTA_STATE_DIR "$WALLET_STATE_DIR"
set_env ZETTA_WORKER_PROCESSES "$WORKER_PROCESSES"
set_env ZETTA_WORKER_TASK_KINDS "wallet-portfolio,wallet-pnl"
set_env ZETTA_REALTIME_TRADE_BATCH_SIZE "5000"
set_env ZETTA_REALTIME_TRADE_MAX_PATHS "5000"

disable_units=(
  zetta-api.service
  zetta-frontier.timer
  zetta-frontier-trades.timer
  zetta-chain-frontier.timer
  zetta-chain-pnl.timer
  zetta-load.timer
  zetta-marts.timer
  zetta-wallet-refresh.timer
  zetta-wallet-candidates.timer
  zetta-wallet-pnl-candidates.timer
  zetta-wallet-pnl-load.timer
  zetta-wallet-rollup.timer
  zetta-wallet-screener.timer
  zetta-ws-market.service
)

systemctl disable --now "${disable_units[@]}" 2>/dev/null || true

if [[ "${STOP_LOCAL_DOCKER:-1}" == "1" ]] && command -v docker >/dev/null 2>&1; then
  if [[ -f "$ZETTA_HOME/docker-compose.yml" ]]; then
    (cd "$ZETTA_HOME" && docker compose stop postgres clickhouse redpanda minio) || true
  fi
fi

systemctl daemon-reload
systemctl enable --now zetta-worker.service
systemctl enable --now zetta-load-trades-realtime.timer
systemctl restart zetta-worker.service

echo "wallet helper configured"
echo "node_id=$NODE_ID"
echo "master=$MASTER_HOST"
echo "worker_processes=$WORKER_PROCESSES"
echo "raw_dir=$WALLET_RAW_DIR"
echo "state_dir=$WALLET_STATE_DIR"
