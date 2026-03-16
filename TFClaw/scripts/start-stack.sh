#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="start"
if [[ "${1:-}" == "--dev" ]]; then
  MODE="dev"
fi

TFCLAW_CONFIG_PATH="${TFCLAW_CONFIG_PATH:-$ROOT_DIR/config.json}"
TFCLAW_TOKEN="${TFCLAW_TOKEN:-}"
TFCLAW_RELAY_URL="${TFCLAW_RELAY_URL:-}"

if command -v jq >/dev/null 2>&1 && [[ -f "$TFCLAW_CONFIG_PATH" ]]; then
  if [[ -z "$TFCLAW_TOKEN" ]]; then
    TFCLAW_TOKEN="$(jq -r '.relay.token // empty' "$TFCLAW_CONFIG_PATH")"
  fi
  if [[ -z "$TFCLAW_RELAY_URL" ]]; then
    TFCLAW_RELAY_URL="$(jq -r '.relay.url // empty' "$TFCLAW_CONFIG_PATH")"
  fi
fi

if [[ -z "$TFCLAW_TOKEN" ]]; then
  echo "[start-stack] missing relay token: set relay.token in $TFCLAW_CONFIG_PATH or TFCLAW_TOKEN in env" >&2
  exit 1
fi

if [[ -z "$TFCLAW_RELAY_URL" ]]; then
  echo "[start-stack] missing relay url: set relay.url in $TFCLAW_CONFIG_PATH or TFCLAW_RELAY_URL in env" >&2
  exit 1
fi

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
}

trap cleanup EXIT INT TERM

start_process() {
  local name="$1"
  shift
  echo "[start-stack] starting ${name} ..."
  "$@" &
  local pid=$!
  PIDS+=("$pid")
  echo "[start-stack] ${name} pid=${pid}"
}

if [[ "$MODE" == "start" ]]; then
  echo "[start-stack] build mode"
  npm run build --workspace @tfclaw/protocol
  npm run build --workspace @tfclaw/server
  npm run build --workspace @tfclaw/terminal-agent
  npm run build --workspace @tfclaw/feishu-gateway

  start_process "server" node apps/server/dist/index.js
  start_process "terminal-agent" env TFCLAW_TOKEN="$TFCLAW_TOKEN" TFCLAW_RELAY_URL="$TFCLAW_RELAY_URL" node apps/terminal-agent/dist/index.js
  start_process "gateway" env TFCLAW_CONFIG_PATH="$TFCLAW_CONFIG_PATH" node apps/feishu-gateway/dist/index.js
else
  echo "[start-stack] dev mode"
  echo "[start-stack] building protocol package for dev ..."
  npm run build --workspace @tfclaw/protocol

  start_process "server(dev)" npm run dev --workspace @tfclaw/server
  start_process "terminal-agent(dev)" env TFCLAW_TOKEN="$TFCLAW_TOKEN" TFCLAW_RELAY_URL="$TFCLAW_RELAY_URL" npm run dev --workspace @tfclaw/terminal-agent
  start_process "gateway(dev)" env TFCLAW_CONFIG_PATH="$TFCLAW_CONFIG_PATH" npm run dev --workspace @tfclaw/feishu-gateway
fi

echo "[start-stack] relay=${TFCLAW_RELAY_URL}"
echo "[start-stack] token=${TFCLAW_TOKEN}"
echo "[start-stack] config=${TFCLAW_CONFIG_PATH}"
echo "[start-stack] all processes started. press Ctrl+C to stop."

wait -n "${PIDS[@]}"
