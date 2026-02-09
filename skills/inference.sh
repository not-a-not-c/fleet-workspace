#!/usr/bin/env bash
# inference.sh — Send a prompt to the fleet's Ollama inference server (on Jetson)
# and return the response. Publishes to fleet/inference/request, listens on
# fleet/inference/result for the matching reply.
#
# Usage:
#   ./inference.sh "What is the capital of France?"
#   ./inference.sh --model tinyllama "Explain gravity in one sentence"
#   ./inference.sh --timeout 60 "Write a haiku about robots"
#
# Environment:
#   MQTT_BROKER — broker hostname (auto-discovered via mDNS if unset)
#   INFERENCE_MODEL — default model (default: tinyllama)
#   INFERENCE_TIMEOUT — seconds to wait for response (default: 120)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE="$(dirname "$SCRIPT_DIR")"
AGENT_NAME="$(grep '# Agent:' "$WORKSPACE/AGENT_IDENTITY.md" 2>/dev/null | cut -d: -f2 | xargs || echo "macbook-prime")"

# ─── Defaults ─────────────────────────────────────────────────────────────────
MODEL="${INFERENCE_MODEL:-tinyllama}"
TIMEOUT="${INFERENCE_TIMEOUT:-120}"

# ─── Parse args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --model)   MODEL="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: inference.sh [--model MODEL] [--timeout SECS] \"prompt\""
      echo ""
      echo "Send a prompt to the fleet Ollama inference server and get a response."
      echo ""
      echo "Options:"
      echo "  --model MODEL    Model to use (default: tinyllama)"
      echo "  --timeout SECS   Max seconds to wait (default: 120)"
      echo ""
      echo "Examples:"
      echo "  inference.sh \"What is 2+2?\""
      echo "  inference.sh --model tinyllama \"Explain quantum computing briefly\""
      exit 0
      ;;
    *)         PROMPT="$1"; shift ;;
  esac
done

if [[ -z "${PROMPT:-}" ]]; then
  echo "ERROR: No prompt provided. Usage: inference.sh [--model MODEL] \"prompt\"" >&2
  exit 1
fi

# ─── Discover broker ─────────────────────────────────────────────────────────
if [[ -z "${MQTT_BROKER:-}" ]]; then
  if [[ -x "$WORKSPACE/resolve-broker.sh" ]]; then
    MQTT_BROKER="$("$WORKSPACE/resolve-broker.sh" 2>/dev/null || true)"
  fi
  if [[ -z "${MQTT_BROKER:-}" ]]; then
    # Fallback to local hostname
    MQTT_BROKER="Johns-MacBook-Pro-5937.local"
  fi
fi

# ─── Build request ───────────────────────────────────────────────────────────
REQUEST_ID="inf-$(date +%s)-$RANDOM"
TS=$(date +%s)

REQUEST_MSG=$(jq -nc \
  --arg id "$REQUEST_ID" \
  --arg from "$AGENT_NAME" \
  --arg model "$MODEL" \
  --arg prompt "$PROMPT" \
  --arg ts "$TS" \
  '{id: $id, from: $from, model: $model, prompt: $prompt, ts: ($ts | tonumber)}')

# ─── Subscribe FIRST, then publish (avoid race) ─────────────────────────────
# We use a temp file + background subscriber pattern
RESULT_FILE=$(mktemp /tmp/inference-result-XXXXXX)
SUB_PID_FILE=$(mktemp /tmp/inference-subpid-XXXXXX)

# Start subscriber in background — listens for our specific request ID
(
  mosquitto_sub -h "$MQTT_BROKER" -t "fleet/inference/result" -W "$TIMEOUT" -F '%p' 2>/dev/null \
  | while IFS= read -r line; do
    REF=$(echo "$line" | jq -r '.ref // empty' 2>/dev/null)
    if [[ "$REF" == "$REQUEST_ID" ]]; then
      echo "$line" > "$RESULT_FILE"
      # Signal success and exit
      exit 0
    fi
  done
) &
SUB_PID=$!
echo "$SUB_PID" > "$SUB_PID_FILE"

# Brief pause to let subscriber connect
sleep 0.3

# ─── Publish request ─────────────────────────────────────────────────────────
mosquitto_pub -h "$MQTT_BROKER" -t "fleet/inference/request" -m "$REQUEST_MSG"

echo "Request $REQUEST_ID sent (model=$MODEL, timeout=${TIMEOUT}s)" >&2
echo "Prompt: $PROMPT" >&2
echo "Waiting for response..." >&2

# ─── Wait for result ─────────────────────────────────────────────────────────
ELAPSED=0
while [[ $ELAPSED -lt $TIMEOUT ]]; do
  if [[ -s "$RESULT_FILE" ]]; then
    # Got a result!
    RESPONSE_TEXT=$(jq -r '.response // .body // "No response field"' "$RESULT_FILE" 2>/dev/null)
    LATENCY=$(jq -r '.latency_ms // "?"' "$RESULT_FILE" 2>/dev/null)
    RESP_MODEL=$(jq -r '.model // "unknown"' "$RESULT_FILE" 2>/dev/null)

    echo "" >&2
    echo "--- Inference Result (model=$RESP_MODEL, latency=${LATENCY}ms) ---" >&2
    echo "$RESPONSE_TEXT"

    # Cleanup
    kill "$SUB_PID" 2>/dev/null || true
    rm -f "$RESULT_FILE" "$SUB_PID_FILE"
    exit 0
  fi
  sleep 1
  ELAPSED=$((ELAPSED + 1))
done

# Timeout
echo "" >&2
echo "ERROR: Timed out after ${TIMEOUT}s waiting for inference result." >&2
echo "The inference server on Jetson may be down. Check:" >&2
echo "  - Is inference-server.py running on jetson.local?" >&2
echo "  - Is Ollama running? (curl http://jetson.local:11434/api/tags)" >&2
kill "$SUB_PID" 2>/dev/null || true
rm -f "$RESULT_FILE" "$SUB_PID_FILE"
exit 1
