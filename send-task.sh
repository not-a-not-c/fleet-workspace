#!/usr/bin/env bash
# send-task.sh — Send a task to an agent and wait for the result
# Usage: ./send-task.sh <target-agent> "task description"
#        ./send-task.sh broadcast "task for everyone"
#        ./send-task.sh --no-wait <target> "fire and forget"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_NAME="$(grep '# Agent:' "$SCRIPT_DIR/AGENT_IDENTITY.md" | cut -d: -f2 | xargs)"

# Parse args
WAIT=true
if [[ "${1:-}" == "--no-wait" ]]; then
  WAIT=false
  shift
fi

TARGET="${1:?Usage: ./send-task.sh [--no-wait] <target-agent|broadcast> \"task description\"}"
TASK_BODY="${2:?Usage: ./send-task.sh [--no-wait] <target-agent|broadcast> \"task description\"}"

# Discover broker
BROKER="$("$SCRIPT_DIR/resolve-broker.sh" 2>/dev/null)"
if [[ -z "$BROKER" ]]; then
  echo "ERROR: Cannot find MQTT broker via mDNS" >&2
  exit 1
fi

# Build task message
TASK_ID="task-$(date +%s)-$RANDOM"
TASK_MSG=$(jq -nc \
  --arg id "$TASK_ID" \
  --arg from "$AGENT_NAME" \
  --arg to "$TARGET" \
  --arg ts "$(date +%s)" \
  --arg body "$TASK_BODY" \
  '{id: $id, from: $from, to: $to, type: "task", ts: ($ts | tonumber), body: $body}')

echo "Task:   $TASK_ID"
echo "To:     $TARGET"
echo "Broker: $BROKER"
echo "Body:   $TASK_BODY"
echo ""

# Publish the task
mosquitto_pub -h "$BROKER" -t "fleet/cmd/$TARGET" -m "$TASK_MSG"
echo "Dispatched."

if [[ "$WAIT" == false ]]; then
  exit 0
fi

# Wait for result — subscribe to the target's result topic and filter by ref
echo "Waiting for result (Ctrl-C to stop)..."
echo ""

if [[ "$TARGET" == "broadcast" ]]; then
  RESULT_TOPIC="fleet/result/#"
else
  RESULT_TOPIC="fleet/result/$TARGET"
fi

mosquitto_sub -h "$BROKER" -t "$RESULT_TOPIC" -F '%p' | while IFS= read -r payload; do
  REF="$(echo "$payload" | jq -r '.ref // empty' 2>/dev/null)"
  if [[ "$REF" == "$TASK_ID" ]]; then
    RESULT_FROM="$(echo "$payload" | jq -r '.from' 2>/dev/null)"
    RESULT_BODY="$(echo "$payload" | jq -r '.body' 2>/dev/null)"
    echo "=== Result from $RESULT_FROM ==="
    echo "$RESULT_BODY"
    echo ""
    # For broadcast, keep listening for more results. For targeted, exit.
    if [[ "$TARGET" != "broadcast" ]]; then
      # Kill the parent mosquitto_sub
      kill $$ 2>/dev/null || true
      exit 0
    fi
  fi
done
