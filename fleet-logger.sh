#!/usr/bin/env bash
# fleet-logger.sh — Central event logger for the agent fleet
# Subscribes to fleet/# and writes all events to fleet.jsonl
# Tracks task latency by correlating results with their originating tasks.
# Usage: ./fleet-logger.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/fleet.jsonl"
BROKER="$("$SCRIPT_DIR/resolve-broker.sh" 2>/dev/null || echo 'localhost')"

echo "[$(date)] Fleet logger starting"
echo "[$(date)] Broker: $BROKER"
echo "[$(date)] Log file: $LOG_FILE"
echo ""

# Track task dispatch timestamps for latency calculation
# We use a temp dir with one file per task ID
TASK_TRACKER=$(mktemp -d)
trap "rm -rf $TASK_TRACKER" EXIT

# Use %j format: each message is a single-line JSON with escaped payload
# This handles multi-line payloads correctly (no split reads)
mosquitto_sub -h "$BROKER" -t 'fleet/#' -F '%j' | while IFS= read -r line; do
  NOW=$(date +%s)

  # Extract topic and payload from the %j JSON wrapper
  TOPIC="$(echo "$line" | jq -r '.topic // empty' 2>/dev/null)"
  PAYLOAD="$(echo "$line" | jq -r '.payload // empty' 2>/dev/null)"

  # Skip empty payloads (cleared retained messages)
  [[ -z "$PAYLOAD" || "$PAYLOAD" == "null" ]] && continue

  # Parse message fields from the inner payload
  MSG_TYPE="$(echo "$PAYLOAD" | jq -r '.type // "unknown"' 2>/dev/null || echo 'unparseable')"
  MSG_FROM="$(echo "$PAYLOAD" | jq -r '.from // "unknown"' 2>/dev/null || echo 'unknown')"
  MSG_TS="$(echo "$PAYLOAD" | jq -r '.ts // 0' 2>/dev/null || echo '0')"

  # Build log entry: add topic and received_at to the original payload
  LOG_ENTRY=$(echo "$PAYLOAD" | jq -c \
    --arg topic "$TOPIC" \
    --argjson received_at "$NOW" \
    '. + {_topic: $topic, _received_at: $received_at}' 2>/dev/null || \
    echo "{\"_topic\":\"$TOPIC\",\"_received_at\":$NOW,\"_raw\":$(echo "$PAYLOAD" | jq -Rs .)}")

  echo "$LOG_ENTRY" >> "$LOG_FILE"

  # ── Task latency tracking ──
  case "$MSG_TYPE" in
    task)
      # Record dispatch timestamp
      TASK_ID="$(echo "$PAYLOAD" | jq -r '.id // empty' 2>/dev/null)"
      if [[ -n "$TASK_ID" ]]; then
        echo "$MSG_TS" > "$TASK_TRACKER/$TASK_ID"
      fi
      printf "[%s] %-12s %-22s -> %-22s %s\n" \
        "$(date +%H:%M:%S)" "TASK" "$MSG_FROM" \
        "$(echo "$TOPIC" | sed 's|fleet/cmd/||')" \
        "$(echo "$PAYLOAD" | jq -r '.body // ""' 2>/dev/null | tr '\n' ' ' | head -c 300)"
      ;;
    result)
      REF="$(echo "$PAYLOAD" | jq -r '.ref // empty' 2>/dev/null)"
      LATENCY="--"
      if [[ -n "$REF" && -f "$TASK_TRACKER/$REF" ]]; then
        DISPATCH_TS=$(cat "$TASK_TRACKER/$REF")
        RESULT_TS="${MSG_TS:-$NOW}"
        LATENCY="$((RESULT_TS - DISPATCH_TS))s"
        rm -f "$TASK_TRACKER/$REF"
        # Append latency to log
        echo "{\"_type\":\"latency\",\"task_id\":\"$REF\",\"from\":\"$MSG_FROM\",\"latency_s\":$((RESULT_TS - DISPATCH_TS)),\"_received_at\":$NOW}" >> "$LOG_FILE"
      fi
      BODY_PREVIEW="$(echo "$PAYLOAD" | jq -r '.body // ""' 2>/dev/null | tr '\n' ' ' | head -c 300)"
      printf "[%s] %-12s %-22s    latency: %-8s %s\n" \
        "$(date +%H:%M:%S)" "RESULT" "$MSG_FROM" "$LATENCY" "$BODY_PREVIEW"
      ;;
    status)
      BODY="$(echo "$PAYLOAD" | jq -r '.body // ""' 2>/dev/null)"
      printf "[%s] %-12s %-22s %s\n" \
        "$(date +%H:%M:%S)" "STATUS" "$MSG_FROM" "$BODY"
      ;;
    heartbeat)
      CPU="$(echo "$PAYLOAD" | jq -r '.body.cpu_pct // "--"' 2>/dev/null)"
      MEM="$(echo "$PAYLOAD" | jq -r '.body.mem_avail_mb // "--"' 2>/dev/null)"
      printf "[%s] %-12s %-22s cpu: %s%%  mem_avail: %sMB\n" \
        "$(date +%H:%M:%S)" "HEARTBEAT" "$MSG_FROM" "$CPU" "$MEM"
      ;;
    log)
      BODY="$(echo "$PAYLOAD" | jq -r '.body // ""' 2>/dev/null)"
      printf "[%s] %-12s %-22s %s\n" \
        "$(date +%H:%M:%S)" "LOG" "$MSG_FROM" "$BODY"
      ;;
    *)
      printf "[%s] %-12s %-22s %s\n" \
        "$(date +%H:%M:%S)" "$MSG_TYPE" "$MSG_FROM" "$(echo "$PAYLOAD" | tr '\n' ' ' | head -c 300)"
      ;;
  esac

done
