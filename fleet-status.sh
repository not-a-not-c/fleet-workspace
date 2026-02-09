#!/usr/bin/env bash
# fleet-status.sh — Show current fleet status at a glance
# Reads retained MQTT messages for status and heartbeats.
# Usage: ./fleet-status.sh [--watch]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BROKER="$("$SCRIPT_DIR/resolve-broker.sh" 2>/dev/null || echo 'localhost')"
NOW=$(date +%s)
WATCH=false
[[ "${1:-}" == "--watch" ]] && WATCH=true

show_status() {
  local NOW=$(date +%s)
  local TMPDIR=$(mktemp -d)
  trap "rm -rf $TMPDIR" RETURN

  # Collect retained status messages (wait 2s for all to arrive)
  timeout 2 mosquitto_sub -h "$BROKER" -t 'fleet/status/#' -v -F '%t %p' \
    > "$TMPDIR/statuses.txt" 2>/dev/null || true

  # Collect retained heartbeats
  timeout 2 mosquitto_sub -h "$BROKER" -t 'fleet/heartbeat/#' -v -F '%t %p' \
    > "$TMPDIR/heartbeats.txt" 2>/dev/null || true

  # Also discover agents via mDNS for completeness
  local DISCOVERED=""
  DISCOVERED=$("$SCRIPT_DIR/discover-peers.sh" 2>/dev/null | grep -v '===' | awk '{print $1}' || true)

  # Build a list of all known agents
  local AGENTS=""
  AGENTS=$(cat "$TMPDIR/statuses.txt" "$TMPDIR/heartbeats.txt" 2>/dev/null \
    | awk '{print $1}' | sed 's|fleet/status/||;s|fleet/heartbeat/||' | sort -u)

  # Add mDNS-discovered agents that might not have status yet
  for a in $DISCOVERED; do
    if ! echo "$AGENTS" | grep -q "^${a}$"; then
      AGENTS="$AGENTS"$'\n'"$a"
    fi
  done
  AGENTS=$(echo "$AGENTS" | sort -u | grep -v '^$')

  if [[ -z "$AGENTS" ]]; then
    echo "No agents found."
    return
  fi

  # Header
  printf "\n%-24s %-10s %6s %6s %6s %10s  %s\n" \
    "AGENT" "STATUS" "CPU%" "MEM%" "DISK%" "LAST SEEN" "TASK"
  printf "%-24s %-10s %6s %6s %6s %10s  %s\n" \
    "------------------------" "----------" "------" "------" "------" "----------" "----"

  # Per agent
  while IFS= read -r agent; do
    [[ -z "$agent" ]] && continue

    # Parse status
    local STATUS_LINE=$(grep "fleet/status/$agent " "$TMPDIR/statuses.txt" 2>/dev/null | tail -1 | cut -d' ' -f2-)
    local STATUS=$(echo "$STATUS_LINE" | jq -r '.body // "unknown"' 2>/dev/null || echo "unknown")
    local STATUS_TS=$(echo "$STATUS_LINE" | jq -r '.ts // 0' 2>/dev/null || echo 0)
    local TASK_REF=$(echo "$STATUS_LINE" | jq -r '.ref // ""' 2>/dev/null || echo "")

    # Parse heartbeat
    local HB_LINE=$(grep "fleet/heartbeat/$agent " "$TMPDIR/heartbeats.txt" 2>/dev/null | tail -1 | cut -d' ' -f2-)
    local CPU=$(echo "$HB_LINE" | jq -r '.body.cpu_pct // "--"' 2>/dev/null || echo "--")
    local MEM_TOTAL=$(echo "$HB_LINE" | jq -r '.body.mem_total_mb // 0' 2>/dev/null || echo 0)
    local MEM_AVAIL=$(echo "$HB_LINE" | jq -r '.body.mem_avail_mb // 0' 2>/dev/null || echo 0)
    local DISK=$(echo "$HB_LINE" | jq -r '.body.disk_pct // "--"' 2>/dev/null || echo "--")
    local HB_TS=$(echo "$HB_LINE" | jq -r '.ts // 0' 2>/dev/null || echo 0)
    local TASKS_DONE=$(echo "$HB_LINE" | jq -r '.body.tasks_completed // 0' 2>/dev/null || echo 0)

    # Calculate memory percentage
    local MEM_PCT="--"
    if [[ "$MEM_TOTAL" -gt 0 ]] 2>/dev/null; then
      MEM_PCT=$(awk "BEGIN {printf \"%.0f\", (1 - $MEM_AVAIL/$MEM_TOTAL) * 100}")
    fi

    # Calculate last seen (use most recent of status or heartbeat)
    local LAST_TS=$STATUS_TS
    [[ "$HB_TS" -gt "$LAST_TS" ]] 2>/dev/null && LAST_TS=$HB_TS
    local LAST_SEEN="never"
    if [[ "$LAST_TS" -gt 0 ]] 2>/dev/null; then
      local AGO=$((NOW - LAST_TS))
      if [[ $AGO -lt 60 ]]; then
        LAST_SEEN="${AGO}s ago"
      elif [[ $AGO -lt 3600 ]]; then
        LAST_SEEN="$((AGO / 60))m ago"
      else
        LAST_SEEN="$((AGO / 3600))h ago"
      fi
    fi

    # Status indicator
    if [[ "$STATUS" == "online" ]]; then
      local TASK_DISPLAY="idle ($TASKS_DONE done)"
    elif [[ "$STATUS" == "busy" ]]; then
      local TASK_DISPLAY="$TASK_REF"
    else
      local TASK_DISPLAY="--"
    fi

    printf "%-24s %-10s %6s %5s%% %5s%% %10s  %s\n" \
      "$agent" "$STATUS" "$CPU" "$MEM_PCT" "$DISK" "$LAST_SEEN" "$TASK_DISPLAY"

  done <<< "$AGENTS"

  echo ""
}

if [[ "$WATCH" == true ]]; then
  while true; do
    clear
    echo "Fleet Status — $(date) — Broker: $BROKER"
    show_status
    sleep 10
  done
else
  echo "Fleet Status — $(date) — Broker: $BROKER"
  show_status
fi
