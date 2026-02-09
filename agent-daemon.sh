#!/usr/bin/env bash
# agent-daemon.sh — Long-running agent that listens for tasks over MQTT
# Discovers broker via mDNS. Uses fleet/ topic hierarchy.
# Publishes heartbeats every 30s with system vitals.
# Usage: ./agent-daemon.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_NAME="$(grep '# Agent:' "$SCRIPT_DIR/AGENT_IDENTITY.md" | cut -d: -f2 | xargs)"
TASK_DIR="$SCRIPT_DIR/tasks"
LOG_DIR="$SCRIPT_DIR/logs"
OS="$(uname -s)"

# File-based counters (subshells can't write to parent vars)
COUNTER_DIR="$SCRIPT_DIR/.counters"
mkdir -p "$COUNTER_DIR"
echo 0 > "$COUNTER_DIR/completed"
echo 0 > "$COUNTER_DIR/active"
DAEMON_START=$(date +%s)

echo "[$(date)] Agent daemon starting: $AGENT_NAME"

# ─── Discover broker via mDNS (retry until found) ────────────────────────────
BROKER=""
while [[ -z "$BROKER" ]]; do
  BROKER="$("$SCRIPT_DIR/resolve-broker.sh" 2>/dev/null || true)"
  if [[ -z "$BROKER" ]]; then
    echo "[$(date)] Waiting for MQTT broker on mDNS..."
    sleep 5
  fi
done
echo "[$(date)] Broker: $BROKER"

# ─── Publish online status (retained) ────────────────────────────────────────
mosquitto_pub -h "$BROKER" -t "fleet/status/$AGENT_NAME" -r \
  -m "{\"from\":\"$AGENT_NAME\",\"type\":\"status\",\"ts\":$(date +%s),\"body\":\"online\"}"
echo "[$(date)] Published status: online (retained)"

# ─── Heartbeat publisher (background) ────────────────────────────────────────
collect_vitals() {
  local cpu_pct mem_total mem_avail disk_pct

  if [[ "$OS" == "Darwin" ]]; then
    cpu_pct=$(ps -A -o %cpu | awk '{s+=$1} END {printf "%.1f", s/4}')
    mem_total=$(sysctl -n hw.memsize | awk '{printf "%.0f", $1/1048576}')
    mem_avail=$(vm_stat 2>/dev/null | awk '/Pages free/{free=$3} /Pages inactive/{inactive=$3} END {printf "%.0f", (free+inactive)*4096/1048576}')
    disk_pct=$(df -h / | awk 'NR==2 {gsub(/%/,""); print $5}')
  else
    cpu_pct=$(awk '{u=$2+$4; t=$2+$4+$5; if(NR==1){ou=u;ot=t} else {printf "%.1f", (u-ou)/(t-ot)*100}}' \
      <(grep 'cpu ' /proc/stat; sleep 1; grep 'cpu ' /proc/stat) 2>/dev/null || echo "0")
    mem_total=$(awk '/MemTotal/{printf "%.0f", $2/1024}' /proc/meminfo)
    mem_avail=$(awk '/MemAvailable/{printf "%.0f", $2/1024}' /proc/meminfo)
    disk_pct=$(df / | awk 'NR==2 {gsub(/%/,""); print $5}')
  fi

  local uptime_s=$(( $(date +%s) - DAEMON_START ))

  jq -nc \
    --arg from "$AGENT_NAME" \
    --arg ts "$(date +%s)" \
    --argjson cpu "${cpu_pct:-0}" \
    --argjson mem_total "${mem_total:-0}" \
    --argjson mem_avail "${mem_avail:-0}" \
    --argjson disk "${disk_pct:-0}" \
    --argjson uptime "$uptime_s" \
    --argjson done "$(cat "$COUNTER_DIR/completed" 2>/dev/null || echo 0)" \
    --argjson active "$(cat "$COUNTER_DIR/active" 2>/dev/null || echo 0)" \
    '{
      from: $from,
      type: "heartbeat",
      ts: ($ts | tonumber),
      body: {
        uptime_s: $uptime,
        cpu_pct: $cpu,
        mem_total_mb: $mem_total,
        mem_avail_mb: $mem_avail,
        disk_pct: $disk,
        tasks_completed: $done,
        tasks_active: $active
      }
    }'
}

heartbeat_loop() {
  while true; do
    sleep 30
    HB=$(collect_vitals 2>/dev/null || echo '{"from":"'"$AGENT_NAME"'","type":"heartbeat","ts":'"$(date +%s)"',"body":{"error":"vitals collection failed"}}')
    mosquitto_pub -h "$BROKER" -t "fleet/heartbeat/$AGENT_NAME" -r -m "$HB" 2>/dev/null || true
  done
}

# Publish first heartbeat immediately, then start loop
HB=$(collect_vitals 2>/dev/null || echo '{}')
mosquitto_pub -h "$BROKER" -t "fleet/heartbeat/$AGENT_NAME" -r -m "$HB" 2>/dev/null || true
heartbeat_loop &
HEARTBEAT_PID=$!
echo "[$(date)] Heartbeat started (pid $HEARTBEAT_PID, every 30s)"

# ─── Set offline status on exit ───────────────────────────────────────────────
cleanup() {
  echo "[$(date)] Shutting down..."
  kill $HEARTBEAT_PID 2>/dev/null || true
  mosquitto_pub -h "$BROKER" -t "fleet/status/$AGENT_NAME" -r \
    -m "{\"from\":\"$AGENT_NAME\",\"type\":\"status\",\"ts\":$(date +%s),\"body\":\"offline\"}" 2>/dev/null || true
  mosquitto_pub -h "$BROKER" -t "fleet/heartbeat/$AGENT_NAME" -r -n 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ─── Listen for tasks ────────────────────────────────────────────────────────
echo "[$(date)] Listening: fleet/cmd/$AGENT_NAME, fleet/cmd/broadcast"

while IFS= read -r payload; do

  MSG_ID="$(echo "$payload" | jq -r '.id // empty' 2>/dev/null)"
  MSG_FROM="$(echo "$payload" | jq -r '.from // "unknown"' 2>/dev/null)"
  MSG_TYPE="$(echo "$payload" | jq -r '.type // empty' 2>/dev/null)"
  TASK_BODY="$(echo "$payload" | jq -r '.body // empty' 2>/dev/null)"

  if [[ "$MSG_TYPE" != "task" || -z "$TASK_BODY" ]]; then
    echo "[$(date)] Ignoring non-task message (type=$MSG_TYPE)"
    continue
  fi

  TASK_ID="${MSG_ID:-task-$(date +%s)-$$}"
  echo "[$(date)] Task $TASK_ID from $MSG_FROM: $TASK_BODY"
  echo "$payload" > "$TASK_DIR/$TASK_ID.json"

  # Increment active counter (file-based so subshells can update)
  echo $(( $(cat "$COUNTER_DIR/active" 2>/dev/null || echo 0) + 1 )) > "$COUNTER_DIR/active"

  # Publish busy status
  mosquitto_pub -h "$BROKER" -t "fleet/status/$AGENT_NAME" -r \
    -m "{\"from\":\"$AGENT_NAME\",\"type\":\"status\",\"ts\":$(date +%s),\"body\":\"busy\",\"ref\":\"$TASK_ID\"}"

  # Execute in background
  (
    TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
    RESULT_FILE="$LOG_DIR/${TASK_ID}-${TIMESTAMP}.log"

    # Discover current peers for context (|| true to survive set -e)
    PEERS=$(mosquitto_sub -h "$BROKER" -t 'fleet/status/#' -W 2 -F '%t %p' 2>/dev/null \
      | while IFS= read -r pline; do
          ptopic="$(echo "$pline" | cut -d' ' -f1)"
          pbody="$(echo "$pline" | cut -d' ' -f2- | jq -r '.body // empty' 2>/dev/null)"
          pagent="$(echo "$ptopic" | sed 's|fleet/status/||')"
          [[ "$pagent" != "$AGENT_NAME" && -n "$pbody" ]] && echo "  - $pagent ($pbody)"
        done || true)

    # List available skills (|| true to survive set -e)
    SKILLS=""
    if [[ -d "$SCRIPT_DIR/skills" ]]; then
      SKILLS=$(ls "$SCRIPT_DIR/skills/"*.sh 2>/dev/null | while read -r sf; do echo "  - $(basename "$sf")"; done || true)
    fi

    SYSTEM_PROMPT="You are $AGENT_NAME, an autonomous agent on host $(hostname) ($(uname -s)/$(uname -m)).
You have full admin access via sudo. Your workspace is $SCRIPT_DIR.

## Fleet Protocol
You are part of a fleet of AI agents communicating over MQTT (broker: $BROKER).
DO NOT publish to MQTT yourself — your daemon handles all result/status/log publishing automatically.
Just write your answer to stdout and the daemon will deliver it.

If you need to send a task to another agent, use:
  mosquitto_pub -h $BROKER -t 'fleet/cmd/<agent>' -m '<json envelope>'

Envelope format for inter-agent tasks:
  {\"id\":\"task-<unix_ts>-<short_label>\",\"from\":\"$AGENT_NAME\",\"to\":\"<target>\",\"type\":\"task\",\"ts\":<unix_ts>,\"body\":\"<instruction>\"}

## Peer Agents Online
${PEERS:-  (none detected)}

## Skills
Reusable scripts in $SCRIPT_DIR/skills/ — run these instead of reinventing:
${SKILLS:-  (none yet — create new skills in $SCRIPT_DIR/skills/ when you build reusable capabilities)}

## Task Context
Task from: $MSG_FROM | Task ID: $TASK_ID
Be concise. Output findings to stdout."

    claude -p \
      --dangerously-skip-permissions \
      --system-prompt "$SYSTEM_PROMPT" \
      "$TASK_BODY" \
      </dev/null > "$RESULT_FILE" 2>&1

    # Publish result (reads from file to handle any characters)
    jq -nc \
      --arg from "$AGENT_NAME" \
      --arg to "$MSG_FROM" \
      --arg ts "$(date +%s)" \
      --arg ref "$TASK_ID" \
      --rawfile body "$RESULT_FILE" \
      '{from: $from, to: $to, type: "result", ts: ($ts | tonumber), body: $body, ref: $ref}' \
    | mosquitto_pub -h "$BROKER" -t "fleet/result/$AGENT_NAME" -s

    # Log completion
    mosquitto_pub -h "$BROKER" -t "fleet/log/$AGENT_NAME" \
      -m "{\"from\":\"$AGENT_NAME\",\"type\":\"log\",\"ts\":$(date +%s),\"body\":\"Completed $TASK_ID\"}"

    # Restore online status
    mosquitto_pub -h "$BROKER" -t "fleet/status/$AGENT_NAME" -r \
      -m "{\"from\":\"$AGENT_NAME\",\"type\":\"status\",\"ts\":$(date +%s),\"body\":\"online\"}"

    # Update file-based counters
    echo $(( $(cat "$COUNTER_DIR/completed" 2>/dev/null || echo 0) + 1 )) > "$COUNTER_DIR/completed"
    echo $(( $(cat "$COUNTER_DIR/active" 2>/dev/null || echo 1) - 1 )) > "$COUNTER_DIR/active"

    echo "[$(date)] $TASK_ID completed."
  ) &

  echo "[$(date)] $TASK_ID dispatched (pid $!)"

done < <(mosquitto_sub -h "$BROKER" \
  -t "fleet/cmd/$AGENT_NAME" \
  -t "fleet/cmd/broadcast" \
  -F '%p')
