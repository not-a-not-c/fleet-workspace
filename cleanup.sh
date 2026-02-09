#!/usr/bin/env bash
# cleanup.sh — Kill duplicate daemons and stale MQTT processes
# Run this BEFORE starting any new long-running service.
set -euo pipefail

CLEANED=0
REPORT=""

log() { REPORT+="  $1"$'\n'; }

# --- Helper: kill duplicates of a process, keeping only the newest ---
# Usage: dedup_process <pgrep_pattern> <label>
dedup_process() {
    local pattern="$1"
    local label="$2"

    # Get PIDs sorted oldest-first (by start time)
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null | xargs -I{} ps -o pid=,lstart= -p {} 2>/dev/null \
           | sort -k2,6 | awk '{print $1}') || true

    local count
    count=$(echo "$pids" | grep -c '[0-9]' || true)

    if [ "$count" -le 1 ]; then
        log "[$label] OK — $count instance(s) running"
        return
    fi

    # Kill all but the last (newest) PID
    local newest
    newest=$(echo "$pids" | tail -1)
    local to_kill
    to_kill=$(echo "$pids" | head -n -1)

    local killed=0
    for pid in $to_kill; do
        if kill "$pid" 2>/dev/null; then
            killed=$((killed + 1))
            CLEANED=$((CLEANED + 1))
        fi
    done
    log "[$label] Killed $killed duplicate(s), kept PID $newest"
}

# --- Helper: kill stale mosquitto processes older than 1 hour ---
kill_stale_mqtt() {
    local label="$1"
    local now
    now=$(date +%s)
    local killed=0

    for pid in $(pgrep -f 'mosquitto_(sub|pub)' 2>/dev/null || true); do
        # Get process start time as epoch
        local start_epoch
        start_epoch=$(ps -o lstart= -p "$pid" 2>/dev/null | xargs -I{} date -j -f "%c" "{}" +%s 2>/dev/null) || continue
        local age=$(( now - start_epoch ))
        if [ "$age" -gt 3600 ]; then
            if kill "$pid" 2>/dev/null; then
                killed=$((killed + 1))
                CLEANED=$((CLEANED + 1))
            fi
        fi
    done
    log "[$label] Killed $killed stale process(es) older than 1 hour"
}

echo "=== Process Cleanup $(date '+%Y-%m-%d %H:%M:%S') ==="
echo ""

# 1. Deduplicate agent-daemon.sh
dedup_process 'agent-daemon\.sh' 'agent-daemon'

# 2. Deduplicate inference-server.py
dedup_process 'inference-server\.py' 'inference-server'

# 3. Deduplicate fleet-chat-sub.py
dedup_process 'fleet-chat-sub\.py' 'fleet-chat-sub'

# 4. Kill stale mosquitto_sub / mosquitto_pub
kill_stale_mqtt 'mosquitto-stale'

echo "$REPORT"
if [ "$CLEANED" -gt 0 ]; then
    echo "Total cleaned: $CLEANED process(es)"
else
    echo "Nothing to clean. All processes healthy."
fi
