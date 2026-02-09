#!/usr/bin/env bash
# healthcheck.sh — Quick system health overview
# Run this at the START of every agent session.
set -euo pipefail

WARNINGS=0
warn() { echo "  WARNING: $1"; WARNINGS=$((WARNINGS + 1)); }
ok()   { echo "  OK: $1"; }

echo "=== Health Check $(date '+%Y-%m-%d %H:%M:%S') ==="
echo ""

# --- 1. Duplicate process detection ---
echo "[Processes]"
for proc in 'agent-daemon\.sh' 'inference-server\.py' 'fleet-chat-sub\.py'; do
    label=$(echo "$proc" | sed 's/\\//g; s/\.sh//; s/\.py//')
    count=$(pgrep -f "$proc" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$count" -gt 1 ]; then
        warn "$label has $count instances running (expected 1) — run cleanup.sh"
    elif [ "$count" -eq 1 ]; then
        ok "$label: 1 instance"
    else
        echo "  --  $label: not running"
    fi
done
echo ""

# --- 2. MQTT process count ---
echo "[MQTT Processes]"
sub_count=$(pgrep -f 'mosquitto_sub' 2>/dev/null | wc -l | tr -d ' ')
pub_count=$(pgrep -f 'mosquitto_pub' 2>/dev/null | wc -l | tr -d ' ')
echo "  mosquitto_sub: $sub_count"
echo "  mosquitto_pub: $pub_count"
total_mqtt=$((sub_count + pub_count))
if [ "$total_mqtt" -gt 10 ]; then
    warn "High MQTT process count ($total_mqtt) — check for leaks, run cleanup.sh"
else
    ok "MQTT process count normal ($total_mqtt)"
fi
echo ""

# --- 3. Disk usage ---
echo "[Disk Usage]"
# Check all mounted filesystems, flag anything over 90%
while IFS= read -r line; do
    usage=$(echo "$line" | awk '{print $5}' | tr -d '%')
    mount=$(echo "$line" | awk '{print $6}')
    fs=$(echo "$line" | awk '{print $1}')
    if [ "$usage" -ge 90 ] 2>/dev/null; then
        warn "$mount ($fs) at ${usage}% — CRITICAL, free space now"
    else
        ok "$mount at ${usage}%"
    fi
done < <(df -h 2>/dev/null | awk 'NR>1 && /^\//' )
echo ""

# --- 4. Memory usage ---
echo "[Memory]"
if command -v free &>/dev/null; then
    # Linux (Jetson)
    free -h | awk '/^Mem:/ {printf "  Total: %s  Used: %s  Free: %s  Available: %s\n", $2, $3, $4, $7}'
    mem_pct=$(free | awk '/^Mem:/ {printf "%.0f", $3/$2*100}')
    if [ "$mem_pct" -ge 90 ]; then
        warn "Memory usage at ${mem_pct}%"
    else
        ok "Memory usage at ${mem_pct}%"
    fi
elif command -v vm_stat &>/dev/null; then
    # macOS
    page_size=$(sysctl -n hw.pagesize)
    pages_free=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
    pages_active=$(vm_stat | awk '/Pages active/ {gsub(/\./,"",$3); print $3}')
    pages_inactive=$(vm_stat | awk '/Pages inactive/ {gsub(/\./,"",$3); print $3}')
    pages_wired=$(vm_stat | awk '/Pages wired/ {gsub(/\./,"",$3); print $3}')
    total_mem=$(sysctl -n hw.memsize)
    used=$(( (pages_active + pages_wired) * page_size ))
    total_gb=$(echo "scale=1; $total_mem / 1073741824" | bc)
    used_gb=$(echo "scale=1; $used / 1073741824" | bc)
    mem_pct=$(echo "scale=0; $used * 100 / $total_mem" | bc)
    echo "  Total: ${total_gb}GB  Used: ${used_gb}GB  (${mem_pct}%)"
    if [ "$mem_pct" -ge 90 ]; then
        warn "Memory usage at ${mem_pct}%"
    else
        ok "Memory usage at ${mem_pct}%"
    fi
fi
echo ""

# --- Summary ---
echo "==============================="
if [ "$WARNINGS" -gt 0 ]; then
    echo "RESULT: $WARNINGS warning(s) — action needed"
    exit 1
else
    echo "RESULT: All clear"
    exit 0
fi
