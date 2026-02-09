#!/bin/bash
# Fleet Health Publisher for macbook-prime
# Publishes system stats to MQTT every 10 seconds

BROKER="Johns-MacBook-Pro-5937.local"
TOPIC="fleet/health/macbook-prime"
INTERVAL=10

while true; do
  # CPU usage (percentage across all cores)
  CPU_USAGE=$(ps -A -o %cpu | awk '{sum+=$1} END {printf "%.1f", sum}')

  # Memory stats
  MEM_TOTAL=16384  # 16GB in MB
  MEM_USED=$(memory_pressure 2>/dev/null | grep "System-wide memory free percentage" | awk '{printf "%.0f", (100 - $NF) / 100 * 16384}' 2>/dev/null)
  if [ -z "$MEM_USED" ]; then
    # Fallback: parse vm_stat
    PAGES_ACTIVE=$(vm_stat | grep "Pages active" | awk '{print $3}' | tr -d '.')
    PAGES_WIRED=$(vm_stat | grep "Pages wired" | awk '{print $4}' | tr -d '.')
    MEM_USED=$(( (PAGES_ACTIVE + PAGES_WIRED) * 16384 / 1048576 ))
  fi

  # Disk usage
  DISK_USED=$(df -h / | tail -1 | awk '{print $5}' | tr -d '%')
  DISK_TOTAL=$(df -h / | tail -1 | awk '{print $2}')

  # CPU temp (macOS - may need powermetrics with sudo)
  CPU_TEMP="N/A"

  # Load average
  LOAD=$(uptime | awk -F'load averages:' '{print $2}' | xargs)

  # Uptime
  UPTIME=$(uptime | awk -F'up ' '{print $2}' | awk -F',' '{print $1 "," $2}')

  TIMESTAMP=$(date +%s)

  PAYLOAD=$(cat <<EOF
{"agent":"macbook-prime","ts":${TIMESTAMP},"hardware":{"chip":"Apple M1","cores":8,"memory_gb":16},"stats":{"cpu_usage_pct":${CPU_USAGE},"mem_used_mb":${MEM_USED},"mem_total_mb":${MEM_TOTAL},"disk_used_pct":${DISK_USED},"disk_total":"${DISK_TOTAL}","load_avg":"${LOAD}","cpu_temp":"${CPU_TEMP}","uptime":"${UPTIME}"}}
EOF
)

  mosquitto_pub -h "$BROKER" -t "$TOPIC" -m "$PAYLOAD" 2>/dev/null
  sleep "$INTERVAL"
done
