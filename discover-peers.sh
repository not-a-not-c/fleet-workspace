#!/usr/bin/env bash
# discover-peers.sh — Discover all Claude agents on the local network via mDNS
# Outputs: name hostname ip
# Usage: ./discover-peers.sh
set -euo pipefail

OS="$(uname -s)"
MDNS_SERVICE_TYPE="_claude-agent._tcp"

if [[ "$OS" == "Darwin" ]]; then
  BROWSE_OUTPUT=$(dns-sd -B "$MDNS_SERVICE_TYPE" local 2>/dev/null &
    PID=$!
    sleep 3
    kill $PID 2>/dev/null
  ) || true

  NAMES=$(echo "$BROWSE_OUTPUT" | awk '/Add/{print $NF}' | sort -u)

  if [[ -z "$NAMES" ]]; then
    echo "No agents found on the network." >&2
    exit 0
  fi

  echo "=== Discovered Agents ==="
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    LOOKUP=$(dns-sd -L "$name" "$MDNS_SERVICE_TYPE" local > /tmp/.dns-sd-lookup-$$ 2>&1 &
      PID=$!
      sleep 2
      kill $PID 2>/dev/null
      cat /tmp/.dns-sd-lookup-$$
      rm -f /tmp/.dns-sd-lookup-$$
    ) || true
    HOST=$(echo "$LOOKUP" | grep 'can be reached at' | head -1 | sed 's/.*can be reached at //' | sed 's/\..*//')
    HOST_FULL="${HOST}.local"
    echo "$name  ${HOST_FULL:-unknown}"
  done <<< "$NAMES"

else
  # Linux: avahi-browse does all the work
  echo "=== Discovered Agents ==="
  avahi-browse -trp "$MDNS_SERVICE_TYPE" 2>/dev/null | grep '^=' | while IFS=';' read -r _ _ _ name _ _ host ip port _; do
    echo "$name  $host  $ip"
  done
fi
