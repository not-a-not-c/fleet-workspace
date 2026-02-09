#!/usr/bin/env bash
# resolve-broker.sh — Discover the MQTT broker via mDNS
# Prints the broker hostname (e.g., macbook.local) to stdout.
# Usage: BROKER=$(./resolve-broker.sh)
set -euo pipefail

OS="$(uname -s)"
MDNS_BROKER_TYPE="_mqtt._tcp"

if [[ "$OS" == "Darwin" ]]; then
  # dns-sd browse is interactive, so we use a timeout + parse approach
  RESULT=$(dns-sd -Z "$MDNS_BROKER_TYPE" local 2>/dev/null &
    PID=$!
    sleep 2
    kill $PID 2>/dev/null
  ) || true

  HOST=$(echo "$RESULT" | grep -oE '[a-zA-Z0-9_-]+\.local\.' | head -1 | sed 's/\.$//')

  if [[ -z "$HOST" ]]; then
    HOST=$(dns-sd -G v4 claude-fleet-broker.local 2>/dev/null &
      PID=$!
      sleep 2
      kill $PID 2>/dev/null
    ) || true
    HOST=$(echo "$HOST" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  fi

else
  # Linux: avahi-browse is much more scriptable
  RESULT=$(avahi-browse -trp "$MDNS_BROKER_TYPE" 2>/dev/null | grep '^=' | head -1)
  HOST=$(echo "$RESULT" | cut -d';' -f7)

  if [[ -n "$HOST" && "$HOST" != *.local ]]; then
    HOST="${HOST}.local"
  fi
fi

if [[ -z "${HOST:-}" ]]; then
  echo "ERROR: Could not find MQTT broker via mDNS ($MDNS_BROKER_TYPE)" >&2
  echo "Is the broker running? Was it started with --broker?" >&2
  exit 1
fi

echo "$HOST"
