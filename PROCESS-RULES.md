# Process Management Rules

**These rules are mandatory for all agents. Violations cause resource leaks and operator pain.**

## Before Starting Any Process

1. **CHECK FIRST** — Never start a process without verifying one isn't already running:
   ```bash
   pgrep -f "script-name" && echo "ALREADY RUNNING" || echo "OK to start"
   ```

2. **KILL BEFORE REPLACE** — Before starting a replacement instance, kill the old one:
   ```bash
   pkill -f "script-name" 2>/dev/null; sleep 1
   ```

3. **RUN cleanup.sh** — Always run `~/agent-workspace/cleanup.sh` before starting any new long-running service.

## When Starting Background Processes

4. **SAVE THE PID** — Always capture and record the PID:
   ```bash
   nohup ./my-daemon.sh </dev/null >logs/my-daemon.log 2>&1 &
   echo $! > /tmp/my-daemon.pid
   ```

5. **VERIFY IT STARTED** — Check the PID is alive after launch:
   ```bash
   kill -0 $(cat /tmp/my-daemon.pid) 2>/dev/null && echo "Running" || echo "FAILED"
   ```

6. **NEVER FIRE-AND-FORGET** — If you `nohup &` something, you own it. Check on it.

## Session Discipline

7. **HEALTHCHECK ON START** — Run `~/agent-workspace/healthcheck.sh` at the beginning of every session.

8. **CLEANUP ON EXIT** — Run `~/agent-workspace/cleanup.sh` before ending a session if you started any background work.

## mosquitto_sub / mosquitto_pub

9. **SHORT-LIVED ONLY** — `mosquitto_pub` calls should be one-shot. Never leave a `mosquitto_pub` running.

10. **ONE SUBSCRIBER PER TOPIC** — Don't stack multiple `mosquitto_sub` on the same topic. Check first:
    ```bash
    pgrep -af "mosquitto_sub.*fleet/cmd/$(hostname)" && echo "ALREADY SUBSCRIBED"
    ```

11. **TIMEOUT OR TRAP** — Long-running `mosquitto_sub` must be wrapped with a trap or managed by the daemon. Never raw-dog a `mosquitto_sub &` without tracking it.

## Forbidden Patterns

- `nohup ./script.sh &` without checking for existing instances
- Starting multiple `mosquitto_sub` on the same topic
- Leaving `curl` or `wget` processes running indefinitely
- Starting a daemon without saving its PID
- Ignoring cleanup.sh warnings
