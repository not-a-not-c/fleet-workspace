#!/usr/bin/env python3
"""
Home Security Dashboard — Cloud-less, 100% local.
Serves on port 8083. Subscribes to MQTT for detection events from Jetson.
Proxies camera snapshots. Stores events in SQLite.
"""

import asyncio
import json
import sqlite3
import time
import os
import sys
import hashlib
import threading
import base64
import io
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import subprocess

# Config
PORT = 8083
MQTT_BROKER = "Johns-MacBook-Pro-5937.local"
MQTT_TOPIC_EVENTS = "fleet/security/events"
MQTT_TOPIC_SNAPSHOTS = "fleet/security/snapshots"
MQTT_TOPIC_STATUS = "fleet/security/status"
CAMERA_IP = "192.168.1.224"
CAMERA_SNAPSHOT_URL = f"http://{CAMERA_IP}/cgi-bin/snapshot.cgi?stream=1"
CAMERA_USER = "admin"
CAMERA_PASS = "123456"
CAMERA_RTSP = f"rtsp://{CAMERA_IP}:554/stream0?username=admin&password=E10ADC3949BA59ABBE56E057F20F883E"
DB_PATH = os.path.join(os.path.dirname(__file__), "security.db")
SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), "snapshots")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# ── SQLite Setup ──────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            event_type TEXT NOT NULL,
            confidence REAL DEFAULT 0,
            description TEXT,
            snapshot_path TEXT,
            camera_ip TEXT,
            source TEXT DEFAULT 'jetson',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_status (
            component TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            last_seen REAL NOT NULL,
            details TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
    conn.commit()
    conn.close()

def db_insert_event(event):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO events (ts, event_type, confidence, description, snapshot_path, camera_ip, source) VALUES (?,?,?,?,?,?,?)",
        (event.get("ts", time.time()), event.get("type", "unknown"),
         event.get("confidence", 0), event.get("description", ""),
         event.get("snapshot_path", ""), event.get("camera_ip", CAMERA_IP),
         event.get("source", "jetson"))
    )
    conn.commit()
    conn.close()

def db_get_events(limit=50, since=None):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if since:
        rows = conn.execute(
            "SELECT * FROM events WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
            (since, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def db_get_timeline(hours=24):
    """Get hourly event counts for timeline."""
    conn = sqlite3.connect(DB_PATH)
    since = time.time() - (hours * 3600)
    rows = conn.execute("""
        SELECT
            CAST((ts - ?) / 3600 AS INTEGER) as hour_bucket,
            event_type,
            COUNT(*) as count
        FROM events
        WHERE ts >= ?
        GROUP BY hour_bucket, event_type
        ORDER BY hour_bucket
    """, (since, since)).fetchall()
    conn.close()

    timeline = {}
    for row in rows:
        h = int(row[0])
        etype = row[1]
        count = row[2]
        if h not in timeline:
            timeline[h] = {}
        timeline[h][etype] = count
    return timeline

def db_update_status(component, status, details=""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO system_status (component, status, last_seen, details) VALUES (?,?,?,?)",
        (component, status, time.time(), details)
    )
    conn.commit()
    conn.close()

def db_get_status():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM system_status").fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── Camera Snapshot ───────────────────────────────────────────────────────
latest_snapshot = {"data": None, "ts": 0, "ok": False}
snapshot_lock = threading.Lock()

def fetch_camera_snapshot():
    """Fetch a JPEG snapshot from the camera using digest auth."""
    try:
        import urllib.request
        import urllib.error

        # Use curl for digest auth (more reliable)
        result = subprocess.run(
            ["curl", "-s", "--connect-timeout", "5", "--max-time", "10",
             "--digest", "-u", f"{CAMERA_USER}:{CAMERA_PASS}",
             CAMERA_SNAPSHOT_URL],
            capture_output=True, timeout=15
        )
        if result.returncode == 0 and len(result.stdout) > 1000:
            # Verify it's actually a JPEG
            if result.stdout[:2] == b'\xff\xd8':
                return result.stdout
        return None
    except Exception as e:
        print(f"[SNAP] Error fetching snapshot: {e}", file=sys.stderr)
        return None

def snapshot_worker():
    """Background thread: fetch camera snapshots every 8 seconds."""
    global latest_snapshot
    while True:
        try:
            data = fetch_camera_snapshot()
            with snapshot_lock:
                if data:
                    latest_snapshot = {"data": data, "ts": time.time(), "ok": True}
                    db_update_status("camera", "online", f"Last frame: {datetime.now().strftime('%H:%M:%S')}")
                else:
                    if latest_snapshot["ok"]:
                        db_update_status("camera", "degraded", "Snapshot fetch failed")
        except Exception as e:
            print(f"[SNAP] Worker error: {e}", file=sys.stderr)
        time.sleep(8)

# ── MQTT Subscriber ───────────────────────────────────────────────────────
def mqtt_worker():
    """Subscribe to security events from Jetson via MQTT."""
    while True:
        try:
            proc = subprocess.Popen(
                ["mosquitto_sub", "-h", MQTT_BROKER,
                 "-t", MQTT_TOPIC_EVENTS,
                 "-t", MQTT_TOPIC_STATUS,
                 "-t", MQTT_TOPIC_SNAPSHOTS,
                 "-v"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            db_update_status("mqtt", "online", "Connected to broker")

            for line in proc.stdout:
                try:
                    line = line.decode("utf-8", errors="replace").strip()
                    if not line:
                        continue

                    # Parse topic and payload
                    parts = line.split(" ", 1)
                    if len(parts) < 2:
                        continue
                    topic, payload = parts[0], parts[1]

                    if topic == MQTT_TOPIC_EVENTS:
                        event = json.loads(payload)
                        print(f"[MQTT] Event: {event.get('type', '?')} - {event.get('description', '')}")
                        db_insert_event(event)

                        # If event has a base64 snapshot, save it
                        if event.get("snapshot_b64"):
                            snap_data = base64.b64decode(event["snapshot_b64"])
                            snap_name = f"event_{int(event.get('ts', time.time()))}_{event.get('type','unknown')}.jpg"
                            snap_path = os.path.join(SNAPSHOT_DIR, snap_name)
                            with open(snap_path, "wb") as f:
                                f.write(snap_data)
                            # Update event with local path
                            conn = sqlite3.connect(DB_PATH)
                            conn.execute(
                                "UPDATE events SET snapshot_path = ? WHERE ts = ? AND event_type = ?",
                                (snap_name, event.get("ts"), event.get("type"))
                            )
                            conn.commit()
                            conn.close()

                    elif topic == MQTT_TOPIC_SNAPSHOTS:
                        # Raw snapshot sent as base64
                        try:
                            msg = json.loads(payload)
                            if msg.get("data"):
                                snap_data = base64.b64decode(msg["data"])
                                snap_name = f"snap_{int(msg.get('ts', time.time()))}.jpg"
                                snap_path = os.path.join(SNAPSHOT_DIR, snap_name)
                                with open(snap_path, "wb") as f:
                                    f.write(snap_data)
                        except:
                            pass

                    elif topic == MQTT_TOPIC_STATUS:
                        status = json.loads(payload)
                        comp = status.get("component", "unknown")
                        db_update_status(comp, status.get("status", "unknown"), status.get("details", ""))

                except json.JSONDecodeError:
                    print(f"[MQTT] Invalid JSON: {line[:100]}", file=sys.stderr)
                except Exception as e:
                    print(f"[MQTT] Error processing: {e}", file=sys.stderr)

        except FileNotFoundError:
            print("[MQTT] mosquitto_sub not found, MQTT disabled", file=sys.stderr)
            db_update_status("mqtt", "offline", "mosquitto_sub not installed")
            break
        except Exception as e:
            print(f"[MQTT] Connection error: {e}, reconnecting...", file=sys.stderr)
            db_update_status("mqtt", "reconnecting", str(e))
            time.sleep(5)

# ── HTTP Dashboard Server ─────────────────────────────────────────────────
class DashboardHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Suppress default logging for clean output
        pass

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_jpeg(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", len(data))
        self.send_header("Cache-Control", "no-cache, no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        try:
            if path == "/" or path == "/index.html":
                self.serve_dashboard()
            elif path == "/api/snapshot":
                self.api_snapshot()
            elif path == "/api/events":
                limit = int(params.get("limit", [50])[0])
                since = params.get("since", [None])[0]
                if since:
                    since = float(since)
                self.send_json(db_get_events(limit=limit, since=since))
            elif path == "/api/timeline":
                hours = int(params.get("hours", [24])[0])
                self.send_json(db_get_timeline(hours=hours))
            elif path == "/api/status":
                self.api_status()
            elif path.startswith("/api/event-snapshot/"):
                self.api_event_snapshot(path.split("/")[-1])
            elif path.startswith("/snapshots/"):
                self.serve_snapshot(path.split("/")[-1])
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not Found")
        except Exception as e:
            print(f"[HTTP] Error: {e}", file=sys.stderr)
            self.send_json({"error": str(e)}, 500)

    def serve_dashboard(self):
        html = DASHBOARD_HTML
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def api_snapshot(self):
        with snapshot_lock:
            if latest_snapshot["data"] and latest_snapshot["ok"]:
                self.send_jpeg(latest_snapshot["data"])
                return
        # No cached snapshot, try direct fetch
        data = fetch_camera_snapshot()
        if data:
            self.send_jpeg(data)
        else:
            self.send_json({"error": "Camera offline"}, 503)

    def api_event_snapshot(self, filename):
        """Serve a saved event snapshot."""
        filepath = os.path.join(SNAPSHOT_DIR, filename)
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                self.send_jpeg(f.read())
        else:
            self.send_json({"error": "Snapshot not found"}, 404)

    def serve_snapshot(self, filename):
        """Serve from snapshots directory."""
        filepath = os.path.join(SNAPSHOT_DIR, filename)
        if os.path.exists(filepath) and not ".." in filename:
            with open(filepath, "rb") as f:
                self.send_jpeg(f.read())
        else:
            self.send_json({"error": "Not found"}, 404)

    def api_status(self):
        statuses = db_get_status()
        status_map = {s["component"]: s for s in statuses}

        # Check camera freshness
        with snapshot_lock:
            cam_age = time.time() - latest_snapshot["ts"] if latest_snapshot["ts"] else 999
            cam_ok = latest_snapshot["ok"] and cam_age < 30

        # Build system health
        now = time.time()
        health = {
            "camera": {
                "status": "online" if cam_ok else "offline",
                "ip": CAMERA_IP,
                "rtsp": CAMERA_RTSP.split("?")[0],
                "last_frame_age": round(cam_age, 1),
                "resolution": "3840x2160 (4K)"
            },
            "detector": status_map.get("detector", {
                "status": "unknown", "last_seen": 0, "details": "Waiting for Jetson..."
            }),
            "mqtt": status_map.get("mqtt", {
                "status": "unknown", "last_seen": 0, "details": ""
            }),
            "dashboard": {
                "status": "online",
                "uptime": round(now - server_start_time, 1),
                "port": PORT
            }
        }

        # Detect stale components
        for comp in ["detector", "mqtt"]:
            if comp in status_map:
                age = now - status_map[comp]["last_seen"]
                if age > 60:
                    health[comp]["status"] = "stale"
                    health[comp]["age"] = round(age, 1)

        self.send_json(health)


# ── Dashboard HTML ────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Home Security — Local Dashboard</title>
<style>
  :root {
    --bg: #0a0e17;
    --surface: #111827;
    --surface2: #1a2236;
    --border: #1e2d4a;
    --text: #e0e6f0;
    --text2: #8892a4;
    --accent: #3b82f6;
    --green: #22c55e;
    --red: #ef4444;
    --orange: #f59e0b;
    --purple: #a855f7;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro', 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    overflow-x: hidden;
  }

  .header {
    background: linear-gradient(135deg, var(--surface) 0%, var(--surface2) 100%);
    border-bottom: 1px solid var(--border);
    padding: 16px 24px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .header h1 {
    font-size: 20px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .header h1 .shield { font-size: 24px; }

  .header .meta {
    font-size: 12px;
    color: var(--text2);
    display: flex;
    gap: 16px;
    align-items: center;
  }

  .pulse {
    width: 8px; height: 8px;
    background: var(--green);
    border-radius: 50%;
    animation: pulse 2s infinite;
    display: inline-block;
  }

  @keyframes pulse {
    0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(34,197,94,0.4); }
    50% { opacity: 0.8; box-shadow: 0 0 0 6px rgba(34,197,94,0); }
  }

  .grid {
    display: grid;
    grid-template-columns: 1fr 400px;
    grid-template-rows: auto auto 1fr;
    gap: 16px;
    padding: 16px 24px;
    max-width: 1600px;
    margin: 0 auto;
  }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
  }

  .card-header {
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text2);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .card-body { padding: 16px; }

  /* Camera Feed */
  .camera-card { grid-column: 1; grid-row: 1 / 3; }

  .camera-feed {
    position: relative;
    background: #000;
    aspect-ratio: 16/9;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .camera-feed img {
    width: 100%;
    height: 100%;
    object-fit: contain;
  }

  .camera-feed .overlay {
    position: absolute;
    top: 12px;
    left: 12px;
    display: flex;
    gap: 8px;
    align-items: center;
  }

  .camera-feed .overlay .badge {
    background: rgba(0,0,0,0.7);
    backdrop-filter: blur(4px);
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 11px;
    font-weight: 500;
  }

  .camera-feed .timestamp {
    position: absolute;
    bottom: 12px;
    right: 12px;
    background: rgba(0,0,0,0.7);
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 11px;
    font-family: 'SF Mono', monospace;
    color: var(--text2);
  }

  .camera-offline {
    color: var(--text2);
    font-size: 14px;
    text-align: center;
  }

  .camera-offline .icon { font-size: 48px; margin-bottom: 12px; }

  /* Events Feed */
  .events-card { grid-column: 2; grid-row: 1 / 4; max-height: calc(100vh - 120px); }

  .events-list {
    max-height: calc(100vh - 220px);
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: var(--border) transparent;
  }

  .event-item {
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
    display: flex;
    gap: 12px;
    align-items: flex-start;
    transition: background 0.15s;
    cursor: pointer;
  }

  .event-item:hover { background: var(--surface2); }

  .event-icon {
    width: 32px;
    height: 32px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    flex-shrink: 0;
  }

  .event-icon.person { background: rgba(59,130,246,0.15); }
  .event-icon.vehicle { background: rgba(245,158,11,0.15); }
  .event-icon.animal { background: rgba(168,85,247,0.15); }
  .event-icon.package { background: rgba(34,197,94,0.15); }
  .event-icon.motion { background: rgba(239,68,68,0.15); }
  .event-icon.unknown { background: rgba(136,146,164,0.15); }

  .event-info { flex: 1; min-width: 0; }
  .event-info .type {
    font-size: 13px;
    font-weight: 600;
    text-transform: capitalize;
  }
  .event-info .desc {
    font-size: 12px;
    color: var(--text2);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .event-info .time {
    font-size: 11px;
    color: var(--text2);
    font-family: 'SF Mono', monospace;
  }

  .event-confidence {
    font-size: 11px;
    color: var(--text2);
    font-family: 'SF Mono', monospace;
    white-space: nowrap;
  }

  /* Timeline */
  .timeline-card { grid-column: 1; grid-row: 3; }

  .timeline-container {
    height: 100px;
    display: flex;
    align-items: flex-end;
    gap: 2px;
    padding: 8px 0;
  }

  .timeline-bar {
    flex: 1;
    min-width: 4px;
    border-radius: 3px 3px 0 0;
    position: relative;
    transition: height 0.3s;
    cursor: pointer;
  }

  .timeline-bar:hover { opacity: 0.8; }

  .timeline-bar .tooltip {
    display: none;
    position: absolute;
    bottom: 100%;
    left: 50%;
    transform: translateX(-50%);
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 11px;
    white-space: nowrap;
    z-index: 10;
  }

  .timeline-bar:hover .tooltip { display: block; }

  .timeline-labels {
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: var(--text2);
    padding-top: 4px;
    font-family: 'SF Mono', monospace;
  }

  /* System Health */
  .health-card { grid-column: 1; grid-row: 2; }

  .health-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
  }

  .health-item {
    background: var(--surface2);
    border-radius: 8px;
    padding: 12px;
    text-align: center;
  }

  .health-item .icon { font-size: 20px; margin-bottom: 6px; }
  .health-item .name { font-size: 11px; color: var(--text2); margin-bottom: 4px; }
  .health-item .status {
    font-size: 12px;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 4px;
    display: inline-block;
  }
  .status-online { color: var(--green); background: rgba(34,197,94,0.1); }
  .status-offline { color: var(--red); background: rgba(239,68,68,0.1); }
  .status-degraded { color: var(--orange); background: rgba(245,158,11,0.1); }
  .status-unknown { color: var(--text2); background: rgba(136,146,164,0.1); }
  .status-stale { color: var(--orange); background: rgba(245,158,11,0.1); }

  .health-item .detail {
    font-size: 10px;
    color: var(--text2);
    margin-top: 4px;
    font-family: 'SF Mono', monospace;
  }

  /* Snapshots Grid */
  .snapshots-card { grid-column: 1; }
  .snapshots-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 8px;
  }
  .snap-thumb {
    aspect-ratio: 16/9;
    border-radius: 6px;
    overflow: hidden;
    position: relative;
    background: #000;
    cursor: pointer;
    border: 1px solid var(--border);
    transition: border-color 0.15s;
  }
  .snap-thumb:hover { border-color: var(--accent); }
  .snap-thumb img {
    width: 100%;
    height: 100%;
    object-fit: cover;
  }
  .snap-thumb .label {
    position: absolute;
    bottom: 0;
    left: 0;
    right: 0;
    background: linear-gradient(transparent, rgba(0,0,0,0.8));
    padding: 4px 6px;
    font-size: 10px;
    color: #fff;
  }

  .empty-state {
    text-align: center;
    color: var(--text2);
    padding: 32px;
    font-size: 14px;
  }
  .empty-state .icon { font-size: 32px; margin-bottom: 8px; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  /* Responsive */
  @media (max-width: 1024px) {
    .grid {
      grid-template-columns: 1fr;
    }
    .camera-card { grid-column: 1; grid-row: auto; }
    .events-card { grid-column: 1; grid-row: auto; max-height: 400px; }
    .health-grid { grid-template-columns: repeat(2, 1fr); }
    .snapshots-grid { grid-template-columns: repeat(3, 1fr); }
  }
</style>
</head>
<body>

<div class="header">
  <h1><span class="shield">&#x1f6e1;</span> Home Security</h1>
  <div class="meta">
    <span><span class="pulse"></span> LIVE</span>
    <span id="clock"></span>
    <span style="color:var(--text2)">100% Local — No Cloud</span>
  </div>
</div>

<div class="grid">
  <!-- Camera Feed -->
  <div class="card camera-card">
    <div class="card-header">
      <span>Live Camera Feed</span>
      <span id="cam-res" style="font-size:11px;color:var(--text2)">4K</span>
    </div>
    <div class="camera-feed" id="camera-feed">
      <div class="camera-offline" id="cam-offline">
        <div class="icon">&#x1f4f7;</div>
        <div>Connecting to camera...</div>
      </div>
      <img id="cam-img" style="display:none" alt="Live Feed">
      <div class="overlay">
        <div class="badge" id="cam-status-badge" style="color:var(--green)">&#x25cf; LIVE</div>
      </div>
      <div class="timestamp" id="cam-timestamp"></div>
    </div>
  </div>

  <!-- System Health -->
  <div class="card health-card">
    <div class="card-header">
      <span>System Health</span>
      <span id="health-updated" style="font-size:11px;color:var(--text2)"></span>
    </div>
    <div class="card-body">
      <div class="health-grid" id="health-grid">
        <!-- Filled by JS -->
      </div>
    </div>
  </div>

  <!-- Events Feed -->
  <div class="card events-card">
    <div class="card-header">
      <span>Detection Events</span>
      <span id="event-count" style="font-size:11px;color:var(--text2)">0 events</span>
    </div>
    <div class="events-list" id="events-list">
      <div class="empty-state">
        <div class="icon">&#x1f50d;</div>
        <div>Waiting for detection events...</div>
        <div style="font-size:12px;margin-top:4px">Jetson AI detector will report here</div>
      </div>
    </div>
  </div>

  <!-- Timeline -->
  <div class="card timeline-card">
    <div class="card-header">
      <span>Event Timeline (24h)</span>
    </div>
    <div class="card-body">
      <div class="timeline-container" id="timeline"></div>
      <div class="timeline-labels">
        <span>24h ago</span>
        <span>18h</span>
        <span>12h</span>
        <span>6h</span>
        <span>Now</span>
      </div>
    </div>
  </div>
</div>

<script>
const EVENT_ICONS = {
  person: '&#x1f6b6;',
  vehicle: '&#x1f697;',
  car: '&#x1f697;',
  animal: '&#x1f43e;',
  dog: '&#x1f415;',
  cat: '&#x1f408;',
  package: '&#x1f4e6;',
  motion: '&#x26a1;',
  unknown: '&#x2753;'
};

const EVENT_COLORS = {
  person: '#3b82f6',
  vehicle: '#f59e0b',
  car: '#f59e0b',
  animal: '#a855f7',
  dog: '#a855f7',
  cat: '#a855f7',
  package: '#22c55e',
  motion: '#ef4444',
  unknown: '#8892a4'
};

function getEventIcon(type) {
  type = (type || 'unknown').toLowerCase();
  for (const [key, icon] of Object.entries(EVENT_ICONS)) {
    if (type.includes(key)) return icon;
  }
  return EVENT_ICONS.unknown;
}

function getEventColor(type) {
  type = (type || 'unknown').toLowerCase();
  for (const [key, color] of Object.entries(EVENT_COLORS)) {
    if (type.includes(key)) return color;
  }
  return EVENT_COLORS.unknown;
}

function getEventClass(type) {
  type = (type || 'unknown').toLowerCase();
  for (const key of Object.keys(EVENT_ICONS)) {
    if (type.includes(key)) return key;
  }
  return 'unknown';
}

function timeAgo(ts) {
  const diff = Date.now()/1000 - ts;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`;
  return new Date(ts*1000).toLocaleDateString();
}

function formatTime(ts) {
  return new Date(ts*1000).toLocaleTimeString();
}

// Clock
function updateClock() {
  document.getElementById('clock').textContent = new Date().toLocaleTimeString();
}
setInterval(updateClock, 1000);
updateClock();

// Camera snapshot refresh
let camOk = false;
function refreshSnapshot() {
  const img = document.getElementById('cam-img');
  const offline = document.getElementById('cam-offline');
  const badge = document.getElementById('cam-status-badge');
  const tsEl = document.getElementById('cam-timestamp');

  const testImg = new Image();
  testImg.onload = function() {
    img.src = testImg.src;
    img.style.display = 'block';
    offline.style.display = 'none';
    badge.innerHTML = '&#x25cf; LIVE';
    badge.style.color = '#22c55e';
    tsEl.textContent = new Date().toLocaleTimeString();
    camOk = true;
  };
  testImg.onerror = function() {
    if (!camOk) {
      img.style.display = 'none';
      offline.style.display = 'block';
      badge.innerHTML = '&#x25cf; OFFLINE';
      badge.style.color = '#ef4444';
    }
  };
  testImg.src = '/api/snapshot?t=' + Date.now();
}
setInterval(refreshSnapshot, 8000);
refreshSnapshot();

// Events
let lastEventTs = 0;
function refreshEvents() {
  fetch('/api/events?limit=50')
    .then(r => r.json())
    .then(events => {
      const el = document.getElementById('events-list');
      const countEl = document.getElementById('event-count');

      if (!events || events.length === 0) {
        countEl.textContent = '0 events';
        return;
      }

      countEl.textContent = events.length + ' events';

      let html = '';
      for (const ev of events) {
        const cls = getEventClass(ev.event_type);
        const icon = getEventIcon(ev.event_type);
        const conf = ev.confidence > 0 ? (ev.confidence * 100).toFixed(0) + '%' : '';
        const hasSnap = ev.snapshot_path ? 'data-snap="' + ev.snapshot_path + '"' : '';

        html += `
          <div class="event-item" ${hasSnap}>
            <div class="event-icon ${cls}">${icon}</div>
            <div class="event-info">
              <div class="type">${ev.event_type}</div>
              <div class="desc">${ev.description || 'No description'}</div>
              <div class="time">${formatTime(ev.ts)} — ${timeAgo(ev.ts)}</div>
            </div>
            ${conf ? '<div class="event-confidence">' + conf + '</div>' : ''}
          </div>`;
      }
      el.innerHTML = html;

      if (events[0] && events[0].ts > lastEventTs) {
        lastEventTs = events[0].ts;
      }
    })
    .catch(e => console.error('Events error:', e));
}
setInterval(refreshEvents, 5000);
refreshEvents();

// Timeline
function refreshTimeline() {
  fetch('/api/timeline?hours=24')
    .then(r => r.json())
    .then(data => {
      const el = document.getElementById('timeline');

      // Find max count for scaling
      let maxCount = 1;
      for (let h = 0; h < 24; h++) {
        const bucket = data[h] || {};
        const total = Object.values(bucket).reduce((a,b) => a+b, 0);
        if (total > maxCount) maxCount = total;
      }

      let html = '';
      for (let h = 0; h < 24; h++) {
        const bucket = data[h] || {};
        const total = Object.values(bucket).reduce((a,b) => a+b, 0);
        const pct = (total / maxCount) * 100;
        const height = Math.max(pct, 2);

        // Pick color by dominant type
        let dominantType = 'motion';
        let dominantCount = 0;
        for (const [t, c] of Object.entries(bucket)) {
          if (c > dominantCount) { dominantType = t; dominantCount = c; }
        }
        const color = total > 0 ? getEventColor(dominantType) : 'var(--border)';

        const hourLabel = new Date(Date.now() - (23-h)*3600*1000).getHours();
        const details = Object.entries(bucket).map(([t,c]) => `${t}: ${c}`).join(', ') || 'No events';

        html += `
          <div class="timeline-bar" style="height:${height}%;background:${color}">
            <div class="tooltip">${hourLabel}:00 — ${details}</div>
          </div>`;
      }
      el.innerHTML = html;
    })
    .catch(e => console.error('Timeline error:', e));
}
setInterval(refreshTimeline, 30000);
refreshTimeline();

// System Health
function refreshHealth() {
  fetch('/api/status')
    .then(r => r.json())
    .then(data => {
      const el = document.getElementById('health-grid');
      const updated = document.getElementById('health-updated');
      updated.textContent = 'Updated ' + new Date().toLocaleTimeString();

      const components = [
        { key: 'camera', icon: '&#x1f4f7;', name: 'Camera' },
        { key: 'detector', icon: '&#x1f9e0;', name: 'AI Detector' },
        { key: 'mqtt', icon: '&#x1f4e1;', name: 'MQTT' },
        { key: 'dashboard', icon: '&#x1f5a5;', name: 'Dashboard' }
      ];

      let html = '';
      for (const comp of components) {
        const d = data[comp.key] || {};
        const status = d.status || 'unknown';
        const detail = d.details || d.detail || '';

        html += `
          <div class="health-item">
            <div class="icon">${comp.icon}</div>
            <div class="name">${comp.name}</div>
            <div class="status status-${status}">${status.toUpperCase()}</div>
            ${detail ? '<div class="detail">' + detail + '</div>' : ''}
          </div>`;
      }
      el.innerHTML = html;
    })
    .catch(e => console.error('Health error:', e));
}
setInterval(refreshHealth, 10000);
refreshHealth();
</script>

</body>
</html>
"""

# ── Main ──────────────────────────────────────────────────────────────────
server_start_time = time.time()

def main():
    print(f"[INIT] Home Security Dashboard v1.0")
    print(f"[INIT] Database: {DB_PATH}")
    print(f"[INIT] Snapshots: {SNAPSHOT_DIR}")
    print(f"[INIT] Camera: {CAMERA_IP}")
    print(f"[INIT] MQTT Broker: {MQTT_BROKER}")

    init_db()
    db_update_status("dashboard", "online", f"Started at {datetime.now().strftime('%H:%M:%S')}")

    # Start background workers
    snap_thread = threading.Thread(target=snapshot_worker, daemon=True)
    snap_thread.start()
    print("[INIT] Snapshot worker started (8s interval)")

    mqtt_thread = threading.Thread(target=mqtt_worker, daemon=True)
    mqtt_thread.start()
    print("[INIT] MQTT subscriber started")

    # Start HTTP server
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"[INIT] Dashboard: http://localhost:{PORT}")
    print(f"[INIT] Ready! Waiting for events from Jetson detector...")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping...")
        server.shutdown()

if __name__ == "__main__":
    main()
