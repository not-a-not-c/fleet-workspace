#!/usr/bin/env python3
"""
Home Security Dashboard — 100% Local, No Cloud
Serves on port 8083. Subscribes to fleet/security/events via MQTT.
Stores events in SQLite. Serves live camera snapshots.
"""

import json
import sqlite3
import threading
import time
import subprocess
import os
import base64
import shutil
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# --- Config ---
DASHBOARD_PORT = 8083
MQTT_BROKER = "Johns-MacBook-Pro-5937.local"
MQTT_TOPIC_EVENTS = "fleet/security/events"
MQTT_TOPIC_STATUS = "fleet/security/status"
CAMERA_RTSP = "rtsp://admin:123456@192.168.1.224:554/stream1"
CAMERA_IP = "192.168.1.224"
DB_PATH = os.path.join(os.path.dirname(__file__), "security.db")
SNAPSHOT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "snapshots")
LIVE_SNAPSHOT = os.path.join(SNAPSHOT_DIR, "live.jpg")

os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# --- Database ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            event_type TEXT NOT NULL,
            confidence REAL,
            description TEXT,
            snapshot_filename TEXT,
            camera_ip TEXT,
            raw_json TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_status (
            component TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            details TEXT
        )
    """)
    conn.commit()
    conn.close()

def store_event(event_data):
    """Store a detection event in SQLite."""
    conn = sqlite3.connect(DB_PATH)
    try:
        # Handle snapshot if included as base64
        snapshot_filename = None
        if "snapshot_b64" in event_data:
            ts_safe = event_data.get("ts", datetime.now().isoformat()).replace(":", "-").replace(".", "-")
            snapshot_filename = f"event_{ts_safe}.jpg"
            snapshot_path = os.path.join(SNAPSHOT_DIR, snapshot_filename)
            with open(snapshot_path, "wb") as f:
                f.write(base64.b64decode(event_data["snapshot_b64"]))
        elif "snapshot_path" in event_data:
            snapshot_filename = os.path.basename(event_data["snapshot_path"])

        conn.execute(
            "INSERT INTO events (ts, event_type, confidence, description, snapshot_filename, camera_ip, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event_data.get("ts", datetime.now().isoformat()),
                event_data.get("type", "unknown"),
                event_data.get("confidence", 0.0),
                event_data.get("description", ""),
                snapshot_filename,
                event_data.get("camera_ip", CAMERA_IP),
                json.dumps(event_data),
            ),
        )
        conn.commit()
        print(f"[DB] Stored event: {event_data.get('type')} - {event_data.get('description', '')[:60]}")
    except Exception as e:
        print(f"[DB] Error storing event: {e}")
    finally:
        conn.close()

def get_recent_events(limit=50):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_event_timeline(hours=24):
    """Get event counts by hour for the timeline."""
    conn = sqlite3.connect(DB_PATH)
    since = (datetime.now() - timedelta(hours=hours)).isoformat()
    rows = conn.execute(
        """SELECT strftime('%Y-%m-%dT%H:00:00', ts) as hour, event_type, COUNT(*) as count
           FROM events WHERE ts > ? GROUP BY hour, event_type ORDER BY hour""",
        (since,),
    ).fetchall()
    conn.close()
    return [{"hour": r[0], "type": r[1], "count": r[2]} for r in rows]

def update_system_status(component, status, details=""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO system_status (component, status, last_seen, details) VALUES (?, ?, ?, ?)",
        (component, status, datetime.now().isoformat(), details),
    )
    conn.commit()
    conn.close()

def get_system_status():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM system_status").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# --- MQTT Subscriber ---
def mqtt_subscriber():
    """Subscribe to security events via mosquitto_sub."""
    update_system_status("mqtt", "starting", "Connecting to broker...")
    while True:
        try:
            proc = subprocess.Popen(
                ["mosquitto_sub", "-h", MQTT_BROKER, "-t", "fleet/security/#", "-v"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            update_system_status("mqtt", "online", f"Connected to {MQTT_BROKER}")
            print(f"[MQTT] Subscribed to fleet/security/# on {MQTT_BROKER}")

            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                # Format: "topic payload"
                parts = line.split(" ", 1)
                if len(parts) < 2:
                    continue
                topic, payload = parts[0], parts[1]

                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    print(f"[MQTT] Non-JSON on {topic}: {payload[:80]}")
                    continue

                if topic == MQTT_TOPIC_EVENTS or topic.startswith("fleet/security/events"):
                    store_event(data)
                    update_system_status("detector", "online", f"Last event: {data.get('type', 'unknown')}")
                elif topic.startswith("fleet/security/status"):
                    comp = data.get("component", "unknown")
                    update_system_status(comp, data.get("status", "unknown"), data.get("details", ""))

        except FileNotFoundError:
            print("[MQTT] mosquitto_sub not found! Install mosquitto clients.")
            update_system_status("mqtt", "error", "mosquitto_sub not found")
            time.sleep(30)
        except Exception as e:
            print(f"[MQTT] Error: {e}")
            update_system_status("mqtt", "error", str(e))
            time.sleep(5)


# --- Live Camera Snapshot ---
def camera_snapshot_loop():
    """Pull a live JPEG from the camera every 8 seconds."""
    update_system_status("camera", "starting", "Connecting...")
    while True:
        try:
            tmp_path = LIVE_SNAPSHOT + ".tmp"
            result = subprocess.run(
                [
                    "ffmpeg", "-rtsp_transport", "tcp",
                    "-i", CAMERA_RTSP,
                    "-frames:v", "1", "-update", "1", "-q:v", "3",
                    tmp_path, "-y",
                ],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and os.path.exists(tmp_path):
                shutil.move(tmp_path, LIVE_SNAPSHOT)
                update_system_status("camera", "online", f"720x480 stream1 @ {CAMERA_IP}")
            else:
                err = result.stderr[-200:] if result.stderr else "unknown"
                update_system_status("camera", "error", f"ffmpeg failed: {err}")
        except subprocess.TimeoutExpired:
            update_system_status("camera", "error", "ffmpeg timeout")
        except Exception as e:
            update_system_status("camera", "error", str(e))
        time.sleep(8)


# --- Dashboard HTML ---
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Home Security Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: #0a0e17; color: #c8d6e5; font-family: 'Segoe UI', system-ui, sans-serif; }
  .header { background: linear-gradient(135deg, #0f1923 0%, #1a2332 100%); padding: 16px 24px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e2d3d; }
  .header h1 { font-size: 1.4em; color: #00d4aa; display: flex; align-items: center; gap: 10px; }
  .header h1::before { content: '\\1F6E1'; font-size: 1.2em; }
  .header .subtitle { color: #5a6a7a; font-size: 0.85em; }
  .status-bar { display: flex; gap: 16px; }
  .status-pill { padding: 4px 12px; border-radius: 12px; font-size: 0.75em; font-weight: 600; }
  .status-online { background: #0d3320; color: #00d4aa; }
  .status-offline { background: #3d1515; color: #ff4757; }
  .status-warning { background: #3d2e15; color: #ffa502; }

  .grid { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: auto auto auto; gap: 16px; padding: 16px; max-width: 1600px; margin: 0 auto; }
  .card { background: #111a27; border: 1px solid #1e2d3d; border-radius: 12px; padding: 16px; }
  .card h2 { font-size: 0.9em; color: #5a8abf; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }

  /* Live Feed */
  .live-feed { grid-column: 1; grid-row: 1 / 3; }
  .live-feed img { width: 100%; border-radius: 8px; border: 2px solid #1e2d3d; }
  .live-indicator { display: inline-flex; align-items: center; gap: 6px; color: #ff4757; font-weight: 700; font-size: 0.8em; }
  .live-dot { width: 8px; height: 8px; background: #ff4757; border-radius: 50%; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .camera-info { margin-top: 8px; font-size: 0.8em; color: #5a6a7a; }

  /* Events Feed */
  .events-feed { grid-column: 2; grid-row: 1; max-height: 420px; overflow-y: auto; }
  .event-item { display: flex; gap: 10px; padding: 8px; border-radius: 8px; margin-bottom: 6px; background: #0d1520; border-left: 3px solid #2a3a4a; }
  .event-item.person { border-left-color: #ff4757; }
  .event-item.vehicle { border-left-color: #ffa502; }
  .event-item.animal { border-left-color: #00d4aa; }
  .event-item.package { border-left-color: #7c5cbf; }
  .event-item.motion { border-left-color: #3498db; }
  .event-icon { font-size: 1.4em; min-width: 32px; text-align: center; }
  .event-details { flex: 1; }
  .event-type { font-weight: 600; font-size: 0.85em; color: #e8e8e8; text-transform: capitalize; }
  .event-desc { font-size: 0.75em; color: #7a8a9a; margin-top: 2px; }
  .event-time { font-size: 0.7em; color: #4a5a6a; margin-top: 2px; }
  .event-confidence { font-size: 0.7em; padding: 1px 6px; border-radius: 8px; background: #1a2a3a; color: #5a9abf; }

  /* System Health */
  .system-health { grid-column: 2; grid-row: 2; }
  .health-item { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #1a2530; }
  .health-item:last-child { border-bottom: none; }
  .health-name { font-size: 0.85em; }
  .health-status { font-size: 0.75em; padding: 3px 10px; border-radius: 10px; font-weight: 600; }
  .health-online { background: #0d3320; color: #00d4aa; }
  .health-offline { background: #3d1515; color: #ff4757; }
  .health-starting { background: #3d2e15; color: #ffa502; }
  .health-details { font-size: 0.7em; color: #5a6a7a; max-width: 250px; text-align: right; }

  /* Timeline */
  .timeline { grid-column: 1 / 3; }
  .timeline-chart { display: flex; align-items: flex-end; gap: 2px; height: 100px; padding: 8px 0; }
  .timeline-bar { flex: 1; min-width: 8px; border-radius: 3px 3px 0 0; position: relative; cursor: pointer; transition: opacity 0.2s; }
  .timeline-bar:hover { opacity: 0.8; }
  .timeline-bar.person { background: #ff4757; }
  .timeline-bar.vehicle { background: #ffa502; }
  .timeline-bar.animal { background: #00d4aa; }
  .timeline-bar.package { background: #7c5cbf; }
  .timeline-bar.motion { background: #3498db; }
  .timeline-bar.empty { background: #1a2530; min-height: 4px; }
  .timeline-labels { display: flex; justify-content: space-between; font-size: 0.65em; color: #4a5a6a; margin-top: 4px; }
  .timeline-legend { display: flex; gap: 16px; margin-top: 8px; }
  .legend-item { display: flex; align-items: center; gap: 4px; font-size: 0.75em; color: #7a8a9a; }
  .legend-dot { width: 10px; height: 10px; border-radius: 2px; }

  /* Snapshots Grid */
  .snapshots { grid-column: 1 / 3; }
  .snapshot-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; }
  .snapshot-item { position: relative; border-radius: 8px; overflow: hidden; border: 1px solid #1e2d3d; }
  .snapshot-item img { width: 100%; aspect-ratio: 16/9; object-fit: cover; display: block; }
  .snapshot-overlay { position: absolute; bottom: 0; left: 0; right: 0; background: linear-gradient(transparent, rgba(0,0,0,0.85)); padding: 6px 8px; }
  .snapshot-label { font-size: 0.7em; color: #fff; font-weight: 600; text-transform: capitalize; }
  .snapshot-time { font-size: 0.6em; color: #aaa; }

  /* No events placeholder */
  .no-events { text-align: center; padding: 40px; color: #3a4a5a; }
  .no-events .icon { font-size: 2em; margin-bottom: 8px; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #0a0e17; }
  ::-webkit-scrollbar-thumb { background: #2a3a4a; border-radius: 3px; }

  @media (max-width: 900px) {
    .grid { grid-template-columns: 1fr; }
    .live-feed, .events-feed, .system-health, .timeline, .snapshots { grid-column: 1; grid-row: auto; }
    .snapshot-grid { grid-template-columns: repeat(3, 1fr); }
  }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Home Security</h1>
    <div class="subtitle">100% Local &mdash; No Cloud &mdash; All data stays on your network</div>
  </div>
  <div class="status-bar" id="headerStatus">
    <span class="status-pill status-online">System Active</span>
  </div>
</div>

<div class="grid">
  <!-- Live Camera Feed -->
  <div class="card live-feed">
    <h2>
      <span class="live-indicator"><span class="live-dot"></span> LIVE</span>
      Camera Feed
    </h2>
    <img id="liveFeed" src="/api/snapshot/live" alt="Live Camera Feed" onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 width=%22720%22 height=%22480%22><rect fill=%22%23111a27%22 width=%22720%22 height=%22480%22/><text x=%2250%25%22 y=%2250%25%22 fill=%22%233a4a5a%22 text-anchor=%22middle%22 dy=%22.3em%22 font-size=%2220%22>Camera Connecting...</text></svg>'">
    <div class="camera-info">
      <span id="cameraStatus">Connecting...</span> &bull; 192.168.1.224 &bull; Stream 1 (720x480)
    </div>
  </div>

  <!-- Detection Events -->
  <div class="card events-feed">
    <h2>Detection Events</h2>
    <div id="eventsList">
      <div class="no-events">
        <div class="icon">&#x1F4F7;</div>
        <div>Waiting for detection events...</div>
        <div style="font-size:0.8em;margin-top:4px;">Events will appear here when the AI detector identifies objects</div>
      </div>
    </div>
  </div>

  <!-- System Health -->
  <div class="card system-health">
    <h2>System Health</h2>
    <div id="healthList">
      <div class="health-item">
        <span class="health-name">Loading...</span>
      </div>
    </div>
  </div>

  <!-- Event Timeline -->
  <div class="card timeline">
    <h2>Event Timeline (24 Hours)</h2>
    <div class="timeline-chart" id="timelineChart"></div>
    <div class="timeline-labels" id="timelineLabels"></div>
    <div class="timeline-legend">
      <div class="legend-item"><div class="legend-dot" style="background:#ff4757"></div> Person</div>
      <div class="legend-item"><div class="legend-dot" style="background:#ffa502"></div> Vehicle</div>
      <div class="legend-item"><div class="legend-dot" style="background:#00d4aa"></div> Animal</div>
      <div class="legend-item"><div class="legend-dot" style="background:#7c5cbf"></div> Package</div>
      <div class="legend-item"><div class="legend-dot" style="background:#3498db"></div> Motion</div>
    </div>
  </div>

  <!-- Recent Snapshots -->
  <div class="card snapshots">
    <h2>Recent Snapshots</h2>
    <div class="snapshot-grid" id="snapshotGrid">
      <div class="no-events" style="grid-column: 1/-1;">
        <div class="icon">&#x1F5BC;</div>
        <div>No event snapshots yet</div>
      </div>
    </div>
  </div>
</div>

<script>
const EVENT_ICONS = {
  person: '\\ud83d\\udeb6',
  vehicle: '\\ud83d\\ude97',
  animal: '\\ud83d\\udc3e',
  package: '\\ud83d\\udce6',
  motion: '\\ud83d\\udca8',
  unknown: '\\u2753'
};

function formatTime(ts) {
  try {
    const d = new Date(ts);
    return d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
  } catch { return ts; }
}

function formatTimeShort(ts) {
  try {
    const d = new Date(ts);
    return d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
  } catch { return ts; }
}

// Refresh live feed
function refreshLiveFeed() {
  const img = document.getElementById('liveFeed');
  img.src = '/api/snapshot/live?t=' + Date.now();
}

// Fetch and render events
async function refreshEvents() {
  try {
    const resp = await fetch('/api/events?limit=30');
    const events = await resp.json();
    const el = document.getElementById('eventsList');

    if (events.length === 0) return;

    el.innerHTML = events.map(ev => `
      <div class="event-item ${ev.event_type || ''}">
        <div class="event-icon">${EVENT_ICONS[ev.event_type] || EVENT_ICONS.unknown}</div>
        <div class="event-details">
          <div class="event-type">${ev.event_type || 'Unknown'}
            <span class="event-confidence">${((ev.confidence || 0) * 100).toFixed(0)}%</span>
          </div>
          <div class="event-desc">${ev.description || ''}</div>
          <div class="event-time">${formatTime(ev.ts)}</div>
        </div>
      </div>
    `).join('');
  } catch(e) { console.error('Events fetch error:', e); }
}

// Fetch and render system health
async function refreshHealth() {
  try {
    const resp = await fetch('/api/health');
    const items = await resp.json();
    const el = document.getElementById('healthList');

    // Default components
    const defaults = [
      {component: 'camera', status: 'unknown', details: ''},
      {component: 'detector', status: 'unknown', details: ''},
      {component: 'mqtt', status: 'unknown', details: ''},
      {component: 'dashboard', status: 'online', details: 'Port 8083'},
    ];

    const merged = {};
    defaults.forEach(d => merged[d.component] = d);
    items.forEach(i => merged[i.component] = i);

    el.innerHTML = Object.values(merged).map(item => {
      const statusClass = item.status === 'online' ? 'health-online' :
                          item.status === 'starting' ? 'health-starting' : 'health-offline';
      const icon = item.component === 'camera' ? '\\ud83d\\udcf7' :
                   item.component === 'detector' ? '\\ud83e\\udde0' :
                   item.component === 'mqtt' ? '\\ud83d\\udce1' :
                   item.component === 'dashboard' ? '\\ud83d\\udcbb' : '\\u2699\\ufe0f';
      return `
        <div class="health-item">
          <span class="health-name">${icon} ${item.component}</span>
          <span class="health-details">${item.details || ''}</span>
          <span class="health-status ${statusClass}">${item.status}</span>
        </div>
      `;
    }).join('');

    // Update camera status text
    const cam = merged['camera'];
    if (cam) {
      document.getElementById('cameraStatus').textContent =
        cam.status === 'online' ? '\\u2705 Online' : cam.status === 'starting' ? '\\u23f3 Connecting' : '\\u274c ' + (cam.details || 'Offline');
    }
  } catch(e) { console.error('Health fetch error:', e); }
}

// Fetch and render timeline
async function refreshTimeline() {
  try {
    const resp = await fetch('/api/timeline');
    const data = await resp.json();
    const chart = document.getElementById('timelineChart');
    const labels = document.getElementById('timelineLabels');

    if (data.length === 0) {
      chart.innerHTML = '<div style="width:100%;text-align:center;color:#3a4a5a;padding:20px;">No events in the last 24 hours</div>';
      return;
    }

    // Group by hour
    const hours = {};
    data.forEach(d => {
      if (!hours[d.hour]) hours[d.hour] = {};
      hours[d.hour][d.type] = d.count;
    });

    const maxCount = Math.max(...data.map(d => d.count), 1);
    const sortedHours = Object.keys(hours).sort();

    chart.innerHTML = sortedHours.map(hour => {
      const types = hours[hour];
      const mainType = Object.keys(types).sort((a,b) => types[b] - types[a])[0];
      const total = Object.values(types).reduce((a,b) => a+b, 0);
      const height = Math.max(8, (total / maxCount) * 90);
      return `<div class="timeline-bar ${mainType}" style="height:${height}px" title="${hour}: ${total} events"></div>`;
    }).join('');

    labels.innerHTML = `<span>${formatTimeShort(sortedHours[0])}</span><span>${formatTimeShort(sortedHours[sortedHours.length-1])}</span>`;
  } catch(e) { console.error('Timeline error:', e); }
}

// Fetch and render snapshots
async function refreshSnapshots() {
  try {
    const resp = await fetch('/api/events?limit=10&with_snapshots=1');
    const events = await resp.json();
    const el = document.getElementById('snapshotGrid');

    const withSnaps = events.filter(e => e.snapshot_filename);
    if (withSnaps.length === 0) return;

    el.innerHTML = withSnaps.map(ev => `
      <div class="snapshot-item">
        <img src="/api/snapshot/${ev.snapshot_filename}" alt="${ev.event_type}" loading="lazy">
        <div class="snapshot-overlay">
          <div class="snapshot-label">${EVENT_ICONS[ev.event_type] || ''} ${ev.event_type}</div>
          <div class="snapshot-time">${formatTime(ev.ts)}</div>
        </div>
      </div>
    `).join('');
  } catch(e) { console.error('Snapshots error:', e); }
}

// Auto-refresh
setInterval(refreshLiveFeed, 8000);
setInterval(refreshEvents, 5000);
setInterval(refreshHealth, 10000);
setInterval(refreshTimeline, 30000);
setInterval(refreshSnapshots, 15000);

// Initial load
refreshEvents();
refreshHealth();
refreshTimeline();
refreshSnapshots();
</script>
</body>
</html>"""


# --- HTTP Handler ---
class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Quieter logging
        if "/api/snapshot/live" not in str(args):
            print(f"[HTTP] {args[0]}" if args else "")

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())

        elif path == "/api/events":
            limit = int(params.get("limit", [50])[0])
            events = get_recent_events(limit)
            self.send_json(events)

        elif path == "/api/timeline":
            hours = int(params.get("hours", [24])[0])
            data = get_event_timeline(hours)
            self.send_json(data)

        elif path == "/api/health":
            status = get_system_status()
            self.send_json(status)

        elif path.startswith("/api/snapshot/"):
            filename = path.split("/api/snapshot/")[1].split("?")[0]
            if filename == "live":
                filepath = LIVE_SNAPSHOT
            else:
                filepath = os.path.join(SNAPSHOT_DIR, os.path.basename(filename))

            if os.path.exists(filepath):
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.end_headers()
                with open(filepath, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()

        else:
            self.send_response(404)
            self.end_headers()

    def send_json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


# --- Main ---
def main():
    print("=" * 60)
    print("  HOME SECURITY DASHBOARD")
    print("  100% Local — No Cloud — No Subscriptions")
    print("=" * 60)

    init_db()
    update_system_status("dashboard", "online", f"Port {DASHBOARD_PORT}")

    # Start background threads
    threading.Thread(target=mqtt_subscriber, daemon=True).start()
    threading.Thread(target=camera_snapshot_loop, daemon=True).start()

    print(f"\n[Dashboard] Starting on http://0.0.0.0:{DASHBOARD_PORT}")
    print(f"[Camera] Pulling snapshots from {CAMERA_RTSP}")
    print(f"[MQTT] Subscribing to fleet/security/# on {MQTT_BROKER}")
    print()

    server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Dashboard] Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
