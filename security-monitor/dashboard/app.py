#!/usr/bin/env python3
"""
Security Monitor Dashboard — Flask + SQLite + MQTT
Cloud-less, local-only security monitoring dashboard.
Subscribes to MQTT detection events from Jetson edge device running YOLOv8.
"""

import base64
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from io import BytesIO

from flask import Flask, g, jsonify, render_template, request, send_file

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPICS = [
    ("fleet/security/events", 1),
    ("fleet/security/status", 1),
]
MQTT_RECONNECT_DELAY = 5  # seconds

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "security.db")
RETENTION_DAYS = 30

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("security-monitor")

# ---------------------------------------------------------------------------
# Flask application
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# Global state shared between MQTT thread and Flask request threads
_mqtt_connected = False
_last_event_ts = 0.0  # epoch seconds of last received event


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    """Return a per-request SQLite connection stored on Flask's `g` object."""
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA busy_timeout=5000")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _get_thread_db() -> sqlite3.Connection:
    """Return a standalone connection for use outside Flask request context."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db():
    """Create tables if they do not exist and run retention cleanup."""
    conn = _get_thread_db()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id        TEXT    NOT NULL,
                ts              TEXT    NOT NULL,
                type            TEXT    NOT NULL,
                confidence      REAL    NOT NULL DEFAULT 0.0,
                bbox_json       TEXT    DEFAULT '[]',
                thumbnail_b64   TEXT    DEFAULT '',
                camera_id       TEXT    NOT NULL DEFAULT 'cam-0',
                created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);

            CREATE TABLE IF NOT EXISTS camera_status (
                camera_id       TEXT PRIMARY KEY,
                ts              TEXT    NOT NULL,
                fps             REAL    NOT NULL DEFAULT 0.0,
                resolution      TEXT    NOT NULL DEFAULT '',
                status          TEXT    NOT NULL DEFAULT 'unknown',
                uptime_seconds  INTEGER NOT NULL DEFAULT 0
            );
        """)
        conn.commit()

        # Retention cleanup
        cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
        cur = conn.execute("DELETE FROM events WHERE created_at < ?", (cutoff,))
        if cur.rowcount:
            log.info("Retention cleanup: removed %d events older than %d days", cur.rowcount, RETENTION_DAYS)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MQTT client
# ---------------------------------------------------------------------------

def _on_connect(client, userdata, flags, reason_code, properties):
    global _mqtt_connected
    if reason_code == 0:
        _mqtt_connected = True
        log.info("MQTT connected to %s:%s", MQTT_BROKER, MQTT_PORT)
        client.subscribe(MQTT_TOPICS)
    else:
        _mqtt_connected = False
        log.warning("MQTT connection failed: reason_code=%s", reason_code)


def _on_disconnect(client, userdata, flags, reason_code, properties):
    global _mqtt_connected
    _mqtt_connected = False
    log.warning("MQTT disconnected (reason_code=%s), will auto-reconnect", reason_code)


def _on_message(client, userdata, msg):
    global _last_event_ts
    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.error("Bad MQTT payload on %s: %s", msg.topic, exc)
        return

    conn = _get_thread_db()
    try:
        if msg.topic == "fleet/security/events":
            _handle_event(conn, payload)
            _last_event_ts = time.time()
        elif msg.topic == "fleet/security/status":
            _handle_status(conn, payload)
    except Exception:
        log.exception("Error handling MQTT message on %s", msg.topic)
    finally:
        conn.close()


def _handle_event(conn, data):
    """Persist a detection event."""
    event_id = data.get("event_id", str(uuid.uuid4()))
    ts = data.get("ts", datetime.now(timezone.utc).isoformat())
    det_type = data.get("type", "unknown")
    confidence = float(data.get("confidence", 0.0))
    bbox = json.dumps(data.get("bbox", []))
    thumbnail = data.get("thumbnail_b64", "")
    camera_id = data.get("camera_id", "cam-0")

    conn.execute(
        """INSERT INTO events (event_id, ts, type, confidence, bbox_json, thumbnail_b64, camera_id)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (event_id, ts, det_type, confidence, bbox, thumbnail, camera_id),
    )
    conn.commit()
    log.debug("Stored event %s type=%s conf=%.2f", event_id, det_type, confidence)


def _handle_status(conn, data):
    """Upsert camera health status."""
    camera_id = data.get("camera_id", "cam-0")
    ts = data.get("ts", datetime.now(timezone.utc).isoformat())
    fps = float(data.get("fps", 0.0))
    resolution = data.get("resolution", "")
    status = data.get("status", "unknown")
    uptime = int(data.get("uptime_seconds", 0))

    conn.execute(
        """INSERT INTO camera_status (camera_id, ts, fps, resolution, status, uptime_seconds)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(camera_id) DO UPDATE SET
               ts=excluded.ts,
               fps=excluded.fps,
               resolution=excluded.resolution,
               status=excluded.status,
               uptime_seconds=excluded.uptime_seconds""",
        (camera_id, ts, fps, resolution, status, uptime),
    )
    conn.commit()


def start_mqtt():
    """Start the MQTT client in a background daemon thread."""
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"security-dashboard-{uuid.uuid4().hex[:8]}",
        clean_session=True,
    )
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message
    client.reconnect_delay_set(min_delay=1, max_delay=MQTT_RECONNECT_DELAY)

    def _loop():
        while True:
            try:
                log.info("Connecting MQTT to %s:%s ...", MQTT_BROKER, MQTT_PORT)
                client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                client.loop_forever()
            except OSError as exc:
                log.warning("MQTT connection error: %s — retrying in %ds", exc, MQTT_RECONNECT_DELAY)
                time.sleep(MQTT_RECONNECT_DELAY)
            except Exception:
                log.exception("Unexpected MQTT error — retrying in %ds", MQTT_RECONNECT_DELAY)
                time.sleep(MQTT_RECONNECT_DELAY)

    t = threading.Thread(target=_loop, daemon=True, name="mqtt-subscriber")
    t.start()
    log.info("MQTT subscriber thread started")


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/connection")
def api_connection():
    """Return MQTT connection status and last event age."""
    now = time.time()
    age = now - _last_event_ts if _last_event_ts > 0 else -1
    return jsonify({
        "mqtt_connected": _mqtt_connected,
        "last_event_age_seconds": round(age, 1) if age >= 0 else None,
    })


@app.route("/api/events")
def api_events():
    """Paginated event listing with optional type filter."""
    db = get_db()
    limit = min(int(request.args.get("limit", 50)), 500)
    offset = int(request.args.get("offset", 0))
    det_type = request.args.get("type", None)

    if det_type:
        rows = db.execute(
            "SELECT id, event_id, ts, type, confidence, bbox_json, camera_id, created_at "
            "FROM events WHERE type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            (det_type, limit, offset),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, event_id, ts, type, confidence, bbox_json, camera_id, created_at "
            "FROM events ORDER BY ts DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()

    events = []
    for r in rows:
        events.append({
            "id": r["id"],
            "event_id": r["event_id"],
            "ts": r["ts"],
            "type": r["type"],
            "confidence": r["confidence"],
            "bbox": json.loads(r["bbox_json"]) if r["bbox_json"] else [],
            "camera_id": r["camera_id"],
            "created_at": r["created_at"],
        })

    total = db.execute(
        "SELECT COUNT(*) FROM events" + (" WHERE type = ?" if det_type else ""),
        (det_type,) if det_type else (),
    ).fetchone()[0]

    return jsonify({"events": events, "total": total, "limit": limit, "offset": offset})


@app.route("/api/events/timeline")
def api_events_timeline():
    """Hourly event counts for the last N hours (default 24)."""
    db = get_db()
    hours = int(request.args.get("hours", 24))
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    rows = db.execute(
        """SELECT strftime('%%Y-%%m-%%dT%%H:00:00', ts) AS hour,
                  type,
                  COUNT(*) AS cnt
           FROM events
           WHERE ts >= ?
           GROUP BY hour, type
           ORDER BY hour""",
        (cutoff,),
    ).fetchall()

    # Build a dict keyed by hour
    timeline = {}
    for r in rows:
        h = r["hour"]
        if h not in timeline:
            timeline[h] = {"hour": h, "total": 0, "by_type": {}}
        timeline[h]["total"] += r["cnt"]
        timeline[h]["by_type"][r["type"]] = r["cnt"]

    # Fill in missing hours so the chart has a continuous axis
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    filled = []
    for i in range(hours, -1, -1):
        h = (now_utc - timedelta(hours=i)).strftime("%Y-%m-%dT%H:00:00")
        if h in timeline:
            filled.append(timeline[h])
        else:
            filled.append({"hour": h, "total": 0, "by_type": {}})

    return jsonify({"timeline": filled, "hours": hours})


@app.route("/api/stats")
def api_stats():
    """Detection counts by type — last hour and last 24 hours."""
    db = get_db()
    now_utc = datetime.now(timezone.utc)
    one_hour_ago = (now_utc - timedelta(hours=1)).isoformat()
    twenty_four_ago = (now_utc - timedelta(hours=24)).isoformat()

    rows_1h = db.execute(
        "SELECT type, COUNT(*) AS cnt FROM events WHERE ts >= ? GROUP BY type",
        (one_hour_ago,),
    ).fetchall()

    rows_24h = db.execute(
        "SELECT type, COUNT(*) AS cnt FROM events WHERE ts >= ? GROUP BY type",
        (twenty_four_ago,),
    ).fetchall()

    total_all = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    # Recent 5 minutes for alert check
    five_min_ago = (now_utc - timedelta(minutes=5)).isoformat()
    person_5m = db.execute(
        "SELECT COUNT(*) FROM events WHERE type = 'person' AND ts >= ?",
        (five_min_ago,),
    ).fetchone()[0]

    stats_1h = {r["type"]: r["cnt"] for r in rows_1h}
    stats_24h = {r["type"]: r["cnt"] for r in rows_24h}

    return jsonify({
        "last_hour": stats_1h,
        "last_24h": stats_24h,
        "total_all_time": total_all,
        "person_last_5min": person_5m,
        "alert": person_5m > 5,
    })


@app.route("/api/cameras")
def api_cameras():
    """Latest health for all known cameras."""
    db = get_db()
    rows = db.execute("SELECT * FROM camera_status ORDER BY camera_id").fetchall()
    cameras = []
    for r in rows:
        cameras.append({
            "camera_id": r["camera_id"],
            "ts": r["ts"],
            "fps": r["fps"],
            "resolution": r["resolution"],
            "status": r["status"],
            "uptime_seconds": r["uptime_seconds"],
        })
    return jsonify({"cameras": cameras})


@app.route("/api/snapshot")
def api_snapshot():
    """Return the latest thumbnail as a JPEG image."""
    db = get_db()
    row = db.execute(
        "SELECT thumbnail_b64 FROM events WHERE thumbnail_b64 != '' ORDER BY ts DESC LIMIT 1"
    ).fetchone()

    if row and row["thumbnail_b64"]:
        try:
            img_bytes = base64.b64decode(row["thumbnail_b64"])
            return send_file(BytesIO(img_bytes), mimetype="image/jpeg")
        except Exception:
            pass

    # Return a 1x1 transparent pixel as fallback
    pixel = base64.b64decode(
        "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoH"
        "BwYIDAoMCwsKCwsKDA0QDA4RCwsRFBcSExMXFxoaGBoeHh4eHh4eHh7/2wBDAQME"
        "BAUEBQkFBQkeEhASHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4e"
        "Hh4eHh4eHh4eHh4eHh7/wAARCAABAAEDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEA"
        "AAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIh"
        "MUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6"
        "Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZ"
        "mqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx"
        "8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREA"
        "AgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAV"
        "YnLRChYkNOEl8RcYI4Q/RFhHRUYnJCk6NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNk"
        "ZWZnaGlqc3R1dnd4eXqCg4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4"
        "ubrCw8TFxsfIycrS09TV1tfY2drh4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIR"
        "AxEAPwD9U6KKKAPkD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP/Z"
    )
    return send_file(BytesIO(pixel), mimetype="image/jpeg")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Initializing database at %s", DB_PATH)
    init_db()
    start_mqtt()
    log.info("Starting Security Monitor Dashboard on http://0.0.0.0:8083")
    app.run(host="0.0.0.0", port=8083, debug=False, threaded=True)
