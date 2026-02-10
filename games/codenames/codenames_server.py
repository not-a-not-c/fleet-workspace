#!/usr/bin/env python3
"""Codenames Game Server — Two AI agent teams play Codenames over MQTT.

Teams: macbook-prime (BLUE), jetson-coordinator (RED)
Each agent alternates between Spymaster and Guesser roles.
Web spectator page on port 8081.

Run: python3 codenames_server.py [--broker HOST]
"""

import json
import subprocess
import random
import re
import time
import threading
import os
import sys
import select
import argparse
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

# ── Constants ────────────────────────────────────────────────────────────────

RESULT_TIMEOUT = 120
MQTT_RETRIES = 3
WEB_PORT = 8081
BROKER = "Johns-MacBook-Pro-5937.local"

WORDS = [
    "AFRICA", "AGENT", "AIR", "ALIEN", "ALPS", "AMAZON", "AMBULANCE", "AMERICA",
    "ANGEL", "ANTARCTICA", "APPLE", "ARM", "ATLANTIS", "AUSTRALIA", "AZTEC",
    "BACK", "BALL", "BAND", "BANK", "BAR", "BARK", "BAT", "BATTERY", "BEACH",
    "BEAR", "BEAT", "BED", "BEIJING", "BELL", "BELT", "BERLIN", "BERMUDA",
    "BERRY", "BILL", "BLOCK", "BOARD", "BOLT", "BOMB", "BOND", "BOOM", "BOOT",
    "BOTTLE", "BOW", "BOX", "BRIDGE", "BRUSH", "BUCK", "BUFFALO", "BUG", "BUGLE",
    "BUTTON", "CALF", "CANADA", "CAP", "CAPITAL", "CAR", "CARD", "CARROT",
    "CASINO", "CAST", "CAT", "CELL", "CENTAUR", "CENTER", "CHAIR", "CHANGE",
    "CHARGE", "CHECK", "CHEST", "CHICK", "CHINA", "CHOCOLATE", "CHURCH",
    "CIRCLE", "CLIFF", "CLOAK", "CLOUD", "CLUB", "CODE", "COLD", "COMIC",
    "COMPOUND", "CONCERT", "CONDUCTOR", "CONTRACT", "COOK", "COPPER", "COTTON",
    "COURT", "COVER", "CRANE", "CRASH", "CRICKET", "CROSS", "CROWN", "CYCLE",
    "CZECH", "DANCE", "DATE", "DAY", "DEATH", "DECK", "DEGREE", "DIAMOND",
    "DICE", "DINOSAUR", "DISEASE", "DOCTOR", "DOG", "DRAFT", "DRAGON", "DRESS",
    "DRILL", "DROP", "DUCK", "DWARF", "EAGLE", "EARTH", "EGYPT", "EMBASSY",
    "ENGINE", "ENGLAND", "EUROPE", "EYE", "FACE", "FAIR", "FALL", "FAN",
    "FENCE", "FIELD", "FIGHTER", "FIGURE", "FILE", "FILM", "FIRE", "FISH",
    "FLUTE", "FLY", "FOOT", "FORCE", "FOREST", "FORK", "FRANCE", "GAME", "GAS",
    "GENIUS", "GERMANY", "GHOST", "GIANT", "GLASS", "GLOVE", "GOLD", "GRACE",
    "GRASS", "GREECE", "GREEN", "GROUND", "HAM", "HAND", "HAWK", "HEAD",
    "HEART", "HELICOPTER", "HIMALAYAS", "HOLE", "HOLLYWOOD", "HONEY", "HOOD",
    "HOOK", "HORN", "HORSE", "HORSESHOE", "HOSPITAL", "HOTEL", "ICE", "INDIA",
    "IRON", "IVORY", "JACK", "JAM", "JET", "JUPITER", "KANGAROO", "KETCHUP",
    "KEY", "KID", "KING", "KIWI", "KNIFE", "KNIGHT", "LAB", "LAP", "LASER",
    "LAWYER", "LEAD", "LEMON", "LEPRECHAUN", "LIFE", "LIGHT", "LIMOUSINE",
]

# ── Game Engine ──────────────────────────────────────────────────────────────

class CodenamesGame:
    def __init__(self):
        self.board_words = random.sample(WORDS, 25)
        self.grid = {}  # word -> color: "RED", "BLUE", "NEUTRAL", "ASSASSIN"
        self.revealed = {}  # word -> True/False
        self.move_history = []  # list of dicts
        self.current_team = "RED"  # RED goes first (9 words)
        self.current_role = "SPYMASTER"
        self.current_clue = None
        self.guesses_remaining = 0
        self.game_over = False
        self.winner = None
        self.turn_number = 0

        # Assign colors: 9 red (first team), 8 blue, 7 neutral, 1 assassin
        indices = list(range(25))
        random.shuffle(indices)
        for i, idx in enumerate(indices):
            word = self.board_words[idx]
            if i < 9:
                self.grid[word] = "RED"
            elif i < 17:
                self.grid[word] = "BLUE"
            elif i < 24:
                self.grid[word] = "NEUTRAL"
            else:
                self.grid[word] = "ASSASSIN"
            self.revealed[word] = False

    def remaining(self, color):
        return sum(1 for w in self.board_words
                   if self.grid[w] == color and not self.revealed[w])

    def reveal_word(self, word):
        """Reveal a word, return its color. Returns None if invalid."""
        word = word.upper().strip()
        if word not in self.grid:
            return None
        if self.revealed[word]:
            return None
        self.revealed[word] = True
        return self.grid[word]

    def check_win(self):
        if self.remaining("RED") == 0:
            self.game_over = True
            self.winner = "RED"
        elif self.remaining("BLUE") == 0:
            self.game_over = True
            self.winner = "BLUE"

    def board_display(self, show_colors=False):
        """Return a text representation of the board."""
        lines = []
        for row in range(5):
            row_parts = []
            for col in range(5):
                word = self.board_words[row * 5 + col]
                if self.revealed[word]:
                    color = self.grid[word]
                    row_parts.append(f"[{word}/{color}]")
                elif show_colors:
                    color = self.grid[word]
                    row_parts.append(f"{word}({color[0]})")
                else:
                    row_parts.append(word)
            lines.append("  ".join(f"{p:<20s}" for p in row_parts))
        return "\n".join(lines)

    def get_state_dict(self):
        board = []
        for i, word in enumerate(self.board_words):
            board.append({
                "word": word,
                "color": self.grid[word],
                "revealed": self.revealed[word],
                "row": i // 5,
                "col": i % 5,
            })
        return {
            "board": board,
            "current_team": self.current_team,
            "current_role": self.current_role,
            "current_clue": self.current_clue,
            "guesses_remaining": self.guesses_remaining,
            "red_remaining": self.remaining("RED"),
            "blue_remaining": self.remaining("BLUE"),
            "game_over": self.game_over,
            "winner": self.winner,
            "turn_number": self.turn_number,
            "move_history": self.move_history[-30:],
            "ts": int(time.time()),
        }


# Team/agent mapping
TEAMS = {
    "RED": "jetson-coordinator",
    "BLUE": "macbook-prime",
}

TEAM_NAMES = {v: k for k, v in TEAMS.items()}

# ── MQTT Layer ───────────────────────────────────────────────────────────────

class MQTTBridge:
    def __init__(self, broker: str, identity: str = "codenames-server"):
        self.broker = broker
        self.identity = identity

    def _mqtt_pub(self, topic: str, payload: str, retain: bool = False):
        cmd = ["mosquitto_pub", "-h", self.broker, "-t", topic, "-m", payload]
        if retain:
            cmd.append("-r")
        for attempt in range(MQTT_RETRIES):
            try:
                subprocess.run(cmd, check=True, timeout=10,
                               capture_output=True, text=True)
                return
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                if attempt == MQTT_RETRIES - 1:
                    raise RuntimeError(f"MQTT publish failed: {e}")
                time.sleep(1 * (attempt + 1))

    def send_task(self, agent: str, body: str) -> str:
        task_id = f"task-{int(time.time())}-{random.randint(1000,9999)}"
        envelope = json.dumps({
            "id": task_id,
            "from": self.identity,
            "to": agent,
            "type": "task",
            "ts": int(time.time()),
            "body": body,
        })
        self._mqtt_pub(f"fleet/cmd/{agent}", envelope)
        return task_id

    def wait_for_result(self, agent: str, task_id: str,
                        timeout: float = RESULT_TIMEOUT) -> Optional[str]:
        topic = f"fleet/result/{agent}"
        cmd = ["mosquitto_sub", "-h", self.broker, "-t", topic,
               "-F", "%p", "-W", str(int(timeout))]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, text=True)
            deadline = time.time() + timeout
            while time.time() < deadline:
                ready, _, _ = select.select([proc.stdout], [], [], 1.0)
                if ready:
                    line = proc.stdout.readline().strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("ref") == task_id:
                        proc.terminate()
                        proc.wait()
                        return msg.get("body", "")
                if proc.poll() is not None:
                    break
            if proc.poll() is None:
                proc.terminate()
                proc.wait()
        except Exception:
            pass
        return None

    def publish_game_state(self, state: dict):
        try:
            self._mqtt_pub("game/codenames/state", json.dumps(state), retain=True)
        except Exception:
            pass


# ── Prompt Builders ──────────────────────────────────────────────────────────

def build_spymaster_prompt(game: CodenamesGame, team: str):
    other_team = "BLUE" if team == "RED" else "RED"
    agent = TEAMS[team]

    prompt = f"""You are the SPYMASTER for the {team} team in a game of Codenames.
You are {agent}.

YOUR GOAL: Give a one-word clue and a number to help your guesser find {team} words.

THE BOARD (you can see all colors — R=Red, B=Blue, N=Neutral, A=Assassin):
{game.board_display(show_colors=True)}

SCORE: RED has {game.remaining('RED')} words left. BLUE has {game.remaining('BLUE')} words left.

UNREVEALED {team} WORDS YOUR GUESSER NEEDS TO FIND:
{', '.join(w for w in game.board_words if game.grid[w] == team and not game.revealed[w])}

ASSASSIN WORD (your guesser must AVOID this):
{', '.join(w for w in game.board_words if game.grid[w] == 'ASSASSIN' and not game.revealed[w])}

{other_team} WORDS (your guesser should AVOID these):
{', '.join(w for w in game.board_words if game.grid[w] == other_team and not game.revealed[w])}

RULES:
- Give exactly ONE word as a clue, and a number indicating how many board words relate to it.
- Your clue must NOT be any word on the board or a form of a word on the board.
- Think about which words your guesser might confuse with opponent's words or the assassin.

RESPOND IN EXACTLY THIS FORMAT (nothing else):
CLUE: <word>:<number>
REASONING: <brief explanation of your thinking>

Example:
CLUE: OCEAN:3
REASONING: BEACH, FISH, and WHALE all relate to ocean.
"""
    return prompt


def build_guesser_prompt(game: CodenamesGame, team: str, clue_word: str, clue_number: int):
    agent = TEAMS[team]

    prompt = f"""You are the GUESSER for the {team} team in a game of Codenames.
You are {agent}.

YOUR GOAL: Based on your spymaster's clue, guess which words on the board belong to your team ({team}).

THE BOARD (unrevealed words only — you do NOT know the colors):
{game.board_display(show_colors=False)}

REVEALED WORDS SO FAR:
{', '.join(f'{w}({game.grid[w]})' for w in game.board_words if game.revealed[w]) or 'None yet'}

SCORE: RED has {game.remaining('RED')} words left. BLUE has {game.remaining('BLUE')} words left.

YOUR SPYMASTER'S CLUE: {clue_word}:{clue_number}

This means {clue_number} words on the board relate to "{clue_word}".

RULES:
- You may guess up to {clue_number + 1} words (the clue number + 1 bonus guess).
- Guess one word at a time. After each correct guess, you can keep going or stop.
- If you guess wrong (opponent's word, neutral, or assassin), your turn ends immediately.
- If you hit the ASSASSIN, your team LOSES instantly.
- Only guess words that are still UNREVEALED on the board.

RESPOND IN EXACTLY THIS FORMAT:
GUESS: <WORD>
REASONING: <why you think this word matches the clue>

Give your SINGLE BEST guess first. You'll be asked again if you can continue.
"""
    return prompt


def build_continue_guess_prompt(game: CodenamesGame, team: str, clue_word: str,
                                 clue_number: int, guesses_made: list, remaining: int):
    agent = TEAMS[team]

    prompt = f"""You are the GUESSER for the {team} team in Codenames. You are {agent}.

YOUR SPYMASTER'S CLUE: {clue_word}:{clue_number}

YOUR GUESSES SO FAR THIS TURN:
{chr(10).join(f'  - {g["word"]} → {g["result"]}' for g in guesses_made)}

You have {remaining} guess(es) remaining this turn.

THE BOARD (current state):
{game.board_display(show_colors=False)}

REVEALED WORDS:
{', '.join(f'{w}({game.grid[w]})' for w in game.board_words if game.revealed[w]) or 'None'}

Should you keep guessing or stop (to avoid risk)?

RESPOND IN EXACTLY ONE OF THESE FORMATS:
GUESS: <WORD>
REASONING: <why>

OR:
STOP
REASONING: <why you're stopping>
"""
    return prompt


# ── Web Spectator ────────────────────────────────────────────────────────────

GAME_STATE_REF = {"state": None}

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Codenames — Fleet Spectator</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: #0d1117; color: #c9d1d9; font-family: 'SF Mono', 'Fira Code', monospace;
    min-height: 100vh; display: flex; flex-direction: column; align-items: center;
    padding: 20px;
  }
  h1 { color: #58a6ff; margin-bottom: 4px; font-size: 1.6em; }
  .subtitle { color: #8b949e; font-size: 0.85em; margin-bottom: 16px; }
  .scoreboard {
    display: flex; gap: 30px; margin-bottom: 16px; font-size: 1.1em;
  }
  .score-red { color: #f85149; font-weight: bold; }
  .score-blue { color: #58a6ff; font-weight: bold; }
  .turn-indicator {
    padding: 8px 20px; border-radius: 8px; font-weight: bold; font-size: 1em;
    margin-bottom: 16px; text-transform: uppercase; letter-spacing: 1px;
  }
  .turn-red { background: #f8514922; color: #f85149; border: 1px solid #f85149; }
  .turn-blue { background: #58a6ff22; color: #58a6ff; border: 1px solid #58a6ff; }
  .turn-over { background: #23883822; color: #3fb950; border: 1px solid #3fb950; }
  .board {
    display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px;
    max-width: 900px; width: 100%; margin-bottom: 20px;
  }
  .card {
    padding: 14px 6px; text-align: center; border-radius: 8px;
    font-weight: bold; font-size: 0.85em; letter-spacing: 0.5px;
    transition: all 0.3s; position: relative; min-height: 60px;
    display: flex; align-items: center; justify-content: center;
    border: 2px solid transparent;
  }
  .card-red { background: #f8514933; color: #f85149; border-color: #f8514966; }
  .card-blue { background: #58a6ff33; color: #58a6ff; border-color: #58a6ff66; }
  .card-neutral { background: #8b949e22; color: #8b949e; border-color: #8b949e44; }
  .card-assassin { background: #1a1a2e; color: #f0f0f0; border-color: #6e40c9; }
  .card-unrevealed {
    background: #161b22; color: #c9d1d9; border-color: #30363d;
  }
  .card-revealed { opacity: 0.55; text-decoration: line-through; }
  .card-revealed.card-assassin { opacity: 1; animation: pulse 1s infinite; }
  @keyframes pulse { 0%,100% { box-shadow: 0 0 8px #6e40c9; } 50% { box-shadow: 0 0 20px #6e40c9; } }
  .color-dot {
    position: absolute; top: 4px; right: 6px; width: 8px; height: 8px;
    border-radius: 50%;
  }
  .dot-red { background: #f85149; }
  .dot-blue { background: #58a6ff; }
  .dot-neutral { background: #8b949e; }
  .dot-assassin { background: #6e40c9; }
  .history {
    max-width: 900px; width: 100%; background: #161b22; border-radius: 10px;
    padding: 16px; max-height: 400px; overflow-y: auto; border: 1px solid #30363d;
  }
  .history h2 { color: #58a6ff; font-size: 1em; margin-bottom: 10px; }
  .move {
    padding: 8px 0; border-bottom: 1px solid #21262d; font-size: 0.82em; line-height: 1.5;
  }
  .move:last-child { border-bottom: none; }
  .move-red { color: #f85149; }
  .move-blue { color: #58a6ff; }
  .move-label { font-weight: bold; }
  .move-reasoning { color: #8b949e; font-style: italic; }
  .clue-text { color: #f0e68c; font-weight: bold; }
  .game-winner {
    font-size: 1.4em; padding: 16px 30px; border-radius: 10px;
    margin: 10px 0; animation: pulse 2s infinite;
  }
  .footer { margin-top: 16px; color: #484f58; font-size: 0.75em; }
</style>
</head>
<body>
<h1>🕵️ CODENAMES</h1>
<div class="subtitle">Fleet AI Battle — macbook-prime (BLUE) vs jetson-coordinator (RED)</div>

<div class="scoreboard">
  <span class="score-red">🔴 RED: <span id="red-score">?</span> left</span>
  <span class="score-blue">🔵 BLUE: <span id="blue-score">?</span> left</span>
</div>

<div id="turn-indicator" class="turn-indicator turn-red">Loading...</div>
<div id="winner-banner"></div>

<div class="board" id="board"></div>

<div class="history" id="history">
  <h2>📜 Move History</h2>
  <div id="moves">Waiting for game to start...</div>
</div>

<div class="footer">Auto-refreshes every 2s • Port 8081</div>

<script>
function update() {
  fetch('/api/state').then(r => r.json()).then(s => {
    // Scoreboard
    document.getElementById('red-score').textContent = s.red_remaining;
    document.getElementById('blue-score').textContent = s.blue_remaining;

    // Turn indicator
    const ti = document.getElementById('turn-indicator');
    if (s.game_over) {
      ti.className = 'turn-indicator turn-over';
      ti.textContent = 'GAME OVER — ' + (s.winner || '?') + ' WINS!';
      document.getElementById('winner-banner').innerHTML =
        '<div class="game-winner turn-over">🏆 ' + s.winner + ' TEAM WINS! 🏆</div>';
    } else {
      const cc = s.current_team === 'RED' ? 'turn-red' : 'turn-blue';
      ti.className = 'turn-indicator ' + cc;
      let text = s.current_team + ' ' + s.current_role;
      if (s.current_clue) text += ' — Clue: ' + s.current_clue;
      if (s.guesses_remaining > 0) text += ' (' + s.guesses_remaining + ' guesses left)';
      ti.textContent = text;
    }

    // Board
    const board = document.getElementById('board');
    board.innerHTML = '';
    (s.board || []).forEach(c => {
      const div = document.createElement('div');
      const colorClass = 'card-' + c.color.toLowerCase();
      div.className = 'card ' + colorClass + (c.revealed ? ' card-revealed' : '');
      // Spectators see all colors — dot on unrevealed
      if (!c.revealed) {
        div.className = 'card card-unrevealed';
        const dot = document.createElement('span');
        dot.className = 'color-dot dot-' + c.color.toLowerCase();
        div.appendChild(dot);
      }
      const txt = document.createElement('span');
      txt.textContent = c.word;
      div.appendChild(txt);
      board.appendChild(div);
    });

    // History
    const movesDiv = document.getElementById('moves');
    if (s.move_history && s.move_history.length > 0) {
      movesDiv.innerHTML = '';
      s.move_history.slice().reverse().forEach(m => {
        const d = document.createElement('div');
        d.className = 'move';
        const tc = m.team === 'RED' ? 'move-red' : 'move-blue';
        let html = '<span class="move-label ' + tc + '">' + m.team + ' ' + m.role + '</span>: ';
        if (m.type === 'clue') {
          html += '<span class="clue-text">' + m.clue + '</span>';
        } else if (m.type === 'guess') {
          const emoji = m.correct ? '✅' : (m.result === 'ASSASSIN' ? '💀' : '❌');
          html += emoji + ' guessed <b>' + m.word + '</b> → ' + m.result;
        } else if (m.type === 'stop') {
          html += '🛑 stopped guessing';
        } else if (m.type === 'timeout') {
          html += '⏰ timed out';
        }
        if (m.reasoning) {
          html += '<br><span class="move-reasoning">"' + m.reasoning + '"</span>';
        }
        d.innerHTML = html;
        movesDiv.appendChild(d);
      });
    }
  }).catch(e => {});
}
setInterval(update, 2000);
update();
</script>
</body>
</html>"""


class SpectatorHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress request logs

    def do_GET(self):
        if self.path == '/api/state':
            state = GAME_STATE_REF.get("state")
            if state:
                body = json.dumps(state).encode()
            else:
                body = json.dumps({"board": [], "move_history": [], "red_remaining": 9,
                                   "blue_remaining": 8, "current_team": "RED",
                                   "current_role": "SPYMASTER", "game_over": False,
                                   "current_clue": None, "guesses_remaining": 0,
                                   "winner": None, "turn_number": 0}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode())
        else:
            self.send_response(404)
            self.end_headers()


def start_web_server(port=WEB_PORT):
    server = HTTPServer(('0.0.0.0', port), SpectatorHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"[WEB] Spectator page: http://localhost:{port}")
    return server


# ── Parsing Helpers ──────────────────────────────────────────────────────────

def parse_clue(text: str):
    """Extract WORD:NUMBER clue from spymaster response."""
    # Try CLUE: WORD:NUMBER format
    m = re.search(r'CLUE:\s*([A-Za-z]+)\s*:\s*(\d+)', text, re.IGNORECASE)
    if m:
        return m.group(1).upper(), int(m.group(2))
    # Fallback: any WORD:NUMBER pattern
    m = re.search(r'\b([A-Za-z]+)\s*:\s*(\d+)\b', text)
    if m:
        return m.group(1).upper(), int(m.group(2))
    return None, None


def parse_guess(text: str, board_words: list):
    """Extract a guessed word from guesser response."""
    # Check for STOP
    if re.search(r'\bSTOP\b', text, re.IGNORECASE):
        return "STOP"
    # Try GUESS: WORD format
    m = re.search(r'GUESS:\s*([A-Za-z]+)', text, re.IGNORECASE)
    if m:
        word = m.group(1).upper()
        if word in board_words:
            return word
    # Fallback: find any board word mentioned
    text_upper = text.upper()
    for w in board_words:
        if w in text_upper and not text_upper.startswith("STOP"):
            return w
    return None


def parse_reasoning(text: str):
    """Extract reasoning from response."""
    m = re.search(r'REASONING:\s*(.+)', text, re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()[:200]
    return ""


# ── Game Loop ────────────────────────────────────────────────────────────────

def run_game(mqtt: MQTTBridge, game: CodenamesGame):
    print("\n" + "=" * 70)
    print("  CODENAMES — Fleet AI Battle")
    print(f"  RED (9 words): jetson-coordinator")
    print(f"  BLUE (8 words): macbook-prime")
    print("=" * 70)
    print(f"\n[BOARD] Assassin: {[w for w in game.board_words if game.grid[w] == 'ASSASSIN']}")
    print(f"[BOARD] Red words: {[w for w in game.board_words if game.grid[w] == 'RED']}")
    print(f"[BOARD] Blue words: {[w for w in game.board_words if game.grid[w] == 'BLUE']}\n")

    GAME_STATE_REF["state"] = game.get_state_dict()
    mqtt.publish_game_state(game.get_state_dict())

    while not game.game_over:
        team = game.current_team
        agent = TEAMS[team]
        other_team = "BLUE" if team == "RED" else "RED"
        game.turn_number += 1

        # ── SPYMASTER PHASE ──────────────────────────────────────────────
        game.current_role = "SPYMASTER"
        GAME_STATE_REF["state"] = game.get_state_dict()
        mqtt.publish_game_state(game.get_state_dict())

        print(f"\n{'─'*50}")
        print(f"  Turn {game.turn_number}: {team} SPYMASTER ({agent})")
        print(f"  {team} remaining: {game.remaining(team)} | {other_team} remaining: {game.remaining(other_team)}")
        print(f"{'─'*50}")

        prompt = build_spymaster_prompt(game, team)
        task_id = mqtt.send_task(agent, prompt)
        print(f"  [MQTT] Sent spymaster prompt to {agent} (task: {task_id})")

        result = mqtt.wait_for_result(agent, task_id, timeout=RESULT_TIMEOUT)

        if not result:
            print(f"  [TIMEOUT] {agent} spymaster timed out!")
            game.move_history.append({
                "team": team, "role": "SPYMASTER", "type": "timeout",
                "reasoning": "No response within time limit"
            })
            game.current_team = other_team
            continue

        print(f"  [RESPONSE] {result[:200]}")

        clue_word, clue_number = parse_clue(result)
        reasoning = parse_reasoning(result)

        if not clue_word or not clue_number:
            print(f"  [ERROR] Could not parse clue from response, skipping turn")
            game.move_history.append({
                "team": team, "role": "SPYMASTER", "type": "timeout",
                "reasoning": f"Could not parse clue: {result[:100]}"
            })
            game.current_team = other_team
            continue

        clue_str = f"{clue_word}:{clue_number}"
        game.current_clue = clue_str
        game.guesses_remaining = clue_number + 1  # +1 bonus guess

        print(f"  [CLUE] {team} Spymaster gives: {clue_str}")
        print(f"  [REASONING] {reasoning}")

        game.move_history.append({
            "team": team, "role": "SPYMASTER", "type": "clue",
            "clue": clue_str, "reasoning": reasoning
        })
        GAME_STATE_REF["state"] = game.get_state_dict()
        mqtt.publish_game_state(game.get_state_dict())

        # ── GUESSER PHASE ────────────────────────────────────────────────
        game.current_role = "GUESSER"
        guesses_made = []
        max_guesses = clue_number + 1

        while game.guesses_remaining > 0 and not game.game_over:
            GAME_STATE_REF["state"] = game.get_state_dict()
            mqtt.publish_game_state(game.get_state_dict())

            if not guesses_made:
                prompt = build_guesser_prompt(game, team, clue_word, clue_number)
            else:
                prompt = build_continue_guess_prompt(
                    game, team, clue_word, clue_number,
                    guesses_made, game.guesses_remaining
                )

            task_id = mqtt.send_task(agent, prompt)
            print(f"  [MQTT] Sent guesser prompt to {agent} (guess #{len(guesses_made)+1})")

            result = mqtt.wait_for_result(agent, task_id, timeout=RESULT_TIMEOUT)

            if not result:
                print(f"  [TIMEOUT] {agent} guesser timed out!")
                game.move_history.append({
                    "team": team, "role": "GUESSER", "type": "timeout",
                    "reasoning": "No response within time limit"
                })
                break

            print(f"  [RESPONSE] {result[:200]}")
            reasoning = parse_reasoning(result)

            # Parse guess
            unrevealed = [w for w in game.board_words if not game.revealed[w]]
            guess_word = parse_guess(result, unrevealed)

            if guess_word == "STOP":
                print(f"  [STOP] {team} Guesser stops guessing")
                game.move_history.append({
                    "team": team, "role": "GUESSER", "type": "stop",
                    "reasoning": reasoning
                })
                break

            if not guess_word:
                print(f"  [ERROR] Could not parse guess, ending turn")
                game.move_history.append({
                    "team": team, "role": "GUESSER", "type": "stop",
                    "reasoning": f"Could not parse: {result[:100]}"
                })
                break

            # Reveal the word
            color = game.reveal_word(guess_word)
            if color is None:
                print(f"  [ERROR] Invalid word: {guess_word}")
                break

            correct = (color == team)
            game.guesses_remaining -= 1

            guesses_made.append({
                "word": guess_word, "result": color, "correct": correct
            })

            print(f"  [GUESS] {guess_word} → {color} {'✅' if correct else '❌'}")

            game.move_history.append({
                "team": team, "role": "GUESSER", "type": "guess",
                "word": guess_word, "result": color, "correct": correct,
                "reasoning": reasoning
            })

            # Check assassin
            if color == "ASSASSIN":
                game.game_over = True
                game.winner = other_team
                print(f"\n  💀 ASSASSIN HIT! {team} ({agent}) loses! {other_team} wins!")
                break

            # Check win condition
            game.check_win()
            if game.game_over:
                print(f"\n  🏆 {game.winner} WINS! All words found!")
                break

            # Wrong guess ends turn
            if not correct:
                print(f"  [WRONG] Turn ends for {team}")
                break

            # Correct but no more guesses
            if game.guesses_remaining <= 0:
                print(f"  [DONE] No more guesses for {team}")
                break

            time.sleep(1)

        # Switch teams
        game.current_clue = None
        game.guesses_remaining = 0
        game.current_team = other_team
        GAME_STATE_REF["state"] = game.get_state_dict()
        mqtt.publish_game_state(game.get_state_dict())
        time.sleep(2)

    # Game over
    print("\n" + "=" * 70)
    print(f"  GAME OVER — Winner: {game.winner}")
    print(f"  Final score — RED: {game.remaining('RED')} left, BLUE: {game.remaining('BLUE')} left")
    print(f"  Total turns: {game.turn_number}")
    print("=" * 70)

    GAME_STATE_REF["state"] = game.get_state_dict()
    mqtt.publish_game_state(game.get_state_dict())


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Codenames Game Server")
    parser.add_argument("--broker", default=BROKER, help="MQTT broker host")
    parser.add_argument("--port", type=int, default=WEB_PORT, help="Web spectator port")
    args = parser.parse_args()

    web_port = args.port
    broker = args.broker
    print(f"[INIT] Broker: {broker}")

    # Test MQTT
    try:
        subprocess.run(
            ["mosquitto_pub", "-h", broker, "-t", "game/codenames/ping", "-m", "test"],
            check=True, timeout=5, capture_output=True)
        print("[INIT] MQTT connection OK")
    except Exception as e:
        print(f"[INIT] MQTT connection failed: {e}")
        sys.exit(1)

    mqtt = MQTTBridge(broker)
    game = CodenamesGame()

    # Start web server
    start_web_server(port=web_port)

    # Run game
    try:
        run_game(mqtt, game)
    except KeyboardInterrupt:
        print("\n[EXIT] Game interrupted")

    # Keep web server alive for viewing final state
    print("[INFO] Web spectator still running. Ctrl+C to exit.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
