import socket
import time
import json
import random
import threading
import argparse
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

parser = argparse.ArgumentParser()
parser.add_argument("--machine",  default="Machine-A")
parser.add_argument("--server",   default="127.0.0.1")
parser.add_argument("--port",     default=20005, type=int)
parser.add_argument("--interval", default=1.0,   type=float)
parser.add_argument("--ui-port",  default=8765,  type=int)
args = parser.parse_args()

# shared state — read/written by send loop and HTTP handler
state = {
    "machine":   args.machine,
    "server":    args.server,
    "port":      args.port,
    "interval":  args.interval,
    "running":   True,
    "signal":    "none",    # last backpressure signal from server
    "sent":      0,
}
lock = threading.Lock()

# --- log content ----------------------------------------------------------

LEVELS     = ["INFO", "INFO", "DEBUG", "WARN", "ERROR"]
COMPONENTS = ["AuthService", "Database", "Cache", "APIGateway", "Scheduler", "FileWatcher"]
MESSAGES   = {
    "INFO":  ["Request completed in {n}ms", "Cache hit ratio {n}%", "Startup OK", "Config reloaded"],
    "DEBUG": ["Query returned {n} rows",    "Buffer flushed ({n} bytes)", "Retry attempt {n}/3"],
    "WARN":  ["Slow query: {n}ms",          "Memory at {n}%",            "Queue depth: {n}"],
    "ERROR": ["Connection timeout",          "Write failed: constraint",  "Token expired"],
}

def make_log():
    level = random.choice(LEVELS)
    msg   = random.choice(MESSAGES[level]).format(n=random.randint(1, 999))
    return {
        "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "machine":   state["machine"],
        "component": random.choice(COMPONENTS),
        "level":     level,
        "message":   msg,
    }

# --- send loop ------------------------------------------------------------

def send_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.3)

    while True:
        with lock:
            running  = state["running"]
            interval = state["interval"]
            dest     = (state["server"], state["port"])

        if not running:
            time.sleep(0.2)
            continue

        entry = make_log()
        sock.sendto(json.dumps(entry).encode(), dest)

        with lock:
            state["sent"] += 1

        print(f"  [{entry['timestamp']}]  {entry['level']:<5}  [{entry['component']}]  {entry['message']}")

        # check for backpressure signal from server
        try:
            data, _ = sock.recvfrom(64)
            signal = data.decode().strip()
            with lock:
                state["signal"] = signal
                if signal == "SLOW_DOWN":
                    state["interval"] = min(state["interval"] * 2, 8.0)
                    print(f"  ⚠ SLOW_DOWN received — interval → {state['interval']:.1f}s")
                elif signal == "SPEED_UP":
                    state["interval"] = max(args.interval, state["interval"] * 0.75)
        except socket.timeout:
            pass

        time.sleep(interval)

# --- tiny HTTP API so the UI can talk to this script ---------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass

    def _send(self, code, body):
        b = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", len(b))
        self.end_headers()
        self.wfile.write(b)

    def do_OPTIONS(self):
        self.send_response(200)
        for h, v in [("Access-Control-Allow-Origin","*"),
                     ("Access-Control-Allow-Methods","GET,POST,OPTIONS"),
                     ("Access-Control-Allow-Headers","Content-Type")]:
            self.send_header(h, v)
        self.end_headers()

    def do_GET(self):
        with lock:
            self._send(200, dict(state))

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n)) if n else {}
        with lock:
            for key in ("machine", "server", "running"):
                if key in body: state[key] = body[key]
            if "port"     in body: state["port"]     = int(body["port"])
            if "interval" in body: state["interval"] = float(body["interval"])
        self._send(200, {"ok": True})

threading.Thread(target=send_loop, daemon=True).start()
print(f"Client '{args.machine}' → {args.server}:{args.port}  |  UI on http://127.0.0.1:{args.ui_port}\n")
HTTPServer(("0.0.0.0", args.ui_port), Handler).serve_forever()