import socket
import time
import json
import random
import threading
import sys
import msvcrt
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("n", type=int, nargs="?", default=4)
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", default=20005, type=int)
parser.add_argument("--interval", default=1.0, type=float)
args = parser.parse_args()

print_lock = threading.Lock()

def safe_print(*a, **kw):
    with print_lock:
        print(*a, **kw)

global_state = {
    "running": True,
    "rapid": False,
}

LEVELS = ["INFO", "INFO", "DEBUG", "WARN", "ERROR"]
COMPONENTS = ["AuthService", "Database", "Cache", "APIGateway"]
MESSAGES = {
    "INFO": ["Request completed in {n}ms", "Cache hit ratio {n}%"],
    "DEBUG": ["Query returned {n} rows", "Buffer flushed ({n} bytes)"],
    "WARN": ["Slow query: {n}ms", "Memory at {n}%"],
    "ERROR": ["Connection timeout", "Token expired"],
}

class ClientThread(threading.Thread):
    def __init__(self, machine_name, host, port, base_interval):
        super().__init__(daemon=True)
        self.machine = machine_name
        self.host = host
        self.port = port
        self.base_interval = base_interval
        self.interval = base_interval
        self.retry_after = 5.0
        self.stopped = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.3)

    def make_log(self):
        level = random.choice(LEVELS)
        msg = random.choice(MESSAGES[level]).format(n=random.randint(1, 999))
        return {
            "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "machine": self.machine,
            "component": random.choice(COMPONENTS),
            "level": level,
            "message": msg,
        }

    def step_down(self):
        if self.interval > self.base_interval:
            self.interval = max(self.base_interval, self.interval / 2)

    def run(self):
        while True:
            if not global_state["running"]:
                time.sleep(0.1)
                continue

            if self.stopped:
                safe_print(f"  [{self.machine}] waiting {self.retry_after}s...")
                time.sleep(self.retry_after)
                self.stopped = False
                self.step_down()
                continue

            self.sock.sendto(json.dumps(self.make_log()).encode(), (self.host, self.port))

            try:
                data, _ = self.sock.recvfrom(64)
                signal = data.decode().strip()
                if signal == "STOP":
                    self.stopped = True
                    safe_print(f"  [{self.machine}] STOP received")
                elif signal == "SLOW_DOWN":
                    self.interval = min(self.interval * 2, 8.0)
                    safe_print(f"  [{self.machine}] SLOW_DOWN -> {self.interval:.1f}s")
            except socket.timeout:
                self.step_down()

            time.sleep(0.05 if global_state["rapid"] else self.interval)

print(f"\n  Starting {args.n} clients pointing to {args.host}:{args.port}")
print("  Controls: [s] pause/resume | [f] toggle rapid fire | [q] quit\n")

for i in range(1, args.n + 1):
    t = ClientThread(f"Machine-{i}", args.host, args.port, args.interval)
    t.start()

try:
    while True:
        ch = msvcrt.getwch()
        if ch in ('q', '\x03'):
            break
        elif ch == 's':
            global_state["running"] = not global_state["running"]
            safe_print(f"  [ALL] {'Running' if global_state['running'] else 'Paused'}")
        elif ch == 'f':
            global_state["rapid"] = not global_state["rapid"]
            safe_print(f"  [ALL] Rapid fire {'ON' if global_state['rapid'] else 'OFF'}")
except KeyboardInterrupt:
    pass
print("\n  Exiting.")