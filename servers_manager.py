import socket
import time
import json
import argparse
import threading
from datetime import datetime
from collections import deque

parser = argparse.ArgumentParser()
parser.add_argument("n", type=int, nargs="?", default=3)
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--start-port", default=20001, type=int)
parser.add_argument("--lb-host", default="127.0.0.1")
parser.add_argument("--lb-port", default=20005, type=int)
args = parser.parse_args()

print_lock = threading.Lock()

def safe_print(*a, **kw):
    with print_lock:
        print(*a, **kw)

class ServerThread(threading.Thread):
    def __init__(self, host, port, lb_addr):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.lb_addr = lb_addr
        
        self.n_buffer = 20
        self.FLUSH_DELAY = 0.3
        self.FLUSH_THRESHOLD = int(self.n_buffer * 0.15)
        self.SLOW_THRESHOLD = int(self.n_buffer * 0.80)
        self.STOP_THRESHOLD = self.n_buffer

        self.stats = {"received": 0, "flushed": 0, "dropped": 0}
        self.log_buffer = []
        self.flush_queue = deque()
        self.next_flush_at = 0.0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.sock.setblocking(False)

    def parse_ts(self, ts_str):
        try:
            dt = datetime.strptime(ts_str, "%H:%M:%S.%f")
            return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
        except Exception:
            return time.time()

    def handle(self, data):
        raw = data.decode(errors="replace").strip()
        
        # Extract client address added by Load Balancer
        if "|" in raw:
            client_str, raw_json = raw.split("|", 1)
        else:
            client_str, raw_json = "unknown", raw
            
        depth = len(self.log_buffer) + len(self.flush_queue)

        if depth >= self.STOP_THRESHOLD:
            self.sock.sendto(f"STOP|{client_str}".encode(), self.lb_addr)
            self.stats["dropped"] += 1
            safe_print(f"  [P:{self.port}] STOP sent -> {client_str}")
            return

        if depth >= self.SLOW_THRESHOLD:
            self.sock.sendto(f"SLOW_DOWN|{client_str}".encode(), self.lb_addr)
            safe_print(f"  [P:{self.port}] SLOW_DOWN sent -> {client_str}")
            return

        try:
            entry = json.loads(raw_json)
            ts_f = self.parse_ts(entry.get("timestamp", ""))
        except Exception:
            ts_f = time.time()

        self.log_buffer.append((ts_f, raw_json))
        self.stats["received"] += 1

        if len(self.log_buffer) >= self.FLUSH_THRESHOLD and not self.flush_queue:
            count = len(self.log_buffer)
            self.log_buffer.sort(key=lambda x: x[0])
            self.flush_queue.extend(self.log_buffer[:count])
            del self.log_buffer[:count]

    def maybe_flush_one(self):
        if not self.flush_queue or time.time() < self.next_flush_at:
            return

        _, line = self.flush_queue.popleft()
        
        try:
            e = json.loads(line)
            log_str = f"  [P:{self.port}] {e['timestamp']} {e['level']:<8} [{e['machine']}] {e['message']}"
        except Exception:
            log_str = f"  [P:{self.port}] {line}"

        safe_print(log_str)

        try:
            self.sock.sendto(f"LOG_FLUSH:{line}".encode(), self.lb_addr)
        except Exception:
            pass

        self.stats["flushed"] += 1
        self.next_flush_at = time.time() + self.FLUSH_DELAY

    def run(self):
        while True:
            try:
                while True:
                    data, _ = self.sock.recvfrom(4096)
                    self.handle(data)
            except BlockingIOError:
                pass

            self.maybe_flush_one()
            time.sleep(0.005)

print(f"\n  Starting {args.n} servers on ports {args.start_port}-{args.start_port + args.n - 1}")
lb_addr = (args.lb_host, args.lb_port)

for i in range(args.n):
    port = args.start_port + i
    t = ServerThread(args.host, port, lb_addr)
    t.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n  Exiting.")