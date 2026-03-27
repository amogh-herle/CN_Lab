import socket
import time
import json
import argparse
from datetime import datetime
from collections import deque

parser = argparse.ArgumentParser(description="UDP Log Server")
parser.add_argument("--host",    default="10.30.204.238", help="Server bind address")
parser.add_argument("--port",    default=20005, type=int)
parser.add_argument("--lb-host", default="10.30.202.168", help="Load balancer IP")
parser.add_argument("--lb-port", default=20000, type=int)
args = parser.parse_args()

HOST    = args.host
PORT    = args.port
LB_ADDR = (args.lb_host, args.lb_port)

MAX_PACKET_SIZE = 4096

n           = 50
FLUSH_DELAY = 0.1

FLUSH_THRESHOLD = int(n * 0.15)
SLOW_THRESHOLD  = int(n * 0.80)
STOP_THRESHOLD  = n

stats = {"received": 0, "flushed": 0, "dropped": 0}

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((HOST, PORT))
sock.setblocking(False)

log_buffer    = []
flush_queue   = deque()
next_flush_at = 0.0

throughput_last_time    = time.time()
throughput_last_flushed = 0


def parse_ts(ts_str):
    try:
        dt = datetime.strptime(ts_str, "%H:%M:%S")
        return dt.hour * 3600 + dt.minute * 60 + dt.second
    except Exception:
        return time.time()


def handle(data, addr):
    raw   = data.decode(errors="replace").strip()
    depth = len(log_buffer) + len(flush_queue)

    if depth >= STOP_THRESHOLD:
        print(f"  [:{PORT}] Threshold reached ({depth}/{STOP_THRESHOLD}) -> sending STOP", flush=True)
        sock.sendto(b"STOP", addr)
        stats["dropped"] += 1
        return

    if depth >= SLOW_THRESHOLD:
        print(f"  [:{PORT}] Threshold reached ({depth}/{SLOW_THRESHOLD}) -> sending SLOW_DOWN", flush=True)
        sock.sendto(b"SLOW_DOWN", addr)
        return

    try:
        entry = json.loads(raw)
        ts_f  = parse_ts(entry.get("timestamp", ""))
    except Exception:
        ts_f = time.time()

    log_buffer.append((ts_f, raw, addr))
    stats["received"] += 1

    if len(log_buffer) >= FLUSH_THRESHOLD and not flush_queue:
        count = len(log_buffer)
        log_buffer.sort(key=lambda x: x[0])
        flush_queue.extend(log_buffer[:count])
        del log_buffer[:count]


def maybe_flush_one():
    global next_flush_at
    if not flush_queue or time.time() < next_flush_at:
        return

    _, line, _ = flush_queue.popleft()
    sock.sendto(line.encode(), LB_ADDR)

    stats["flushed"] += 1
    next_flush_at = time.time() + FLUSH_DELAY


def maybe_print_throughput():
    global throughput_last_time, throughput_last_flushed
    now = time.time()
    if now - throughput_last_time >= 1.0:
        logs_sec = stats["flushed"] - throughput_last_flushed
        print(f"  [:{PORT}] {logs_sec} logs/sec | buf: {len(log_buffer)+len(flush_queue)}/{STOP_THRESHOLD}", flush=True)
        throughput_last_flushed = stats["flushed"]
        throughput_last_time    = now


print(f"  Server {HOST}:{PORT} | LB={LB_ADDR} | n={n} | flush@{FLUSH_THRESHOLD} | slow@{SLOW_THRESHOLD} | stop@{STOP_THRESHOLD}", flush=True)

while True:
    try:
        while True:
            data, addr = sock.recvfrom(MAX_PACKET_SIZE)
            handle(data, addr)
    except BlockingIOError:
        pass

    maybe_flush_one()
    maybe_print_throughput()

    time.sleep(0.005)