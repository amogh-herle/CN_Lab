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
parser.add_argument("--lb-port", default=21000, type=int)
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

stats = {"received": 0, "flushed": 0, "dropped": 0, "slow_sent": 0, "stop_sent": 0}

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((HOST, PORT))
sock.setblocking(False)

log_buffer    = []
flush_queue   = deque()
next_flush_at = 0.0

# True when buffer hit STOP_THRESHOLD — stop accepting until depth < SLOW_THRESHOLD
server_stopped = False

throughput_last_time    = time.time()
throughput_last_flushed = 0


def parse_ts(ts_str):
    try:
        dt = datetime.strptime(ts_str, "%H:%M:%S")
        return dt.hour * 3600 + dt.minute * 60 + dt.second
    except Exception:
        return time.time()


def handle(data, addr):
    global server_stopped
    raw   = data.decode(errors="replace").strip()
    depth = len(log_buffer) + len(flush_queue)

    # ── STOP gate: once stopped, only re-open when depth < SLOW_THRESHOLD ───
    if server_stopped:
        if depth < SLOW_THRESHOLD:
            server_stopped = False
            print(f"  [:{PORT}] Buffer drained below SLOW threshold ({depth}/{SLOW_THRESHOLD}) -> RESUMING", flush=True)
        else:
            # still congested — drop and re-send STOP
            sock.sendto(b"STOP", addr)
            stats["dropped"] += 1
            return

    # ── STOP threshold ────────────────────────────────────────────────────
    if depth >= STOP_THRESHOLD:
        server_stopped = True
        print(f"  [:{PORT}] STOP  ({depth}/{STOP_THRESHOLD}) -> buffer full, dropping", flush=True)
        sock.sendto(b"STOP", addr)
        stats["dropped"] += 1
        stats["stop_sent"] += 1
        return

    # ── SLOW_DOWN threshold: warn but still accept ─────────────────────────
    if depth >= SLOW_THRESHOLD:
        print(f"  [:{PORT}] SLOW_DOWN ({depth}/{SLOW_THRESHOLD})", flush=True)
        sock.sendto(b"SLOW_DOWN", addr)
        stats["slow_sent"] += 1

    # ── buffer the log ────────────────────────────────────────────────────
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
        depth    = len(log_buffer) + len(flush_queue)
        status   = "STOPPED" if server_stopped else ("SLOW" if depth >= SLOW_THRESHOLD else "OK")
        print(
            f"  [:{PORT}] {logs_sec:>4} logs/s | buf {depth:>3}/{STOP_THRESHOLD} | "
            f"slow={stats['slow_sent']} stop={stats['stop_sent']} drop={stats['dropped']} [{status}]",
            flush=True
        )
        throughput_last_flushed = stats["flushed"]
        throughput_last_time    = now


print(f"  Server {HOST}:{PORT} | LB={LB_ADDR} | buf={n} | slow@{SLOW_THRESHOLD} | stop@{STOP_THRESHOLD}", flush=True)

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