"""
UDPLoadBalancer.py
- Listens for clients on --client-port (default 22000)
- Forwards to servers using least-connections
- Receives flushed logs + signals back from servers on --lb-port (default 21000)
- Merges logs by timestamp -> aggregated_logs.txt

Usage:
    python UDPLoadBalancer.py
    python UDPLoadBalancer.py --servers 20000 20001 20002
"""

import socket
import time
import json
import heapq
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description="UDP Load Balancer + Aggregator")
parser.add_argument("--host",        default="127.0.0.1")
parser.add_argument("--client-port", default=22000, type=int, help="Port clients connect to")
parser.add_argument("--lb-port",     default=21000, type=int, help="Port servers send logs/signals back to")
parser.add_argument("--servers",     default=[20000, 20001], nargs="+", type=int, help="Server ports")
args = parser.parse_args()

HOST         = args.host
CLIENT_PORT  = args.client_port
LB_PORT      = args.lb_port
SERVER_ADDRS = [(HOST, p) for p in args.servers]

MAX_PACKET = 4096
AGG_FILE   = "aggregated_logs.txt"

# socket facing clients
client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_sock.bind((HOST, CLIENT_PORT))
client_sock.setblocking(False)

# socket facing servers (must be bound so servers can send back to it)
server_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_sock.bind((HOST, LB_PORT))
server_sock.setblocking(False)

active      = {s: 0 for s in SERVER_ADDRS}
last_client = {}

agg_heap = []
agg_file = open(AGG_FILE, "a", buffering=1)

stats = {"forwarded": 0, "aggregated": 0, "signals": 0}
last_status = time.time()


def parse_ts(ts_str):
    try:
        dt = datetime.strptime(ts_str, "%H:%M:%S.%f")
        return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
    except Exception:
        return time.time()


def pick_server():
    return min(SERVER_ADDRS, key=lambda s: active[s])


def flush_agg_heap():
    cutoff = time.time() - 2.0
    while agg_heap and agg_heap[0][0] < cutoff:
        ts_f, log_str = heapq.heappop(agg_heap)
        print(f"  {log_str}", flush=True)
        agg_file.write(log_str + "\n")
        stats["aggregated"] += 1


def print_status():
    print(f"\n  {'SERVER':<22} {'ACTIVE':>6} {'FORWARDED':>10}")
    for s in SERVER_ADDRS:
        print(f"  {s[0]}:{s[1]:<16} {active[s]:>6}")
    print(f"  Total forwarded: {stats['forwarded']} | Aggregated: {stats['aggregated']} | Signals: {stats['signals']}")
    print(f"  Heap depth: {len(agg_heap)}\n")


print(f"  Load Balancer | clients->{HOST}:{CLIENT_PORT} | servers->{HOST}:{LB_PORT}")
print(f"  Servers : {[f'{HOST}:{p}' for p in args.servers]}")
print(f"  Output  : {AGG_FILE}\n")

while True:

    # ── 1. clients → LB → forward to least-busy server ───────────────────
    try:
        while True:
            data, client_addr = client_sock.recvfrom(MAX_PACKET)
            server = pick_server()
            print(f"  [LB] Routing client log to server {server[0]}:{server[1]}", flush=True)
            active[server] += 1
            last_client[server] = client_addr
            server_sock.sendto(data, server)
            stats["forwarded"] += 1
    except BlockingIOError:
        pass

    # ── 2. servers → LB: flushed logs or signals ──────────────────────────
    try:
        while True:
            data, server_addr = server_sock.recvfrom(MAX_PACKET)
            raw = data.decode(errors="replace").strip()

            if raw in ("SLOW_DOWN", "STOP"):
                if active.get(server_addr, 0) > 0:
                    active[server_addr] -= 1
                if server_addr in last_client:
                    client_sock.sendto(data, last_client[server_addr])
                    stats["signals"] += 1
                    print(f"  [{raw}] :{server_addr[1]} -> client", flush=True)
            else:
                try:
                    entry   = json.loads(raw)
                    ts_f    = parse_ts(entry.get("timestamp", ""))
                    log_str = f"{entry['timestamp']}  {entry['level']:<8}  [{entry['machine']}]  {entry['message']}"
                except Exception:
                    ts_f    = time.time()
                    log_str = raw

                heapq.heappush(agg_heap, (ts_f, log_str))

    except BlockingIOError:
        pass

    # ── 3. drain aggregation heap ─────────────────────────────────────────
    flush_agg_heap()

    # ── 4. periodic status ────────────────────────────────────────────────
    if time.time() - last_status >= 3.0:
        print_status()
        last_status = time.time()

    time.sleep(0.005)