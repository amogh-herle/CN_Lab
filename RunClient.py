"""
run_clients.py — Launch n UDP log clients in background pointing at load balancer
Usage:
    python run_clients.py 4
    python run_clients.py 4 --port 22000 --interval 0.5
"""

import subprocess
import sys
import time
import math
import argparse

parser = argparse.ArgumentParser(description="Launch n UDP log clients")
parser.add_argument("n",          type=int)
parser.add_argument("--host",     default="10.30.202.168")
parser.add_argument("--port",     default=22000, type=int, help="Load balancer client-facing port")
parser.add_argument("--interval", default=1.0, type=float)
args = parser.parse_args()

FLUSH_DELAY   = 0.3
incoming_rate = args.n / args.interval
drain_rate    = 1.0 / FLUSH_DELAY
recommended   = math.ceil((incoming_rate / drain_rate) * 3)

print()
print(f"  Clients   : {args.n}")
print(f"  LB port   : {args.host}:{args.port}")
print(f"  Interval  : {args.interval}s per client")
print(f"  Incoming  : {incoming_rate:.2f} logs/sec | Drain: {drain_rate:.2f} logs/sec")
print(f"  Recommended buffer (n) : {recommended} — set n={recommended} in UDPServer.py")
print()

confirm = input(f"  Launch {args.n} clients? (y/n): ").strip().lower()
if confirm != 'y':
    print("  Aborted.")
    sys.exit(0)

print()

processes = []
ctrl_ports = []

for i in range(1, args.n + 1):
    name = f"Machine-{i}"
    ctrl_port = 30000 + i
    ctrl_ports.append(ctrl_port)
    
    cmd  = ["python", "Udpclient.py", "--headless",
            "--host", args.host, "--port", str(args.port),
            "--name", name, "--interval", str(args.interval),
            "--ctrl-port", str(ctrl_port)]
    p = subprocess.Popen(cmd)
    processes.append(p)
    print(f"  [{i}] {name} started (pid {p.pid}) | ctrl port {ctrl_port}")
    time.sleep(0.2)

print()
print(f"  All {args.n} clients running.")
print("  Press [f] to toggle rapid fire for all clients.")
print("  Press [q] or Ctrl+C to stop all.")

import socket
import msvcrt

master_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

try:
    while True:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ('q', '\x03'):
                break
            elif ch == 'f':
                print("  [Master] Toggling rapid fire on all clients...", flush=True)
                for cp in ctrl_ports:
                    master_sock.sendto(b"TOGGLE_RAPID", ("10.30.202.168", cp))
        time.sleep(0.1)
except KeyboardInterrupt:
    pass

print("\n  Stopping all clients...")
for p in processes:
    p.terminate()
print("  Done.")