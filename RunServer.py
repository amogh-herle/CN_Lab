"""
run_servers.py — Launch n UDP servers on consecutive ports starting from 20000
Usage:
    python run_servers.py 3
    python run_servers.py 3 --start-port 20000 --lb-port 21000
"""

import subprocess
import sys
import time
import argparse

parser = argparse.ArgumentParser(description="Launch n UDP servers")
parser.add_argument("n",            type=int, help="Number of servers")
parser.add_argument("--start-port", default=20000, type=int)
parser.add_argument("--lb-port",    default=21000, type=int)
parser.add_argument("--host",       default="192.168.137.209", help="Bind IP for servers")
parser.add_argument("--lb-host",    default="192.168.137.1", help="Load balancer IP for servers to send to")
args = parser.parse_args()

ports = list(range(args.start_port, args.start_port + args.n))

print()
print(f"  Launching {args.n} servers on ports {ports}")
print(f"  Each server forwards flushed logs to LB on port {args.lb_port}")
print()

processes = []

for i, port in enumerate(ports):
    p = subprocess.Popen(
        ["python", "UDPServer.py",
         "--host",    args.host,
         "--port",    str(port),
         "--lb-host", args.lb_host,
         "--lb-port", str(args.lb_port)]
    )
    processes.append(p)
    print(f"  [server {i+1}] port {port} started (pid {p.pid})")
    time.sleep(0.1)

print()
print(f"  All {args.n} servers running. Now start the load balancer:")
print(f"    python UDPLoadBalancer.py --servers {' '.join(str(p) for p in ports)}")
print()
print("  Press Ctrl+C to stop all servers.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n  Stopping all servers...")
    for p in processes:
        p.terminate()
    print("  Done.")