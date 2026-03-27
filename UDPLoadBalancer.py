import socket
import time
import json
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description="UDP Load Balancer")
parser.add_argument("--host",    default="127.0.0.1", help="Bind address")
parser.add_argument("--port",    default=20005, type=int, help="Bind UDP port")
parser.add_argument("--servers", default="127.0.0.1:20001,127.0.0.1:20002,127.0.0.1:20003")
args = parser.parse_args()

MAX_PACKET_SIZE = 4096
AGG_FLUSH_INTERVAL = 2.0
STATS_INTERVAL     = 2.0

servers = []
for s in args.servers.split(","):
    h, p = s.strip().split(":")
    servers.append((h, int(p)))
server_set = set(servers)

counters = {s: 0 for s in servers}
stats = {"forwarded": 0, "relayed": 0, "aggregated": 0}

agg_buffer = []
agg_file   = open("aggregated_logs.txt", "a", buffering=1)
agg_last_flush = time.time()

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.host, args.port))
sock.setblocking(False)

def parse_ts(ts_str):
    try:
        dt = datetime.strptime(ts_str, "%H:%M:%S.%f")
        return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
    except Exception:
        return time.time()

def pick_server():
    return min(servers, key=lambda s: counters[s])

def counter_display():
    return " | ".join([f"S{i+1}({s[1]}): {counters[s]}" for i, s in enumerate(servers)])

def handle_client_packet(data, client_addr):
    server = pick_server()
    # Embed client address into the payload for stateless routing
    addr_prefix = f"{client_addr[0]}:{client_addr[1]}|".encode()
    sock.sendto(addr_prefix + data, server)
    counters[server] += 1
    stats["forwarded"] += 1

def handle_server_response(data, server_addr):
    msg = data.decode(errors="replace").strip()

    if msg.startswith("SLOW_DOWN|") or msg.startswith("STOP|"):
        signal, client_str = msg.split("|", 1)
        c_ip, c_port = client_str.split(":")
        client_addr = (c_ip, int(c_port))
        
        sock.sendto(signal.encode(), client_addr)
        counters[server_addr] = max(0, counters[server_addr] - 1)
        stats["relayed"] += 1
        
        label = "⚠" if signal == "SLOW_DOWN" else "🛑"
        print(f"  {label} [{signal}] from :{server_addr[1]} → relayed to client  [{counter_display()}]")

    elif msg.startswith("LOG_FLUSH:"):
        log_json = msg[len("LOG_FLUSH:"):]
        counters[server_addr] = max(0, counters[server_addr] - 1)
        stats["aggregated"] += 1

        try:
            e = json.loads(log_json)
            ts_f = parse_ts(e.get("timestamp", ""))
            line = f"  {e['timestamp']}  {e['level']:<8}  [{e['machine']}]  {e['component']:<14}  {e['message']}"
        except Exception:
            ts_f = time.time()
            line = f"  {log_json}"

        agg_buffer.append((ts_f, line))

def maybe_flush_aggregation():
    global agg_last_flush
    if not agg_buffer or time.time() - agg_last_flush < AGG_FLUSH_INTERVAL: return
    
    agg_buffer.sort(key=lambda x: x[0])
    count = len(agg_buffer)
    for _, line in agg_buffer:
        agg_file.write(line + "\n")
    agg_buffer.clear()
    agg_last_flush = time.time()
    print(f"  📝 Aggregated {count} log entries → aggregated_logs.txt")

last_stats_time = time.time()
def maybe_print_stats():
    global last_stats_time
    now = time.time()
    if now - last_stats_time < STATS_INTERVAL: return
    last_stats_time = now
    print(f"  [stats] fwd={stats['forwarded']} relayed={stats['relayed']} aggregated={stats['aggregated']} [{counter_display()}]")

print(f"\n  UDP Load Balancer on {args.host}:{args.port}")
while True:
    try:
        while True:
            data, addr = sock.recvfrom(MAX_PACKET_SIZE)
            if addr in server_set:
                handle_server_response(data, addr)
            else:
                handle_client_packet(data, addr)
    except BlockingIOError:
        pass

    maybe_flush_aggregation()
    maybe_print_stats()
    time.sleep(0.001)