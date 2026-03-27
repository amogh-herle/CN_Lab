import socket
import time
import json
import sys
from datetime import datetime
from collections import deque

HOST = "127.0.0.1"
MAX_PACKET_SIZE = 4096

n = 5  # flush triggers at n entries, max buffer is 2n
FLUSH_DELAY = 0.3  # artificial processing delay per log entry (seconds)

def run_server(server_id, port):
    stats = {"received": 0, "flushed": 0, "dropped": 0}
    
    logfile = open(f"logs_server{server_id}.txt", "a", buffering=1)
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, port))
    sock.setblocking(False)
    
    log_buffer = []  # (timestamp_float, raw_json, addr)
    flush_queue = deque()  # sorted entries waiting to be printed one by one
    next_flush_at = 0.0
    
    throughput_last_time = time.time()
    throughput_last_flushed = 0
    
    SLOW_THRESHOLD = int(2 * n * 0.9)
    STOP_THRESHOLD = 2 * n
    
    print(f"  Server-{server_id} {HOST}:{port} | n={n} | slow@{SLOW_THRESHOLD} | stop@{STOP_THRESHOLD} | delay={FLUSH_DELAY}s/entry\n")
    
    def parse_ts(ts_str):
        try:
            dt = datetime.strptime(ts_str, "%H:%M:%S.%f")
            return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
        except Exception:
            return time.time()
    
    def handle(data, addr):
        raw = data.decode(errors="replace").strip()
        depth = len(log_buffer) + len(flush_queue)  # true total entries in system
        
        # ---------- Backpressure Handling ----------
        if depth >= STOP_THRESHOLD:
            sock.sendto(b"STOP", addr)
            stats["dropped"] += 1
            print(f"  [Server-{server_id}] [STOP sent -> {addr[0]}:{addr[1]}]")
            return
        
        if depth >= SLOW_THRESHOLD:
            sock.sendto(b"SLOW_DOWN", addr)
            print(f"  [Server-{server_id}] [SLOW_DOWN sent -> {addr[0]}:{addr[1]}]")
        
        # ---------- Streaming Ingestion ----------
        try:
            entry = json.loads(raw)
            ts_f = parse_ts(entry.get("timestamp", ""))
        except Exception:
            ts_f = time.time()
        
        log_buffer.append((ts_f, raw, addr))
        stats["received"] += 1
        
        # ---------- Time Ordering ----------
        if len(log_buffer) >= n:
            print(f"  [Server-{server_id}] Buffer reached {n} — sorting and draining...")
            log_buffer.sort(key=lambda x: x[0])
            flush_queue.extend(log_buffer[:n])
            del log_buffer[:n]
    
    def maybe_flush_one():
        nonlocal next_flush_at
        if not flush_queue or time.time() < next_flush_at:
            return
        
        _, line, _ = flush_queue.popleft()
        try:
            e = json.loads(line)
            log_str = f"  {e['timestamp']}  [{e.get('machine', 'unknown')}]  {e.get('message', line)}"
        except Exception:
            log_str = f"  {line}"
        
        print(log_str)
        logfile.write(log_str + "\n")
        
        stats["flushed"] += 1
        next_flush_at = time.time() + FLUSH_DELAY
        
        if not flush_queue:
            depth = len(log_buffer) + len(flush_queue)
            print(f"  [Server-{server_id}] [ buffer: {depth}/{STOP_THRESHOLD} | received: {stats['received']} | dropped: {stats['dropped']} ]\n")
    
    def maybe_print_throughput():
        nonlocal throughput_last_time, throughput_last_flushed
        now = time.time()
        # ---------- Throughput Evaluation ----------
        if now - throughput_last_time >= 1.0:
            logs_sec = stats["flushed"] - throughput_last_flushed
            print(f"  [Server-{server_id}] [throughput] {logs_sec} logs/sec")
            throughput_last_flushed = stats["flushed"]
            throughput_last_time = now
    
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

if __name__ == "__main__":
    server_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 20005
    
    run_server(server_id, port)
