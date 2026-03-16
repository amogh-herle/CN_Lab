import socket
import time
import json
import heapq
from datetime import datetime

HOST = "127.0.0.1"
PORT = 20005
BUFFER = 4096
QUEUE_LIMIT = 15       # backpressure kicks in above this
FLUSH_EVERY = 2.0      # seconds between ordered flushes

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((HOST, PORT))
sock.setblocking(False)   # non-blocking so we can also do timed flushes

heap  = []   # (timestamp_float, raw_json_string)
stats = {"received": 0, "flushed": 0, "dropped": 0}
last_flush = time.time()


def flush_logs():
    batch = []
    while heap:
        batch.append(heapq.heappop(heap))

    for _, line in batch:
        try:
            e = json.loads(line)
            print(f"  {e['timestamp']}  {e['level']:<8}  [{e['machine']}]  {e['message']}")
            stats["flushed"] += 1
        except Exception:
            print(f"  {line}")

    if batch:
        print(f"  — flushed {len(batch)} logs | received {stats['received']} | dropped {stats['dropped']}\n")


def handle(data, addr):
    raw = data.decode(errors="replace").strip()

    # backpressure: queue too deep, tell client to slow down
    if len(heap) >= QUEUE_LIMIT:
        sock.sendto(b"SLOW_DOWN", addr)
        stats["dropped"] += 1
        return

    # queue recovering, tell client it can speed up
    if len(heap) < QUEUE_LIMIT // 3:
        sock.sendto(b"SPEED_UP", addr)

    try:
        entry  = json.loads(raw)
        ts_str = entry.get("timestamp", "")
        ts_dt  = datetime.strptime(ts_str, "%H:%M:%S.%f")
        ts_f   = ts_dt.hour * 3600 + ts_dt.minute * 60 + ts_dt.second + ts_dt.microsecond / 1e6
    except Exception:
        ts_f = time.time()

    heapq.heappush(heap, (ts_f, raw))
    stats["received"] += 1


print(f"Server listening on {HOST}:{PORT}  (backpressure at queue depth {QUEUE_LIMIT})\n")

while True:
    # try to receive a packet
    try:
        data, addr = sock.recvfrom(BUFFER)
        handle(data, addr)
    except BlockingIOError:
        pass   # no packet right now, that's fine

    # flush on a timer
    if time.time() - last_flush >= FLUSH_EVERY:
        flush_logs()
        last_flush = time.time()

    time.sleep(0.01)   # small sleep to avoid busy-spinning