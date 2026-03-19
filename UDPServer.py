import socket
import time
import json
from datetime import datetime

HOST  = "127.0.0.1"
PORT  = 20005
BUFFER_SIZE = 4096

n      = 10          # tune this — flush threshold is n, max buffer is 2n
stats  = {"received": 0, "flushed": 0, "dropped": 0}

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((HOST, PORT))
sock.setblocking(False)

log_buffer = []   # list of (timestamp_float, raw_json_string, addr)


def parse_ts(ts_str):
    try:
        dt = datetime.strptime(ts_str, "%H:%M:%S.%f")
        return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
    except Exception:
        return time.time()


def flush_oldest(count):
    """Sort buffer by timestamp and flush the oldest `count` entries."""
    log_buffer.sort(key=lambda x: x[0])
    to_flush = log_buffer[:count]
    del log_buffer[:count]

    print(f"\n  [ flushing {count} entries ]")
    for _, line, _ in to_flush:
        try:
            e = json.loads(line)
            print(f"  {e['timestamp']}  {e['level']:<8}  [{e['machine']}]  {e['message']}")
        except Exception:
            print(f"  {line}")

    stats["flushed"] += count
    print(f"  [ buffer: {len(log_buffer)}/{2*n} | received: {stats['received']} | dropped: {stats['dropped']} ]\n")


def handle(data, addr):
    raw = data.decode(errors="replace").strip()

    # buffer completely full — stop the client entirely
    if len(log_buffer) >= 2 * n:
        sock.sendto(b"STOP", addr)
        stats["dropped"] += 1
        return

    # buffer 90% full — slow the client down
    if len(log_buffer) >= int(2 * n * 0.9):
        sock.sendto(b"SLOW_DOWN", addr)
        stats["dropped"] += 1
        return

    # parse and add to buffer
    try:
        entry = json.loads(raw)
        ts_f  = parse_ts(entry.get("timestamp", ""))
    except Exception:
        ts_f = time.time()

    log_buffer.append((ts_f, raw, addr))
    stats["received"] += 1

    # buffer hit n — flush oldest n/2 entries
    if len(log_buffer) == n:
        flush_count = n // 2
        print(f"  Buffer reached {n} — sorting and flushing {flush_count} entries...")
        flush_oldest(flush_count)

    # buffer recovering below 2n//3 — tell client to speed up
    if len(log_buffer) < (2 * n) // 3:
        sock.sendto(b"SPEED_UP", addr)


print(f"Server on {HOST}:{PORT} | n={n} | flush at {n} | slow at 90% | stop at 100% (max {2*n})\n")

while True:
    try:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        handle(data, addr)
    except BlockingIOError:
        pass

    time.sleep(0.01)