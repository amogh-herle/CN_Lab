import socket
import time
import json
import random
import threading
import sys
import msvcrt
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description="UDP Log Client")
parser.add_argument("--host",     default="127.0.0.1", help="Server IP address")
parser.add_argument("--port",     default=20005, type=int, help="Server UDP port")
parser.add_argument("--name",     default="Machine-A",  help="Client/machine name")
parser.add_argument("--interval", default=1.0,  type=float, help="Send interval in seconds")
args = parser.parse_args()

state = {
    "machine":     args.name,
    "interval":    args.interval,
    "retry_after": 5.0,
    "running":     True,
    "stopped":     False,
    "rapid":       False,
    "typing":      False,
}

HOST = args.host
PORT = args.port

LEVELS     = ["INFO", "INFO", "DEBUG", "WARN", "ERROR"]
COMPONENTS = ["AuthService", "Database", "Cache", "APIGateway", "Scheduler", "FileWatcher"]
MESSAGES   = {
    "INFO":  ["Request completed in {n}ms", "Cache hit ratio {n}%", "Startup OK", "Config reloaded"],
    "DEBUG": ["Query returned {n} rows", "Buffer flushed ({n} bytes)", "Retry attempt {n}/3"],
    "WARN":  ["Slow query: {n}ms", "Memory at {n}%", "Queue depth: {n}"],
    "ERROR": ["Connection timeout", "Write failed: constraint", "Token expired"],
}

def make_log():
    level = random.choice(LEVELS)
    msg   = random.choice(MESSAGES[level]).format(n=random.randint(1, 999))
    return {
        "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "machine":   state["machine"],
        "component": random.choice(COMPONENTS),
        "level":     level,
        "message":   msg,
    }

def print_help():
    print("\n  [s] start / pause")
    print("  [f] toggle rapid fire")
    print("  [n] set machine name")
    print("  [i] set send interval")
    print("  [r] set retry duration after STOP")
    print("  [h] help")
    print("  [q] quit\n")

def send_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.3)

    while True:
        if state["typing"] or not state["running"]:
            time.sleep(0.1)
            continue

        if state["stopped"]:
            retry = state["retry_after"]
            print(f"  [STOP] waiting {retry}s before retry...")
            time.sleep(retry)
            state["stopped"] = False
            print("  [resuming]")
            continue

        sock.sendto(json.dumps(make_log()).encode(), (HOST, PORT))

        try:
            data, _ = sock.recvfrom(64)
            signal  = data.decode().strip()
            if signal == "STOP":
                state["stopped"] = True
                state["rapid"]   = False
                print("  [STOP received]")
            elif signal == "SLOW_DOWN":
                state["interval"] = min(state["interval"] * 2, 8.0)
                state["rapid"]    = False
                print(f"  [SLOW_DOWN] interval -> {state['interval']:.1f}s")
        except socket.timeout:
            pass

        time.sleep(0.05 if state["rapid"] else state["interval"])

def prompt(label):
    state["typing"] = True
    time.sleep(0.15)
    val = input(label).strip()
    state["typing"] = False
    return val

def input_loop():
    print_help()
    while True:
        ch = msvcrt.getwch()

        if ch in ('q', '\x03'):
            print("\n  Exiting.")
            sys.exit(0)

        elif ch == 's':
            state["running"] = not state["running"]
            print(f"  [{'running' if state['running'] else 'paused'}]")

        elif ch == 'f':
            state["rapid"] = not state["rapid"]
            print(f"  [rapid fire {'ON  -- hammering server' if state['rapid'] else 'OFF'}]")

        elif ch == 'n':
            val = prompt("  Machine name: ")
            if val:
                state["machine"] = val
                print(f"  name -> '{val}'")

        elif ch == 'i':
            val = prompt("  Interval (seconds): ")
            try:
                state["interval"] = max(0.1, float(val))
                print(f"  interval -> {state['interval']}s")
            except ValueError:
                print("  invalid")

        elif ch == 'r':
            val = prompt("  Retry after STOP (seconds): ")
            try:
                state["retry_after"] = max(1.0, float(val))
                print(f"  retry -> {state['retry_after']}s")
            except ValueError:
                print("  invalid")

        elif ch == 'h':
            print_help()


print(f"  UDP Log Client | {HOST}:{PORT} | name: {state['machine']} | interval: {state['interval']}s")

threading.Thread(target=send_loop, daemon=True).start()

try:
    input_loop()
except KeyboardInterrupt:
    print("\n  Exiting.")
    sys.exit(0)