import socket
import time
import json
import random
import threading
import sys
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description="UDP Log Client")
parser.add_argument("--host",       default="127.0.0.1", help="Server IP address")
parser.add_argument("--port",       default=22000, type=int, help="Load balancer client port")
parser.add_argument("--name",       default="Machine-A", help="Client/machine name")
parser.add_argument("--interval",   default=1.0, type=float, help="Send interval in seconds")
parser.add_argument("--headless",   action="store_true", help="No keyboard input (for background use)")
parser.add_argument("--ctrl-port", default=0, type=int, help="Local port for master control commands")
args = parser.parse_args()

HOST = args.host
PORT = args.port

MAX_INTERVAL = 16.0

state = {
    "machine":       args.name,
    "interval":      args.interval,
    "base_interval": args.interval,
    "retry_after":   5.0,
    "running":       True,
    "stopped":       False,
    "rapid":         False,
    "typing":        False,
}

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

def step_down():
    current = state["interval"]
    base    = state["base_interval"]
    if current > base:
        state["interval"] = max(base, current / 2)
        print(f"  [{state['machine']}] [recovering] interval -> {state['interval']:.1f}s", flush=True)

def control_loop():
    if args.ctrl_port == 0:
        return
    ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ctrl_sock.bind(("127.0.0.1", args.ctrl_port))
    while True:
        data, _ = ctrl_sock.recvfrom(64)
        cmd = data.decode().strip()
        if cmd == "TOGGLE_RAPID":
            state["rapid"] = not state["rapid"]
            print(f"  [{state['machine']}] Rapid fire {'ON' if state['rapid'] else 'OFF'}", flush=True)

def send_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.3)

    while True:
        if state["typing"] or not state["running"]:
            time.sleep(0.1)
            continue

        if state["stopped"]:
            retry = state["retry_after"]
            print(f"  [{state['machine']}] [STOP] waiting {retry}s before retry...", flush=True)
            time.sleep(retry)
            state["stopped"] = False
            step_down()
            continue

        sock.sendto(json.dumps(make_log()).encode(), (HOST, PORT))

        try:
            data, _ = sock.recvfrom(64)
            signal  = data.decode().strip()
            if signal == "STOP":
                state["stopped"] = True
                state["rapid"]   = False
                print(f"  [{state['machine']}] [STOP received]", flush=True)
            elif signal == "SLOW_DOWN":
                state["interval"] = min(state["interval"] * 2, MAX_INTERVAL)
                state["rapid"]    = False
                print(f"  [{state['machine']}] [SLOW_DOWN] interval -> {state['interval']:.1f}s", flush=True)
        except socket.timeout:
            step_down()
        

        time.sleep(0.05 if state["rapid"] else state["interval"])


def input_loop():
    import msvcrt

    def print_help():
        print("\n  [s] start / pause")
        print("  [f] toggle rapid fire")
        print("  [n] set machine name")
        print("  [i] set send interval")
        print("  [r] set retry duration after STOP")
        print("  [h] help")
        print("  [q] quit\n")

    def prompt(label):
        state["typing"] = True
        time.sleep(0.15)
        val = input(label).strip()
        state["typing"] = False
        return val

    print(f"  UDP Log Client | {HOST}:{PORT} | name: {state['machine']} | interval: {state['interval']}s")
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
            print(f"  [rapid fire {'ON -- hammering server' if state['rapid'] else 'OFF'}]")
        elif ch == 'n':
            val = prompt("  Machine name: ")
            if val:
                state["machine"] = val
                print(f"  name -> '{val}'")
        elif ch == 'i':
            val = prompt("  Interval (seconds): ")
            try:
                iv = max(0.1, float(val))
                state["interval"] = iv
                state["base_interval"] = iv
                print(f"  interval -> {iv}s")
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

threading.Thread(target=control_loop, daemon=True).start()
threading.Thread(target=send_loop, daemon=True).start()

if args.headless:
    # background mode — just keep alive, only signals get printed
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit(0)
else:
    try:
        input_loop()
    except KeyboardInterrupt:
        print("\n  Exiting.")
        sys.exit(0)